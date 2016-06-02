%% @author Northscale <info@northscale.com>
%% @copyright 2016 NorthScale, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
-module(kv_monitor).

-define(CHECK_STATUS_PERIOD, 1000). % 1 second

-include("ns_common.hrl").

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
%% API
-export([get_nodes/0, get_node/1, get_status/1, analyze_status/2]).

-record(state, {
          nodes :: dict()
         }).

%% gen_server handlers

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:process_flag(priority, high),
    timer2:send_interval(?CHECK_STATUS_PERIOD, check_status),
    self() ! check_status,
    {ok, #state{nodes=dict:new()}}.

handle_call({get_node, Node}, _From, #state{nodes=Nodes} = State) ->
    RV = case dict:find(Node, Nodes) of
             {ok, Status} ->
                 Status;
             _ ->
                 []
         end,
    {reply, RV, State};

handle_call(get_nodes, _From, #state{nodes=Nodes} = State) ->
    {reply, dict:to_list(Nodes), State}.

handle_cast(Msg, State) ->
    ?log_debug("Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(check_status, State) ->
    Nodes = dcp_traffic_spy:get_nodes(),
    Fun = fun ({Node, NodeStatus}, AccAll) ->
            LastHeard = proplists:get_value(node_last_heard, NodeStatus),
            NodeState = health_monitor:get_state(LastHeard),
            BucketList = proplists:get_value(buckets, NodeStatus),
            BAcc = lists:foldl(
                    fun ({Bucket, {bucket_last_heard, BLH}}, Acc) ->
                        [{Bucket, health_monitor:get_state(BLH), BLH} | Acc]
                    end, [], BucketList),
            NodeInfo = {Node, [{node_state, NodeState}, {buckets, BAcc}]},
            [NodeInfo | AccAll]
          end,
    Nodes1 = lists:foldl(Fun, [], Nodes),
    %% Additional checks for local node.
    Nodes2 = check_local_node_status(Nodes1),
    %% TODO: Check for missing active buckets
    %% DCP traffic spy will not have entry for bucket without replicas
    Nodes3 = dict:from_list(Nodes2),
    {noreply, State#state{nodes=Nodes3}};

handle_info(Info, State) ->
    ?log_debug("Unexpected message ~p in state", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% API

get_nodes() ->
    try gen_server:call(?MODULE, get_nodes) of
        Nodes -> Nodes
    catch
        E:R ->
            ?log_debug("Error attempting to get nodes: ~p", [{E, R}]),
            dict:new()
    end.

get_node(Node) ->
    try gen_server:call(?MODULE, {get_node, Node}) of
        Status -> Status
    catch
        E:R ->
            ?log_debug("Error attempting to get node ~p: ~p", [Node, {E, R}]),
            []
    end.

get_status(Node) ->
    case get_node(Node) of
        [] ->
            %% Local node is not monitoring KV status for this Node.
            [];
        [_, {buckets, BucketList}] ->
            lists:foldl(
                fun ({Bucket, State, LastHeard}, Acc) ->
                   [{Bucket, State, LastHeard} | Acc]
                end, [], BucketList)
    end.

%% A node is considered healthy if atleast one node reports that
%% all buckets on the node are active or warmed (ready).
get_node_state(Node, Status) ->
    case proplists:get_value(kv, Status, unknown) of
        unknown ->
            inactive;
        [] ->
            inactive;
        BucketList ->
            NodeBuckets = ns_bucket:node_bucket_names(Node),
            ReadyBuckets = lists:filtermap(
                                fun ({Bucket, State, _}) ->
                                   case State of
                                        active ->
                                            {true, Bucket};
                                        ready ->
                                            {true, Bucket};
                                        _ ->
                                            false
                                    end
                                end, BucketList),
            NotReadyBucktes = ordsets:subtract(lists:sort(NodeBuckets),
                                               lists:sort(ReadyBuckets)),
            ?log_debug("Node:~p, NodeBuckets:~p ReadyBuckets:~p ~n",
                       [Node, NodeBuckets, ReadyBuckets]),
            case NotReadyBucktes of
                [] ->
                    active;
                _ ->
                    case ReadyBuckets of
                        [] ->
                            inactive;
                        _ ->
                            {not_ready_buckets, NotReadyBucktes}
                    end
            end
    end.
analyze_status(Node, Latest) ->
    [ActiveRV, InactiveRV, PartialRV] = lists:foldl(
                fun ({_OtherNode, inactive, _}, [Active, Inactive, Partial]) ->
                        %% Consider view of only those nodes which are active.
                        [Active, Inactive, Partial];
                    ({OtherNode, _, AllStatus}, [Active, Inactive, Partial]) ->
                        Status = proplists:get_value(Node, AllStatus, []),
                        State = get_node_state(Node, Status),
                        ?log_debug("Partial:~p ~n", [Partial]),
                        ?log_debug("OtherNode:~p Node:~p Status:~p ~n State:~p ~n", [OtherNode, Node, Status, State]),
                        case State of
                            active ->
                                [[Node | Active], Inactive, Partial];
                            inactive ->
                                [Active, [Node | Inactive], Partial];
                            Other ->
                                [Active, Inactive, [{Node, Other} | Partial]]
                        end
                end, [[], [], []], Latest),
    case ActiveRV of
        [] ->
            case PartialRV of
                [] ->
                    unhealthy;
                _ ->
                    PartialRV
            end;
        _ ->
            %% Atleast one node has reported that all buckets on the node are
            %% active.
            healthy
    end.

%% Internal functions
check_local_node_status(Nodes) ->
    case lists:keyfind(node(), 1, Nodes) of
        false ->
            %% Local node is missing.
            %% Get status using old method i.e. from ns_memcached
            add_local_node_status(Nodes);
        {_, NodeInfo} ->
            %% Get status using old method i.e. from ns_memcached for
            %% buckets that do not show DCP traffic activity.
            Buckets = proplists:get_value(buckets, NodeInfo),
            ActiveBuckets = lists:filtermap(
                                    fun ({Bucket, State, _}) ->
                                        case State of
                                            active ->
                                                {true, Bucket};
                                            _ ->
                                                false
                                        end
                                    end, Buckets),
            ?log_debug("ActiveBuckets:~p ~n", [ActiveBuckets]),
            case ns_memcached:active_buckets() -- ActiveBuckets of
                [] ->
                    Nodes;
                _ ->
                    update_local_node_status(Nodes, ActiveBuckets)
            end
    end.

add_local_node_status(Nodes) ->
    [get_local_node_status([]) | Nodes].

update_local_node_status(Nodes, ActiveBuckets) ->
    lists:keyreplace(node(), 1, Nodes, get_local_node_status(ActiveBuckets)).

get_bucket_state(Bucket, ReadyBuckets, ActiveBuckets) ->
    case lists:member(Bucket, ActiveBuckets) of
        true ->
            active;
        false ->
            case lists:member(Bucket, ReadyBuckets) of
                true ->
                    ready;
                false ->
                    case ns_memcached:connected(node(), Bucket) of
                        true ->
                            warmed_but_not_ready;
                        false ->
                            not_ready
                    end
            end
    end.
get_local_node_status(ActiveBuckets) ->
    AllBuckets = ns_memcached:active_buckets(),
    ReadyBuckets = ns_memcached:warmed_buckets(),
    %% Node is considered active if atleast one bucket is ready.
    NodeState = case ReadyBuckets of
                    [] ->
                        no_ready_buckets;
                    _ ->
                        active
                end,
    BAcc = lists:foldl(
                fun (Bucket, Acc) ->
                    BState = get_bucket_state(Bucket,
                                              ReadyBuckets, ActiveBuckets),
                    [{Bucket, BState, erlang:now()} | Acc]
                end, [], AllBuckets),
   {node(), [{node_state, NodeState}, {buckets, BAcc}]}.



