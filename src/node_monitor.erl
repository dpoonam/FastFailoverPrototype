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
-module(node_monitor).

-define(HEARTBEAT_PERIOD, 1000). % 1 second
-include("ns_common.hrl").

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
%% API
-export([get_nodes/0, get_node/1, get_node_status/1,
         get_cluster_status/0]).

-record(state, {
          nodes :: dict(),
          nodes_wanted :: [node()]
         }).

%% gen_server handlers

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:process_flag(priority, high),
    timer2:send_interval(?HEARTBEAT_PERIOD, send_heartbeat),
    self() ! send_heartbeat,
    ns_pubsub:subscribe_link(ns_config_events, fun handle_config_event/2, empty),
    {ok, #state{nodes=dict:new(),
                nodes_wanted=ns_node_disco:nodes_wanted()}}.


handle_config_event({nodes_wanted, _} = Msg, State) ->
    node_monitor ! Msg,
    State;
handle_config_event(_, State) ->
    State.

%% TODO: Move to node status analyzer. Also breakup the function.
handle_call({get_node_status, Node}, _From, #state{nodes=Nodes} = State) ->
    %% Get local node's view of the status of the specified Node.
    RV = case dict:find(node(), Nodes) of
             {ok, Status} ->
                case lists:keyfind(Node, 1, Status) of
                    false ->
                        <<"unhealthy">>;
                    {Node, NodeStatus} ->
                      NSState = get_ns_server_state(NodeStatus),
                      KVState = get_kv_state(Node, NodeStatus),
                      get_node_state(NSState, KVState)
                end;
             _ ->
                <<"unhealthy">>
         end,
    {reply, RV, State};

handle_call({get_node, Node}, _From, #state{nodes=Nodes} = State) ->
    %% Get local node's view of the status of the specified Node.
    RV = case dict:find(node(), Nodes) of
             {ok, Status} ->
                 lists:keyfind(Node, 1, Status);
             _ ->
                 []
         end,
    {reply, RV, State};

handle_call(get_nodes, _From, #state{nodes=Nodes} = State) ->
    NodeStatus = case dict:find(node(), Nodes) of
                    {ok, Status} ->
                        Status;
                    _ ->
                        []
                end,
    {reply, NodeStatus, State};

handle_call(get_cluster_status, _From, #state{nodes=Nodes} = State) ->
    {reply, dict:to_list(Nodes), State}.

handle_cast({heartbeat, Node, Status}, #state{nodes = Nodes} = State) ->
    ns_server_monitor:update_node_status(Node),
    NewNodes = dict:store(Node, Status, Nodes),
    {noreply, State#state{nodes=NewNodes}};
handle_cast(Msg, State) ->
    ?log_debug("Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(send_heartbeat,
            #state{nodes = Nodes, nodes_wanted = NodesWanted} = State) ->
    Status = update_status(NodesWanted),
    send_heartbeat(Status, NodesWanted),
    NewNodes = dict:store(node(), Status, Nodes),
    {noreply, State#state{nodes=NewNodes}};

handle_info({nodes_wanted, NewNodes0}, #state{nodes=Statuses} = State) ->
    {NewNodes, NewStatuses} = health_monitor:process_nodes_wanted(NewNodes0,
                                                                  Statuses),
    {noreply, State#state{nodes=NewStatuses, nodes_wanted=NewNodes}};
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
            []
    end.

get_node(Node) ->
    try gen_server:call(?MODULE, {get_node, Node}) of
        Status -> Status
    catch
        E:R ->
            ?log_debug("Error attempting to get node ~p: ~p", [Node, {E, R}]),
            []
    end.

get_node_status(Node) ->
    try gen_server:call(?MODULE, {get_node_status, Node}) of
        Status -> Status
    catch
        E:R ->
            ?log_debug("Error attempting to get node ~p: ~p", [Node, {E, R}]),
            []
    end.

get_cluster_status() ->
    try gen_server:call(?MODULE, get_cluster_status) of
        Status -> Status
    catch
        E:R ->
            ?log_debug("Error attempting to get cluster view ~p", [{E, R}]),
            []
    end.
%% Internal functions

update_status(NodesWanted) ->
    lists:foldl(
        fun (Node, Acc) ->
                NewStatus = get_all_status(all_monitors(), Node, []),
                [{Node, NewStatus} | Acc]
        end, [], NodesWanted).

get_all_status([], _, Acc) ->
    Acc;
get_all_status([kv | Rest], Node, Acc) ->
    KVStatus = {kv, get_bucket_status(Node)},
    get_all_status(Rest, Node, [KVStatus | Acc]);
get_all_status([ns_server | Rest], Node, Acc) ->
    NSStatus = {ns_server, get_ns_server_status(Node)},
    get_all_status(Rest, Node, [NSStatus | Acc]).

get_bucket_status(Node) ->
    case kv_monitor:get_node(Node) of
        [] ->
            %% Local node is not monitoring KV status for this Node.
            [];
        [_, {buckets, BucketList}] ->
            lists:foldl(
                fun ({Bucket, State, LastHeard}, Acc) ->
                   [{Bucket, State, update_ts(LastHeard)} | Acc]
                end, [], BucketList)
    end.

get_ns_server_status(Node) ->
    case ns_server_monitor:get_node(Node) of
        unknown ->
            [];
        {State, LastHeard} ->
            {State, update_ts(LastHeard)}
    end.

%% Replace last heard time stamp with time difference
update_ts(LastHeard) ->
    timer:now_diff(erlang:now(), LastHeard).

skip_heartbeats_to() ->
    case testpoint:get(skip_heartbeat_to) of
        false ->
            [];
        SkipList ->
            SkipList
    end.
send_heartbeat(Status, NodesWanted) ->
    SkipList = skip_heartbeats_to(),
    SendList = NodesWanted -- SkipList,
    catch misc:parallel_map(
       fun (N) ->
            gen_server:cast({?MODULE, N}, {heartbeat, node(), Status})
       end, SendList, ?HEARTBEAT_PERIOD - 10).

get_ns_server_state(Status) ->
    case proplists:get_value(ns_server, Status, unknown) of
        unknown ->
            inactive;
        [] ->
            inactive;
        {State, _} ->
            State
    end.

get_kv_state(Node, Status) ->
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
            case ordsets:is_subset(lists:sort(NodeBuckets),
                                   lists:sort(ReadyBuckets)) of
                true ->
                    active;
                false ->
                    case ReadyBuckets of
                        [] ->
                            warmup;
                        _ ->
                            partial_ready
                    end
            end
    end.

%% TODO: Rewrite.
%% Compare ns-server and KV state and return appropriate node state.
%% get_node_state(ns_server_state, kv_state)
get_node_state(active, active) ->
    <<"healthy">>;
get_node_state(active, warmup) ->
    <<"warmup">>;
get_node_state(active, partial_ready) ->
    <<"warmup">>;
get_node_state(active, inactive) ->
    <<"needs_attention">>;
get_node_state(inactive, inactive) ->
    <<"unhealthy">>;
get_node_state(inactive, warmup) ->
    <<"unhealthy">>;
get_node_state(inactive, _) ->
    <<"needs_attention">>.


all_monitors() ->
    [kv, ns_server].
