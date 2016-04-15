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

-define(HEARTBEAT_PERIOD, 2000). % 2 second
-include("ns_common.hrl").

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
%% API
-export([get_nodes/0, get_node/1, get_node_status/1]).

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
    ns_pubsub:subscribe_link(ns_config_events, fun handle_config_event/2),
    {ok, #state{nodes=dict:new(),
                nodes_wanted=ns_node_disco:nodes_wanted()}}.


handle_config_event({nodes_wanted, _} = Msg, State) ->
    node_monitor ! Msg,
    State;
handle_config_event(_, State) ->
    State.

%% TODO: Move to node status analyzer. Also breakup the function.
handle_call({get_node_status, Node}, _From, #state{nodes=Nodes} = State) ->
    RV = case dict:find(Node, Nodes) of
             {ok, Status} ->
                NSState = get_ns_server_state(Status),
                KVState = get_kv_state(Node, Status),
                get_node_state(NSState, KVState);
             _ ->
                <<"unhealthy">>
         end,
    {reply, RV, State};

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

handle_cast({heartbeat, Node, Status},
            #state{nodes_wanted = NodesWanted} = State) ->
    ?log_debug("Received heartbeat from ~p ~n ~p ", [Node, Status]),
    NewStatus = process_heartbeat(Node, Status, NodesWanted),
    NewNodes = dict:from_list(NewStatus),
    {noreply, State#state{nodes=NewNodes}};
handle_cast(Msg, State) ->
    ?log_debug("Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(send_heartbeat, #state{nodes_wanted = NodesWanted} = State) ->
    Status = update_status(State, NodesWanted),
    send_heartbeat(Status, NodesWanted),
    NewStatus = dict:from_list(Status),
    {noreply, State#state{nodes=NewStatus}};

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

get_node_status(Node) ->
    try gen_server:call(?MODULE, {get_node_status, Node}) of
        Status -> Status
    catch
        E:R ->
            ?log_debug("Error attempting to get node ~p: ~p", [Node, {E, R}]),
            []
    end.
%% Internal functions

update_status(#state{nodes=NodesDict}, NodesWanted) ->
    update_node_status(dict:to_list(NodesDict), NodesWanted).

update_node_status(NodesList, NodesWanted) ->
    lists:foldl(
        fun (Node, Acc) ->
                GlobalStatus = case lists:keyfind(Node, 1, NodesList) of
                                false ->
                                    [];
                                {Node, Status} ->
                                    Status
                                end,
                NewStatus =  get_all_status([kv, ns_server],
                                            Node, GlobalStatus, []),
                [{Node, NewStatus} | Acc]
        end, [], NodesWanted).

get_all_status([], _, _, Acc) ->
    Acc;
get_all_status([kv | Rest], Node, GlobalStatus, Acc) ->
    KVStatus = {kv, get_bucket_status(Node, GlobalStatus)},
    get_all_status(Rest, Node, GlobalStatus, [KVStatus | Acc]);
get_all_status([ns_server | Rest], Node, GlobalStatus, Acc) ->
    NSStatus = {ns_server, get_ns_server_status(Node, GlobalStatus)},
    get_all_status(Rest, Node, GlobalStatus, [NSStatus | Acc]).

get_bucket_status(Node, GlobalStatus) ->
    GlobalBuckets = proplists:get_value(kv, GlobalStatus, unknown),
    case kv_monitor:get_node(Node) of
        [] ->
            %% Local node is not monitoring KV status for this Node.
            GlobalBuckets;
        LocalKV ->
            LocalBuckets = proplists:get_value(buckets, LocalKV, unknown),
            case GlobalBuckets of
                unknown ->
                    LocalBuckets;
                _ ->
                    get_latest_bucket_status(LocalBuckets, GlobalBuckets)
            end
    end.

%% For a node, say Node A, some remote node might have more uptodate view
%% of the state of buckets on Node A than we do.
%% Compare the global view with the local one and return the most current
%% info.
get_latest_bucket_status(LocalBuckets, GlobalBuckets) ->
    {LB, GB} = lists:foldl(
           fun ({Bucket, _, LastHeard} = LocalBucket, {AccL, AccG}) ->
                   case lists:keyfind(Bucket, 1, GlobalBuckets) of
                       false ->
                          {[LocalBucket | AccL], AccG};
                       {_, _, GlobalLastHeard} = GlobalBucket ->
                               RB = if LastHeard >= GlobalLastHeard -> LocalBucket;
                                  LastHeard < GlobalLastHeard -> GlobalBucket
                               end,
                                {[RB | AccL], lists:keydelete(Bucket, 1, AccG)}
                   end
           end, {[], GlobalBuckets}, LocalBuckets),
    LB ++ GB.

get_ns_server_status(Node, GlobalStatus) ->
    LocalState = ns_server_monitor:get_node(Node),
    case GlobalStatus of
        [] ->
            LocalState;
        _ ->
            case proplists:get_value(ns_server, GlobalStatus, unknown) of
                unknown ->
                    LocalState;
                [] ->
                    LocalState;
                {_, GlobalLastHeard} = GlobalState ->
                    case LocalState of
                        unknown ->
                            GlobalState;
                        {_, LastHeard} ->
                            if LastHeard >= GlobalLastHeard -> LocalState;
                               LastHeard < GlobalLastHeard -> GlobalState
                            end
                    end
            end
    end.

process_status([], _, Acc) ->
    Acc;
process_status([kv | Rest], Status, Acc) ->
    case proplists:get_value(kv, Status, unknown) of
        unknown ->
            process_status(Rest, Status, [{kv, unknown} | Acc]);
        Buckets ->
            %% Time across the nodes may not be in sync.
            %% So, we send the time diff over the wire.
            NewStatus = lists:map(
                            fun ({Bucket, State, LastHeard}) ->
                                    {Bucket, State,
                                     timer:now_diff(erlang:now(), LastHeard)}
                            end, Buckets),
            process_status(Rest, Status, [{kv, NewStatus} | Acc])
    end;
process_status([ns_server | Rest], Status, Acc) ->
    case proplists:get_value(ns_server, Status, unknown) of
        unknown ->
            process_status(Rest, Status, [{ns_server, unknown} | Acc]);
        {State, LastHeard} ->
            NewStatus = {State, timer:now_diff(erlang:now(), LastHeard)},
            process_status(Rest, Status, [{ns_server, NewStatus} | Acc])
    end.

process_all_status(Status) ->
    lists:foldl(
        fun ({Node, NodeStatus}, Acc) ->
            NS1 = process_status([kv, ns_server], NodeStatus, []),
            [{Node, NS1} | Acc]
        end, [], Status).

skip_heartbeats_to() ->
    case testpoint:get(skip_heartbeat_to) of
        false ->
            [];
        SkipList ->
            SkipList
    end.
send_heartbeat(Status, NodesWanted) ->
    %% TODO: Call process_all_status to adjust for time difference among nodes.
    %% Currently assuming that clock on all nodes is in sync.
    %% Status1 = process_all_status(Status),
    SkipList = skip_heartbeats_to(),
    SendList = NodesWanted -- SkipList,
    ?log_debug("Skipping heartbeats to ~p ~n", [SkipList]),
    ?log_debug("Sending heartbeats to ~p ~n", [SendList]),
    catch misc:parallel_map(
       fun (N) ->
            gen_server:cast({?MODULE, N}, {heartbeat, node(), Status})
       end, SendList, ?HEARTBEAT_PERIOD - 10).

process_heartbeat(Node, Status, NodesWanted) ->
    ns_server_monitor:update_node_status(Node),
    update_node_status(Status, NodesWanted).

get_ns_server_state(Status) ->
    case proplists:get_value(ns_server, Status, unknown) of
        unknown ->
            inactive;
        {State, _} ->
            State
    end.

get_kv_state(Node, Status) ->
    case proplists:get_value(kv, Status, unknown) of
        unknown ->
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
