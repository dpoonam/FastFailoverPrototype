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

-define(INACTIVE_TIME, 5000000). % 5 seconds in microseconds
-define(HEARTBEAT_PERIOD, 1000). % 1 second

-include("ns_common.hrl").

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
%% API
-export([get_nodes/0, get_node/1]).

-record(state, {
          nodes :: dict()
         }).

%% gen_server handlers

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:process_flag(priority, high),
    timer2:send_interval(?HEARTBEAT_PERIOD, send_heartbeat),
    self() ! send_heartbeat,
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

handle_cast({heartbeat, Node, Status}, State) ->
    ?log_debug("Received heartbeat from ~p ~n ~p ", [Node, Status]),
    {noreply, State};
handle_cast(Msg, State) ->
    ?log_debug("Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(send_heartbeat, State) ->
    AllNodes = [node() | nodes()],
    Status = update_status(State, AllNodes),
    catch misc:parallel_map(
       fun (N) ->
          gen_server:cast({node_monitor, N}, {heartbeat, node(), Status})
       end, AllNodes, ?HEARTBEAT_PERIOD - 1000),
    NewNodes = dict:from_list(Status),
    {noreply, State#state{nodes=NewNodes}};

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

%% Internal functions

update_status(State, AllNodes) ->
    update_node_status(State, AllNodes, []).

update_node_status(_State, [], Acc) ->
    Acc;
update_node_status(#state{nodes=NodesDict} = State, [Node | Rest], Acc) ->
    GlobalStatus = case dict:find(Node, NodesDict) of
                    {ok, V} ->
                        V;
                    error ->
                        []
                end,
    NewStatus = {Node, get_all_status([kv, ns_server], Node, GlobalStatus, [])},
    update_node_status(State, Rest, [NewStatus | Acc]).

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
    LocalNode = Node =:= node(),
    %% ns-server is active for local node.
    %% This will change if we move node_monitor outside ns-server.
    case LocalNode of
        true ->
            active;
        false ->
            case GlobalStatus of
                [] ->
                    unknown;
                _ ->
                    proplists:get_value(ns_server, GlobalStatus, unknown)
            end
    end.
