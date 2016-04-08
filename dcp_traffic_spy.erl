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
-module(dcp_traffic_spy).

-define(STALE_TIME, 5000000). % 5 seconds in microseconds

-include("ns_common.hrl").

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
%% API
-export([get_nodes/0, get_node/1,
         node_alive/2]).

-record(state, {
          nodes :: dict()
         }).

%% gen_server handlers

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:process_flag(priority, high),
    {ok, #state{nodes=dict:new()}}.

handle_call({get_node, Node}, _From, #state{nodes=Nodes} = State) ->
    RV = case dict:find(Node, Nodes) of
             {ok, Status} ->
                 LiveNodes = [node() | nodes()],
                 annotate_status(Node, Status, now(), LiveNodes);
             _ ->
                 []
         end,
    {reply, RV, State};

handle_call(get_nodes, _From, #state{nodes=Nodes} = State) ->
    Now = erlang:now(),
    LiveNodes = [node()|nodes()],
    Nodes1 = dict:map(
               fun (Node, Status) ->
                       annotate_status(Node, Status, Now, LiveNodes)
               end, Nodes),
    {reply, dict:to_list(Nodes1), State}.

handle_cast({node_alive, Node, NodeInfo}, State) ->
    case lists:member(Node, ns_node_disco:nodes_wanted()) of
        true ->
            {noreply, update_status(Node, NodeInfo, State)};
        false ->
            ?log_debug("Ignoring unknown node ~p", [Node]),
            {noreply, State}
    end;

handle_cast(Msg, State) ->
    ?log_debug("Unexpected cast: ~p", [Msg]),
    {noreply, State}.

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

node_alive(Node, NodeInfo) ->
    gen_server:cast(?MODULE, {node_alive, Node, NodeInfo}).

%% Internal functions

update_status(Node, NodeInfo, State) ->
    Bucket = proplists:get_value(bucket, NodeInfo),
    Type = proplists:get_value(type, NodeInfo),
    Nodes = State#state.nodes,
    Info = {Bucket, {bucket_last_heard, erlang:now()}},
    Buckets = case dict:find(Node, Nodes) of
             {ok, OldList} ->
                    {type, Type} =  lists:keyfind(type, 1, OldList),
                    {buckets, BucketList} = lists:keyfind(buckets, 1, OldList),
                    case lists:keyreplace(Bucket, 1, BucketList, Info) of
                        BucketList ->
                            [Info | BucketList];
                        NewList ->
                            NewList
                    end;
             _ ->
                 [Info]
         end,
    Val = [{type, Type}, {node_last_heard, erlang:now()}, {buckets, Buckets}],
    Nodes1 = dict:store(Node, Val, Nodes),
    State#state{nodes=Nodes1}.

annotate_status(Node, Status, Now, LiveNodes) ->
    LastHeard = proplists:get_value(node_last_heard, Status),
    Stale = case timer:now_diff(Now, LastHeard) of
                T when T > ?STALE_TIME ->
                    [ stale | Status];
                _ -> Status
            end,
    case lists:member(Node, LiveNodes) of
        true ->
            [ {last_update_diff, timer:now_diff(Now, LastHeard)/1000} | Stale];
        false ->
            [ down | Stale ]
    end.
