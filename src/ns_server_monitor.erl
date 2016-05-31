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
-module(ns_server_monitor).

-define(CHECK_STATUS_PERIOD, 1000). % 1 second

-include("ns_common.hrl").

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
%% API
-export([get_nodes/0, get_node/1, update_node_status/1,
         get_status/1, analyze_status/2]).

-record(state, {
          nodes :: dict(),
          nodes_wanted :: [node()]
         }).

%% gen_server handlers

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:process_flag(priority, high),
    timer2:send_interval(?CHECK_STATUS_PERIOD, check_status),
    self() ! check_status,
    ns_pubsub:subscribe_link(ns_config_events, fun handle_config_event/2, empty),
    {ok, #state{nodes=dict:new(),
                nodes_wanted=ns_node_disco:nodes_wanted()}}.

handle_config_event({nodes_wanted, _} = Msg, State) ->
    ns_server_monitor ! Msg,
    State;
handle_config_event(_, State) ->
    State.

handle_call({get_node, Node}, _From, #state{nodes=Nodes} = State) ->
    RV = case dict:find(Node, Nodes) of
             {ok, Status} ->
                 Status;
             _ ->
                unknown 
         end,
    {reply, RV, State};

handle_call(get_nodes, _From, #state{nodes=Nodes} = State) ->
    {reply, dict:to_list(Nodes), State};

handle_call({update_node_status, Node}, _From, #state{nodes=Nodes} = State) ->
    NewNodes = dict:store(Node, {active, erlang:now()}, Nodes),
    {reply, NewNodes, State#state{nodes=NewNodes}}.

handle_cast(Msg, State) ->
    ?log_debug("Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(check_status,
            #state{nodes=Nodes, nodes_wanted = NodesWanted} = State) ->
    NewList = lists:foldl(
                    fun (Node, Acc) ->
                        LocalNode = Node =:= node(),
                        NS = case LocalNode of
                                true ->
                                    {active, erlang:now()};
                                false ->
                                    case dict:find(Node, Nodes) of
                                        {ok, {_, LastHeard}} ->
                                            {health_monitor:get_state(LastHeard), LastHeard};
                                        _ ->
                                             unknown
                                    end
                            end,
                        [{Node, NS} | Acc]
                    end, [], NodesWanted),
    NewNodes = dict:from_list(NewList),
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

update_node_status(Node) ->
    try gen_server:call(?MODULE, {update_node_status, Node}) of
        Status -> Status
    catch
        E:R ->
            ?log_debug("Error attempting to update node ~p: ~p", [Node, {E, R}]),
            []
    end.

get_status(Node) ->
    case get_node(Node) of
        unknown ->
            [];
        {State, LastHeard} ->
            {State, LastHeard}
    end.

get_node_state(Status) ->
    case proplists:get_value(ns_server, Status, unknown) of
        unknown ->
            inactive;
        [] ->
            inactive;
        {State, _} ->
            State
    end.
%% A node, say NodeA, is considered healthy if ns_server on all nodes say 
%% NodeA is active.
%% NodeA is unhealthy if all nodes say it is inactive.
%% If only a subset of nodes say NodeA is active then it is pontentially
%% a network partition or flaky communication.
analyze_status(Node, Latest) ->
    [HealthyM, UnhealthyM] = lists:foldl(
                fun ({_OtherNode, inactive, _}, [Healthy, Unhealthy]) ->
                        %% Consider view of only those nodes which are active.
                        [Healthy, Unhealthy];
                    ({OtherNode, _, AllStatus}, [Healthy, Unhealthy]) ->
                        NodeStatus = proplists:get_value(Node, AllStatus, []),
                        State = get_node_state(NodeStatus), 
                        case State of
                            active ->
                                [[OtherNode | Healthy], Unhealthy];
                            _ ->
                                [Healthy, [OtherNode | Unhealthy]]
                        end
                end, [[], []], Latest),
   case UnhealthyM of
        [] ->
            %% All nodes say the node is healthy
            healthy;
        _ ->
            case HealthyM of
                [] ->
                    %% All nodes say the node is unhealthy
                    unhealthy;
                _ ->
                    {potential_network_partition, lists:sort(UnhealthyM)}
            end
    end.
