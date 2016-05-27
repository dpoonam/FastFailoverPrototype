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
-module(health_monitor).

-define(INACTIVE_TIME, 2000000). % 2 seconds in microseconds

-compile(export_all).
-include("ns_common.hrl").

get_state(LastHeard) ->
    Diff = timer:now_diff(erlang:now(), LastHeard),
    if Diff =< ?INACTIVE_TIME -> active;
       Diff > ?INACTIVE_TIME -> inactive
    end.

process_nodes_wanted(Nodes, Status) ->
    NewNodes = lists:sort(Nodes),
    CurrentNodes = lists:sort(dict:fetch_keys(Status)),
    ToRemove = ordsets:subtract(CurrentNodes, NewNodes),
    NewStatus = lists:foldl(
          fun (Node, Acc) ->
                  dict:erase(Node, Acc)
          end, Status, ToRemove),
    {NewNodes, NewStatus}.

%% Replace last heard time stamp with time difference
update_ts(LastHeard) ->
    timer:now_diff(erlang:now(), LastHeard).

all_monitors() ->
    [kv, ns_server].

monitor_module(Monitor) ->
    list_to_atom(atom_to_list(Monitor) ++ "_monitor").
