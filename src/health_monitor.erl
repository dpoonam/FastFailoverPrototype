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

-define(INACTIVE_TIME, 5000000). % 5 seconds in microseconds

-include("ns_common.hrl").

%% API
-export([get_state/1]).

get_state(LastHeard) ->
    Diff = timer:now_diff(erlang:now(), LastHeard),
    if Diff =< ?INACTIVE_TIME -> active;
       Diff > ?INACTIVE_TIME -> inactive
    end.

