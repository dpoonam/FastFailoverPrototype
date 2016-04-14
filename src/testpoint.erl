%% @author Couchbase <info@couchbase.com>
%% @copyright 2016 Couchbase, Inc.
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

-module(testpoint).

-define(TESTPOINT_STORE, testpoints).

-include("ns_common.hrl").

%% APIs

-export([get/1,
         set/2,
         delete/1]).

%% Exported APIs

get(Key) ->
    simple_store:get(?TESTPOINT_STORE, Key).

%% TODO: No need to persist the TPs.
%% Add function for non-persistent set() in simple_store.
set(Key, Value) ->
    simple_store:set(?TESTPOINT_STORE, Key, Value).

delete(Key) ->
    simple_store:delete(?TESTPOINT_STORE, Key).
