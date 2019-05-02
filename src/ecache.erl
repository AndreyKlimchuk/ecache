-module(ecache).

-include("ecache.hrl").

-export([cache_sup/4, cache_ttl_sup/5]).
-export([dirty/2, dirty/3, dirty_memoize/4, empty/1, get/2, memoize/4]).
-export([stats/1, total_size/1]).
-export([rand/2, rand_keys/2]).
-export([timestamp/0]).

-define(TIMEOUT, infinity).

%% ===================================================================
%% Supervisory helpers
%% ===================================================================

cache_sup(Name, Mod, Fun, Size) ->
    {Name, {ecache_server, start_link, [Name, Mod, Fun, Size]}, permanent, brutal_kill, worker, [ecache_server]}.

cache_ttl_sup(Name, Mod, Fun, Size, TTL) ->
    {Name, {ecache_server, start_link, [Name, Mod, Fun, Size, TTL]}, permanent, brutal_kill, worker, [ecache_server]}.

%% ===================================================================
%% API
%% ===================================================================

get(Name, Key) -> get_value(Name, key(Key)).

memoize(Name, Module, Fun, Key) -> get_value(Name, key(Module, Fun, Key)).

dirty_memoize(Name, Module, Fun, Key) -> delete_datum(Name, key(Module, Fun, Key)).

empty(Name) -> delete_datum(Name).

total_size(Name) -> ecache_server:total_size(Name).

stats(Name) -> ecache_server:stats(Name).

dirty(Name, Key, NewData) -> replace_datum(key(Key), NewData, Name).

dirty(Name, Key) -> delete_datum(Name, key(Key)).

rand(Name, Count) -> get_rand(Name, data, Count).

rand_keys(Name, Count) -> get_rand(Name, keys, Count).

timestamp() -> erlang:monotonic_time(milli_seconds).

%% ===================================================================
%% Internal
%% ===================================================================

get_value(Name, UseKey) ->
    case fetch_data(UseKey, Name) of
        {ok, Data} ->
            gen_server:cast(Name, found),
            Data;
        {ecache, notfound} ->
            {ok, Launcher} = gen_server:call(Name, {launch, UseKey}),
            wait_launcher(Launcher, Name, UseKey);
        {ecache, Launcher} when is_pid(Launcher) ->
            wait_launcher(Launcher, Name, UseKey)
    end.

wait_launcher(Launcher, Name, UseKey) ->
    Ref = monitor(process, Launcher),
    receive
        {'DOWN', Ref, process, _, Reason} -> case Reason of
                                                 {ok, Data} ->
                                                     gen_server:cast(Name, found),
                                                     Data;
                                                 {error, Error} -> Error;
                                                 _noproc -> get_value(Name, UseKey)
                                             end
    end.

update_ttl(Index, #datum{key = Key, ttl = unlimited}) ->
    ets:update_element(Index, Key, {#datum.last_active, timestamp()});
update_ttl(Index, #datum{key = Key, started = Started, ttl = TTL, type = actual_time, reaper = Reaper}) ->
    Timestamp = timestamp(),
    % Get total time in seconds this datum has been running.  Convert to ms.
    % If we are less than the TTL, update with TTL-used (TTL in ms too) else, we ran out of time.  expire on next loop.
    TTLRemaining = case time_diff(Timestamp, Started) of
                       StartedNowDiff when StartedNowDiff < TTL -> TTL - StartedNowDiff;
                       _ -> 0
                   end,
    ping_reaper(Reaper, TTLRemaining),
    ets:update_element(Index, Key, [{#datum.last_active, Timestamp}, {#datum.remaining_ttl, TTLRemaining}]);
update_ttl(Index, #datum{key = Key, ttl = TTL, reaper = Reaper}) ->
    ping_reaper(Reaper, TTL),
    ets:update_element(Index, Key, {#datum.last_active, timestamp()}).


delete_datum(Name) ->
    ecache_reaper:kill_ttl_reapers(ets:select(Name, [{#datum{reaper = '$1', _ = '_'}, [], ['$1']}])),
    ets:delete_all_objects(Name),
    ok.

delete_datum(Name, Key) ->
    case ets:take(Name, Key) of
        [#datum{reaper = Reaper}] -> ecache_reaper:kill_ttl_reaper(Reaper);
        _ -> true
    end,
    ok.

replace_datum(Key, Data, Index) when is_tuple(Key) ->
    ets:update_element(Index, Key, [{#datum.data, Data}, {#datum.last_active, timestamp()}]),
    ok.

get_rand(Tab, Type, Count) ->
    AllKeys = get_all_keys(Tab),
    Length = length(AllKeys),
    lists:map(if
                  Type =:= data -> fun(K) ->
                                       {ok, Data} = fetch_data(K, Tab),
                                       Data
                                   end;
                  Type =:= keys -> fun unkey/1
              end,
              if
                  Length =< Count -> AllKeys;
                    true -> lists:map(fun(K) -> lists:nth(K, AllKeys) end,
                                      lists:map(fun(_) -> crypto:rand_uniform(1, Length) end,
                                                lists:seq(1, Count)))
              end).

fetch_data(Key, Tab) when is_tuple(Key) ->
    case ets:lookup(Tab, Key) of
        [#datum{mgr = undefined, data = Data} = Datum] ->
            update_ttl(Tab, Datum),
            {ok, Data};
        [#datum{mgr = P}] when is_pid(P) -> {ecache, P};
        [] -> {ecache, notfound}
    end.

get_all_keys(Index) -> get_all_keys(Index, [ets:first(Index)]).

get_all_keys(_, ['$end_of_table'|Acc]) -> Acc;
get_all_keys(Index, [Key|_] = Acc) -> get_all_keys(Index, [ets:next(Index, Key)|Acc]).

-compile({inline, [time_diff/2]}).
time_diff(T2, T1) -> T2 - T1.

-compile({inline, [ping_reaper/2]}).
ping_reaper(Reaper, NewTTL) when is_pid(Reaper) -> ecache_reaper:update_ttl_reaper(Reaper, NewTTL);
ping_reaper(_, _) -> ok.

-compile({inline, [key/1, key/3]}).
-compile({inline, [unkey/1]}).
% keys are tagged/boxed so you can't cross-pollute a cache when using
% memoize mode versus the normal one-key-per-arg mode.
% Implication: *always* add key(Key) to keys from a user.  Don't pass user
% created keys directly to ets.
% The boxing overhead on 64 bit systems is: atom + tuple = 8 + 16 = 24 bytes
% The boxing overhead on 32 bit systems is: atom + tuple = 4 +  8 = 12 bytes
key(M, F, A) -> {ecache_multi, {M, F, A}}.
key(Key)     -> {ecache_plain, Key}.
unkey({ecache_plain, Key}) -> Key;
unkey({ecache_multi, {_, _, _} = MFA}) -> MFA.