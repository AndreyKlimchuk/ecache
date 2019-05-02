-module(ecache_server).

-behaviour(gen_server).

-include("ecache.hrl").

-export([start_link/3, start_link/4, start_link/5, start_link/6]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).
-export([total_size/1, stats/1]).

-record(cache, {name :: atom(),
                table_pad = 0 :: non_neg_integer(),
                data_module :: module(),
                reaper :: undefined|pid(),
                data_accessor :: atom(),
                size = unlimited :: unlimited|non_neg_integer(),
                pending = #{} :: map(),
                found = 0 :: non_neg_integer(),
                launched = 0 :: non_neg_integer(),
                policy = mru :: mru|actual_time,
                ttl = unlimited :: unlimited|non_neg_integer()}).

% make 8 MB cache
start_link(Name, Mod, Fun) -> start_link(Name, Mod, Fun, 8).

% make 5 minute expiry cache
start_link(Name, Mod, Fun, Size) -> start_link(Name, Mod, Fun, Size, 300000).

% make MRU policy cache
start_link(Name, Mod, Fun, Size, Time) -> start_link(Name, Mod, Fun, Size, Time, mru).

start_link(Name, Mod, Fun, Size, Time, Policy) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, Mod, Fun, Size, Time, Policy], []).

total_size(Name) -> gen_server:call(Name, total_size).

stats(Name) -> gen_server:call(Name, stats).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------

init([Name, Mod, Fun, Size, Time, Policy]) when is_atom(Mod), is_atom(Fun), is_atom(Policy),
                                                Time =:= unlimited orelse is_integer(Time) andalso Time > 0 ->
    process_flag(trap_exit, true),
    ets:new(Name, [set, named_table, compressed, public, % public because we spawn writers
                   {keypos, #datum.key},    % use Key stored in record
                   {read_concurrency, true},
                   {write_concurrency, true}]),
    init(#cache{name = Name,
                table_pad = ets:info(Name, memory),
                data_module = Mod,
                data_accessor = Fun,
                size = Size,
                policy = Policy,
                ttl = Time});
init(#cache{size = unlimited} = State) -> {ok, State};
init(#cache{name = Name, size = Size} = State) when is_integer(Size), Size > 0 ->
    SizeBytes = Size * (1024 * 1024),
    {ok, State#cache{reaper = start_reaper(Name, SizeBytes), size = SizeBytes}}.

handle_call({launch, {ecache_plain, Key} = UseKey}, _, #cache{data_module = M, data_accessor = F} = State) ->
    generic_launch(UseKey, State, M, F, Key);
handle_call({launch, {ecache_multi, {M, F, Key}} = UseKey}, _, State) ->
    generic_launch(UseKey, State, M, F, Key);

handle_call(total_size, _From, #cache{} = State) -> {reply, cache_bytes(State), State};
handle_call(stats, _From, #cache{name = Tab,
                                 found = Found, launched = Launched, policy = Policy, ttl = TTL} = State) ->
    EtsInfo = ets:info(Tab),
    {reply,
     [{cache_name, proplists:get_value(name, EtsInfo)},
      {memory_size_bytes, cache_bytes(State, proplists:get_value(memory, EtsInfo))},
      {datum_count, proplists:get_value(size, EtsInfo)},
      {found, Found}, {launched, Launched},
      {policy, Policy}, {ttl, TTL}],
     State};
handle_call(reap_oldest, _From, #cache{name = Tab} = State) ->
    DatumNow = #datum{last_active = ecache:timestamp()},
    LeastActive = ets:foldl(fun(#datum{last_active = LA} = A, #datum{last_active = Acc}) when LA < Acc -> A;
                               (_, Acc) -> Acc
                            end, DatumNow, Tab),
    LeastActive =:= DatumNow orelse delete_object(Tab, LeastActive),
    {reply, cache_bytes(State), State};
handle_call(Arbitrary, _From, State) -> {reply, {arbitrary, Arbitrary}, State}.

handle_cast({launched, Key}, #cache{launched = Launched} = State) ->
    erase(pd_key(Key)),
    {noreply, State#cache{launched = Launched + 1}};
handle_cast({launch_failed, Key}, State) ->
    erase(pd_key(Key)),
    {noreply, State};
handle_cast(found, #cache{found = Found} = State) ->
    {noreply, State#cache{found = Found + 1}};
handle_cast(Req, State) ->
    error_logger:warning_msg("Other cast of: ~p~n", [Req]),
    {noreply, State}.

handle_info({'EXIT', Pid, _Reason}, #cache{reaper = Pid, name = Name, size = Size} = State) ->
    {noreply, State#cache{reaper = start_reaper(Name, Size)}};
handle_info({'EXIT', _Pid, _Reason}, State) -> {noreply, State};
handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State) -> {noreply, State};
handle_info(Info, State) ->
    error_logger:warning_msg("Other info of: ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, #cache{reaper = Pid}) when is_pid(Pid) -> gen_server:stop(Pid);
terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% ===================================================================
%% Private
%% ===================================================================

cache_bytes(#cache{name = Tab} = State) -> cache_bytes(State, ets:info(Tab, memory)).

cache_bytes(#cache{table_pad = TabPad}, Mem) -> (Mem - TabPad) * erlang:system_info(wordsize).

delete_object(Index, #datum{reaper = Reaper} = Datum) ->
    ecache_reaper:kill_ttl_reaper(Reaper),
    ets:delete_object(Index, Datum).

-ifdef(OTP_RELEASE).
-if (?OTP_RELEASE >= 21).
-define(EXCEPTION(Class, Reason, Stacktrace), Class:Reason:Stacktrace).
-define(GET_STACK(Stacktrace), Stacktrace).
-endif.
-endif.
-ifndef(EXCEPTION).
-define(EXCEPTION(Class, Reason, _), Class:Reason).
-endif.
-ifndef(GET_STACK).
-define(GET_STACK(_), erlang:get_stacktrace()).
-endif.

generic_launch(UseKey, #cache{name = Tab} = State, M, F, Key) ->
    Pid = case get(pd_key(UseKey)) of
              Launcher when is_pid(Launcher) -> Launcher;
              undefined ->
                  #cache{ttl = TTL, policy = Policy} = State,
                  P = self(),
                  Launcher = spawn(fun() ->
                                       process_flag(trap_exit, true),
                                       R = case launch_datum(Key, Tab, M, F, TTL, Policy, UseKey) of
                                               {ok, _} = Data ->
                                                   gen_server:cast(P, {launched, UseKey}),
                                                   Data;
                                               Error ->
                                                   gen_server:cast(P, {launch_failed, UseKey}),
                                                   ets:delete(Tab, UseKey),
                                                   {error, Error}
                                           end,
                                       exit(R)
                                   end),
                  ets:insert_new(Tab, #datum{key = UseKey, mgr = Launcher}),
                  put(pd_key(UseKey), Launcher),
                  Launcher
          end,
    {reply, {ok, Pid}, State}.

launch_datum(Key, Index, Module, Accessor, TTL, Policy, UseKey) ->
    try Module:Accessor(Key) of
        CacheData ->
            ets:insert(Index, launch_datum_ttl_reaper(Index, UseKey, create_datum(UseKey, CacheData, TTL, Policy))),
            {ok, CacheData}
    catch
        ?EXCEPTION(How, What, Stacktrace) -> {ecache_datum_error, {{How, What}, ?GET_STACK(Stacktrace)}}
    end.

launch_datum_ttl_reaper(_, _, #datum{remaining_ttl = unlimited} = Datum) -> Datum;
launch_datum_ttl_reaper(Name, Key, #datum{remaining_ttl = TTL} = Datum) ->
    Datum#datum{reaper = ecache_reaper:start_ttl_reaper(Name, Key, TTL)}.

-compile({inline, [create_datum/4]}).
create_datum(DatumKey, Data, TTL, Type) ->
    Timestamp = ecache:timestamp(),
    #datum{key = DatumKey, data = Data, type = Type,
           started = Timestamp, ttl = TTL, remaining_ttl = TTL, last_active = Timestamp}.

start_reaper(Name, Size) ->
    {ok, Pid} = ecache_reaper:start_link(Name, Size),
    Pid.

pd_key(Key) -> {'$ecache_key', Key}.
-compile({inline, [pd_key/1]}).
