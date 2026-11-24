-module(eventcore_memory_ffi).
-behaviour(gen_server).
-export([start/1, call/2, stop/1, acquire/2, init/1, handle_call/3, handle_cast/2, handle_info/2]).

start(Initial) ->
    case gen_server:start(?MODULE, {self(), Initial}, []) of
        {ok, Pid} -> {ok, Pid};
        {error, _Reason} -> {error, nil}
    end.

call(Pid, Operation) ->
    try gen_server:call(Pid, Operation, infinity) of
        Reply -> {ok, Reply}
    catch
        exit:_Reason -> {error, nil}
    end.

stop(Pid) ->
    try gen_server:stop(Pid) of
        ok -> {ok, nil}
    catch
        exit:_Reason -> {error, nil}
    end.

acquire(Pid, Name) ->
    try gen_server:call(Pid, {acquire, Name}, infinity) of
        {ok, Token} ->
            {ok, fun() ->
                try gen_server:call(Pid, {release, Name, Token}, infinity)
                catch exit:_ -> nil
                end
            end};
        Error -> Error
    catch
        exit:_ -> {error, coordinator_unavailable}
    end.

init({Owner, Initial}) ->
    {ok, {erlang:monitor(process, Owner), Initial, #{}}}.

handle_call({acquire, Name}, {Holder, _}, {OwnerMonitor, State, Leases}) ->
    case live_lease(maps:find(Name, Leases)) of
        true -> {reply, {error, already_leading}, {OwnerMonitor, State, Leases}};
        false ->
            Token = erlang:monitor(process, Holder),
            {reply, {ok, Token}, {OwnerMonitor, State, Leases#{Name => {Holder, Token}}}}
    end;
handle_call({release, Name, Token}, _From, {OwnerMonitor, State, Leases}) ->
    case maps:find(Name, Leases) of
        {ok, {_, Token}} ->
            erlang:demonitor(Token, [flush]),
            {reply, nil, {OwnerMonitor, State, maps:remove(Name, Leases)}};
        _ -> {reply, nil, {OwnerMonitor, State, Leases}}
    end;
handle_call(Operation, _From, {OwnerMonitor, State, Leases}) ->
    {Updated, Reply} = Operation(State),
    {reply, Reply, {OwnerMonitor, Updated, Leases}}.

handle_cast(_Message, State) -> {noreply, State}.

handle_info({'DOWN', OwnerMonitor, process, _Owner, _Reason},
            {OwnerMonitor, _, _} = State) ->
    {stop, normal, State};
handle_info({'DOWN', HolderMonitor, process, _Holder, _Reason},
            {OwnerMonitor, State, Leases}) ->
    Remaining = maps:filter(fun(_, {_, Token}) -> Token =/= HolderMonitor end, Leases),
    {noreply, {OwnerMonitor, State, Remaining}};
handle_info(_Message, State) -> {noreply, State}.


live_lease(error) -> false;
% The monitor also reports disconnection for a holder on another node.
live_lease({ok, {Holder, _}}) when node(Holder) =/= node() -> true;
live_lease({ok, {Holder, Token}}) ->
    case erlang:is_process_alive(Holder) of
        true -> true;
        false ->
            erlang:demonitor(Token, [flush]),
            false
    end.
