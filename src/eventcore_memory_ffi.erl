-module(eventcore_memory_ffi).
-behaviour(gen_server).
-export([start/1, call/2, stop/1, init/1, handle_call/3, handle_cast/2, handle_info/2]).

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

init({Owner, Initial}) ->
    {ok, {erlang:monitor(process, Owner), Initial}}.

handle_call(Operation, _From, {OwnerMonitor, State}) ->
    {Updated, Reply} = Operation(State),
    {reply, Reply, {OwnerMonitor, Updated}}.

handle_cast(_Message, State) -> {noreply, State}.

handle_info({'DOWN', OwnerMonitor, process, _Owner, _Reason}, {OwnerMonitor, _} = State) ->
    {stop, normal, State};
handle_info(_Message, State) -> {noreply, State}.
