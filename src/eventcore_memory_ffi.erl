-module(eventcore_memory_ffi).
-behaviour(gen_server).
-export([start/1, call/2, stop/1, init/1, handle_call/3, handle_cast/2]).

start(Initial) ->
    case gen_server:start(?MODULE, Initial, []) of
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

init(Initial) -> {ok, Initial}.

handle_call(Operation, _From, State) ->
    {Updated, Reply} = Operation(State),
    {reply, Reply, Updated}.

handle_cast(_Message, State) -> {noreply, State}.
