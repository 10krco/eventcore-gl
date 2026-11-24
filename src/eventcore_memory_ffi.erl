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
    {ok, {erlang:monitor(process, Owner), Initial, #{}}}.

handle_call({acquire, Name}, {Holder, _}, {OwnerMonitor, State, Leases}) ->
    {Reply, Updated} = eventcore_projection_leases:acquire(Name, Holder, Leases),
    {reply, Reply, {OwnerMonitor, State, Updated}};
handle_call({release, Name, Token}, _From, {OwnerMonitor, State, Leases}) ->
    Updated = eventcore_projection_leases:release(Name, Token, Leases),
    {reply, nil, {OwnerMonitor, State, Updated}};
handle_call(Operation, _From, {OwnerMonitor, State, Leases}) ->
    {Updated, Reply} = Operation(State),
    {reply, Reply, {OwnerMonitor, Updated, Leases}}.

handle_cast(_Message, State) -> {noreply, State}.

handle_info({'DOWN', OwnerMonitor, process, _Owner, _Reason},
            {OwnerMonitor, _, _} = State) ->
    {stop, normal, State};
handle_info({'DOWN', HolderMonitor, process, _Holder, _Reason},
            {OwnerMonitor, State, Leases}) ->
    Remaining = eventcore_projection_leases:down(HolderMonitor, Leases),
    {noreply, {OwnerMonitor, State, Remaining}};
handle_info(_Message, State) -> {noreply, State}.
