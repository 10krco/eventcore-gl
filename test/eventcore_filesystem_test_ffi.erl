-module(eventcore_filesystem_test_ffi).
-export([temp_root/0, remove_root/1, transaction_paths/1, tamper/1, remove/1,
         copy_transactions/2, copy_replica/2, downgrade_transaction/1, add_malformed_transaction/2,
         open_with_failed_ancestor_sync/2, is_directory/1, check_os_lock/2, check_holder_exit/2, await_lease/2, kill_projection_lock/0, exercise_checkpoint_handoff/6]).

temp_root() ->
    Random = binary:encode_hex(crypto:strong_rand_bytes(16), lowercase),
    Directory = filename:join(<<"/tmp">>, <<"eventcore-journal-", Random/binary>>),
    ok = file:make_dir(Directory),
    Directory.

remove_root(Root) ->
    ok = file:del_dir_r(Root),
    nil.

transaction_paths(Root) ->
    Events = filename:join(Root, <<"events">>),
    {ok, Files} = file:list_dir(Events),
    [{list_to_binary(filename:rootname(File)), filename:join(Events, File)}
        || File <- Files, filename:extension(File) =:= ".txn"].

tamper(Path) ->
    ok = file:write_file(Path, <<"edited">>, [append]),
    nil.

remove(Path) ->
    ok = file:delete(Path),
    nil.

copy_transactions(Source, Destination) ->
    Directory = filename:join(Destination, <<"events">>),
    lists:foreach(fun({_, Path}) ->
        {ok, _} = file:copy(Path, filename:join(Directory, filename:basename(Path)))
    end, transaction_paths(Source)),
    nil.

add_malformed_transaction(Root, Path) ->
    {ok, Bytes} = file:read_file(Path),
    {3, Time, Replica, Nonce, Parents, Bases, [Event], none} = binary_to_term(Bytes, [safe]),
    Invalid = term_to_binary({2, Time, Replica, Nonce, Parents, Bases, [Event | 0]}),
    Id = binary:encode_hex(crypto:hash(sha256, Invalid), lowercase),
    Directory = filename:join(Root, <<"events">>),
    ok = file:write_file(filename:join(Directory, <<Id/binary, ".txn">>), Invalid),
    Id.

open_with_failed_ancestor_sync(Root, Ancestor) ->
    Sync = fun(Directory) when Directory =:= Ancestor -> {error, eio};
              (_) -> ok
           end,
    case eventcore_filesystem_ffi:start(Root, full, Sync) of
        {ok, Server} -> eventcore_filesystem_ffi:stop(Server);
        Error -> Error
    end.

is_directory(Path) -> filelib:is_dir(Path).

copy_replica(Source, Destination) ->
    Local = filename:join(Destination, <<".eventcore">>),
    ok = filelib:ensure_path(Local),
    {ok, _} = file:copy(filename:join(Source, <<".eventcore/replica">>),
                        filename:join(Local, <<"replica">>)),
    nil.

downgrade_transaction(Path) ->
    {ok, Bytes} = file:read_file(Path),
    {3, Time, _, Nonce, Parents, Bases, Events, none} = binary_to_term(Bytes, [safe]),
    Original = term_to_binary({1, Time, Nonce, Parents, Bases, Events}),
    Id = binary:encode_hex(crypto:hash(sha256, Original), lowercase),
    Destination = filename:join(filename:dirname(Path), <<Id/binary, ".txn">>),
    ok = file:write_file(Destination, Original),
    ok = file:delete(Path),
    nil.

check_os_lock(Root, Name) ->
    Stem = binary:encode_hex(crypto:hash(sha256, Name), lowercase),
    Path = filename:join([Root, <<".eventcore">>, <<"projection">>, <<"locks">>,
                          <<Stem/binary, ".lock">>]),
    Executable = os:find_executable("flock"),
    Port = open_port({spawn_executable, Executable},
                     [exit_status, {args, ["-n", "-E", "75", "--", Path, "true"]}]),
    receive
        {Port, {exit_status, 75}} -> nil;
        {Port, {exit_status, Status}} -> error({expected_lock_contention, Status})
    after 5000 ->
        catch port_close(Port),
        error(lock_probe_timeout)
    end.

check_holder_exit(Acquire, Name) ->
    Parent = self(),
    {Holder, Monitor} = spawn_monitor(fun() ->
        {ok, {checked_lease, Release, _, _}} = Acquire(Name),
        Parent ! {self(), Release},
        receive stop -> ok end
    end),
    receive
        {Holder, StaleRelease} ->
            {error, already_leading} = Acquire(Name),
            Holder ! stop,
            receive {'DOWN', Monitor, process, Holder, normal} -> ok
            after 5000 -> error(holder_exit_timeout)
            end,
            {checked_lease, Release, _, _} = await_lease(Acquire, Name),
            try
                nil = StaleRelease(),
                {error, already_leading} = Acquire(Name),
                nil
            after
                Release()
            end;
        {'DOWN', Monitor, process, Holder, Reason} -> error({holder_failed, Reason})
    after 5000 ->
        Holder ! stop,
        error(holder_acquire_timeout)
    end.

await_release(_, _, 0) -> error(lease_not_released);
await_release(Acquire, Name, Attempts) ->
    case Acquire(Name) of
        {ok, Lease} -> Lease;
        {error, already_leading} ->
            timer:sleep(10),
            await_release(Acquire, Name, Attempts - 1)
    end.


await_lease(Acquire, Name) -> await_release(Acquire, Name, 250).

kill_projection_lock() ->
    {monitored_by, Monitors} = process_info(self(), monitored_by),
    Workers = [Pid || Pid <- Monitors,
                      process_info(Pid, current_function) =:=
                          {current_function, {eventcore_filesystem_ffi, projection_lock_worker, 5}}],
    Ports = lists:flatmap(fun(Worker) ->
        {links, Links} = process_info(Worker, links),
        [Port || Port <- Links, is_port(Port)]
    end, Workers),
    [Port] = Ports,
    kill_port(Port).

kill_port(Port) ->
    {os_pid, Pid} = erlang:port_info(Port, os_pid),
    Monitor = monitor(port, Port),
    Executable = os:find_executable("kill"),
    Killer = open_port({spawn_executable, Executable},
                       [exit_status, {args, ["-KILL", integer_to_list(Pid)]}]),
    receive {Killer, {exit_status, 0}} -> ok
    after 2000 -> error(lock_kill_failed)
    end,
    receive {'DOWN', Monitor, port, Port, _} -> nil
    after 2000 -> error(lock_port_still_alive)
    end.

exercise_checkpoint_handoff(Acquire, Name, Save, OldPosition, NewPosition, KillFence) ->
    Parent = self(),
    Ref = make_ref(),
    {Old, OldMonitor} = spawn_monitor(fun() ->
        {ok, {checked_lease, Release, _, {Name, Lock, Check}}} = Acquire(Name),
        Pause = fun() ->
            {ok, nil} = Check(),
            {links, Links} = process_info(self(), links),
            [Port] = [Link || Link <- Links, is_port(Link)],
            kill_projection_lock(),
            Parent ! {Ref, old_paused, Port},
            receive {Ref, resume} -> {ok, nil}
            after 3000 -> error(old_resume_timeout)
            end
        end,
        Result = Save(Name, OldPosition, {Name, Lock, Pause}),
        Release(),
        Parent ! {Ref, old_result, Result}
    end),
    Fence = receive {Ref, old_paused, Port} -> Port
    after 4000 -> error(old_pause_timeout)
    end,
    {New, NewMonitor} = spawn_monitor(fun() ->
        {checked_lease, Release, _, {Name, Lock, Check}} = await_lease(Acquire, Name),
        Probe = fun() ->
            Parent ! {Ref, new_checked},
            Check()
        end,
        Parent ! {Ref, new_start},
        Result = Save(Name, NewPosition, {Name, Lock, Probe}),
        Release(),
        Parent ! {Ref, new_result, Result}
    end),
    receive {Ref, new_start} -> ok after 3000 -> error(new_start_timeout) end,
    receive {Ref, new_checked} -> error(replacement_entered_paused_fence)
    after 100 -> ok
    end,
    ExpectedOld = case KillFence of
        true ->
            kill_port(Fence),
            receive {Ref, new_result, {ok, nil}} -> ok
            after 3000 -> error(replacement_blocked_after_fence_loss)
            end,
            {error, lease_lost};
        false -> {ok, nil}
    end,
    Old ! {Ref, resume},
    receive {Ref, old_result, ExpectedOld} -> ok
    after 3000 -> error(old_result_timeout)
    end,
    case KillFence of
        true -> ok;
        false ->
            receive {Ref, new_result, {ok, nil}} -> ok
            after 3000 -> error(new_result_timeout)
            end
    end,
    receive {'DOWN', OldMonitor, process, Old, normal} -> ok
    after 3000 -> error(old_holder_timeout)
    end,
    receive {'DOWN', NewMonitor, process, New, normal} -> ok
    after 3000 -> error(new_holder_timeout)
    end,
    receive {Ref, new_checked} -> nil after 0 -> error(new_check_missing) end.
