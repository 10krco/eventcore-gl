-module(eventcore_postgres_test_ffi).
-export([with_database/1, using_pool/2, check_operations/1]).

with_database(Work) ->
    Suffix = binary:encode_hex(crypto:strong_rand_bytes(12), lowercase),
    Root = "/tmp/eventcore-pg-" ++ binary_to_list(Suffix),
    Data = filename:join(Root, "data"),
    Log = filename:join(Root, "postgres.log"),
    ok = file:make_dir(Root),
    try
        ok = file:change_mode(Root, 8#700),
        run("initdb", ["-D", Data, "--no-locale", "--encoding=UTF8",
                       "--auth=trust", "--username=eventcore"]),
        run("pg_ctl", ["-D", Data, "-l", Log, "-w", "-t", "10", "-o",
                       "-h '' -k " ++ Root, "start"]),
        Work(list_to_binary(Root))
    after
        case filelib:is_file(filename:join(Data, "postmaster.pid")) of
            true -> run("pg_ctl", ["-D", Data, "-w", "-t", "10", "-m", "immediate", "stop"]);
            false -> ok
        end,
        ok = file:del_dir_r(Root)
    end.

using_pool(Pid, Work) ->
    try Work()
    after
        unlink(Pid),
        gen_server:stop(Pid)
    end.

run(Name, Args) ->
    case os:find_executable(Name) of
        false -> error({postgres_test_executable_missing, Name});
        Executable ->
            Port = open_port({spawn_executable, Executable},
                             [binary, exit_status, use_stdio, stderr_to_stdout, {args, Args}]),
            collect(Port, Name, [])
    end.

collect(Port, Name, Output) ->
    receive
        {Port, {data, Bytes}} -> collect(Port, Name, [Bytes | Output]);
        {Port, {exit_status, 0}} -> ok;
        {Port, {exit_status, Code}} ->
            error({postgres_test_command_failed, Name, Code,
                   iolist_to_binary(lists:reverse(Output))})
    after 15000 ->
        port_close(Port),
        error({postgres_test_command_timed_out, Name})
    end.

check_operations(Work) ->
    Module = eventcore_postgres_ffi,
    Pid = self(),
    Filter = fun
        (#{msg := {report, #{operation := Operation}}, meta := #{pid := Pid0}}, Pid0)
            when is_binary(Operation) ->
            Pid0 ! {eventcore_postgres_operation, Operation},
            stop;
        (_, _) -> ignore
    end,
    Previous = logger:get_module_level([Module]),
    ok = logger:set_module_level(Module, info),
    ok = logger:add_primary_filter(eventcore_postgres_test, {Filter, Pid}),
    try
        Work(),
        Operations = drain_operations([]),
        true = lists:all(fun(Name) -> lists:member(Name, Operations) end,
                         [<<"postgres.read_stream">>, <<"postgres.read_stream_after">>,
                          <<"postgres.append_events">>]),
        nil
    after
        logger:remove_primary_filter(eventcore_postgres_test),
        case Previous of
            [] -> logger:unset_module_level(Module);
            [{Module, Level}] -> logger:set_module_level(Module, Level)
        end
    end.

drain_operations(Operations) ->
    receive
        {eventcore_postgres_operation, Operation} -> drain_operations([Operation | Operations])
    after 0 -> Operations
    end.
