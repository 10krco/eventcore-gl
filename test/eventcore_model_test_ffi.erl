-module(eventcore_model_test_ffi).
-export([compile_failure/1]).

compile_failure(Fixture) ->
    Compiler = os:find_executable("gleam"),
    Fixtures = filename:absname("test/fixtures/model_compile_fail"),
    Libraries = filename:absname("build/dev/erlang"),
    Temp = filename:join("build", "model-fixture-" ++ os:getpid() ++ "-" ++
                         integer_to_list(erlang:unique_integer([positive]))),
    ok = file:make_dir(Temp),
    try
        Source = filename:join(Temp, "src"),
        ok = file:make_dir(Source),
        File = binary_to_list(Fixture) ++ ".gleam",
        {ok, _} = file:copy(filename:join(Fixtures, "gleam.toml"),
                            filename:join(Temp, "gleam.toml")),
        {ok, _} = file:copy(filename:join(Fixtures, binary_to_list(Fixture) ++ ".fixture"),
                            filename:join(Source, File)),
        case Fixture of
            <<"missing_field">> ->
                {ok, _} = file:copy("test/model_transfer_example.gleam",
                                    filename:join(Source, "model_transfer_example.gleam"));
            _ -> ok
        end,
        Arguments = ["compile-package", "--target", "erlang", "--no-beam",
                     "--package", filename:absname(Temp), "--out",
                     filename:absname(filename:join(Temp, "out")),
                     "--lib", Libraries],
        Port = open_port({spawn_executable, Compiler},
                         [binary, exit_status, stderr_to_stdout, {args, Arguments}]),
        collect(Port, [])
    after
        ok = file:del_dir_r(Temp)
    end.

collect(Port, Chunks) ->
    receive
        {Port, {data, Bytes}} -> collect(Port, [Bytes | Chunks]);
        {Port, {exit_status, Status}} ->
            {Status, iolist_to_binary(lists:reverse(Chunks))}
    after 10000 ->
        port_close(Port),
        error(compiler_timeout)
    end.
