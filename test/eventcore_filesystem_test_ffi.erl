-module(eventcore_filesystem_test_ffi).
-export([temp_root/0, remove_root/1, transaction_paths/1, tamper/1, remove/1,
         copy_transactions/2, copy_replica/2, downgrade_transaction/1, add_malformed_transaction/2,
         open_with_failed_ancestor_sync/2, is_directory/1]).

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
    {2, Time, Replica, Nonce, Parents, Bases, [Event]} = binary_to_term(Bytes, [safe]),
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
    {2, Time, _, Nonce, Parents, Bases, Events} = binary_to_term(Bytes, [safe]),
    Original = term_to_binary({1, Time, Nonce, Parents, Bases, Events}),
    Id = binary:encode_hex(crypto:hash(sha256, Original), lowercase),
    Destination = filename:join(filename:dirname(Path), <<Id/binary, ".txn">>),
    ok = file:write_file(Destination, Original),
    ok = file:delete(Path),
    nil.
