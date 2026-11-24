-module(eventcore_filesystem_test_ffi).
-export([temp_root/0, remove_root/1, transaction_paths/1, tamper/1, remove/1]).

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
