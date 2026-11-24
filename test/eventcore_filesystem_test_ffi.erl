-module(eventcore_filesystem_test_ffi).
-export([temp_root/0, remove_root/1, transaction_paths/1, tamper/1, remove/1,
         copy_transactions/2, add_malformed_transaction/2,
         set_mode/2, is_directory/1]).

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
    {1, Time, Nonce, Parents, Bases, [Event]} = binary_to_term(Bytes, [safe]),
    Invalid = term_to_binary({1, Time, Nonce, Parents, Bases, [Event | 0]}),
    Id = binary:encode_hex(crypto:hash(sha256, Invalid), lowercase),
    Directory = filename:join(Root, <<"events">>),
    ok = file:write_file(filename:join(Directory, <<Id/binary, ".txn">>), Invalid),
    Id.

set_mode(Path, Mode) ->
    ok = file:change_mode(Path, Mode),
    nil.

is_directory(Path) -> filelib:is_dir(Path).
