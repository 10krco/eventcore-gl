-module(eventcore_filesystem_replica).
-include_lib("kernel/include/file.hrl").
-export([load/3]).

load(_Root, {some, Id}, _Publish) -> {ok, Id};
load(Root, none, Publish) ->
    Directory = filename:join(Root, <<".eventcore">>),
    Path = filename:join(Directory, <<"replica">>),
    case filelib:ensure_path(Directory) of
        ok ->
            case fingerprint(Root) of
                {ok, Current} -> load_identity(Path, Current, Publish);
                Error -> Error
            end;
        Error -> Error
    end.

fingerprint(Root) ->
    case file:read_file_info(Root, [raw]) of
        {ok, #file_info{inode = Inode, major_device = Device}} ->
            Git = case file:read_file_info(filename:join(Root, <<".git">>), [raw]) of
                {ok, #file_info{inode = GitInode}} -> GitInode;
                _ -> undefined
            end,
            Bytes = term_to_binary({machine_id(), Root, Device, Inode, Git}),
            {ok, binary:encode_hex(crypto:hash(sha256, Bytes), lowercase)};
        Error -> Error
    end.

machine_id() ->
    case file:read_file(<<"/etc/machine-id">>) of
        {ok, Id} -> Id;
        _ ->
            case file:read_file(<<"/var/lib/dbus/machine-id">>) of
                {ok, Id} -> Id;
                _ -> inet:gethostname()
            end
    end.

load_identity(Path, Current, Publish) ->
    case file:read_file(Path) of
        {ok, <<Current:64/binary, Id:32/binary>>} ->
            case valid_id(Id) of
                true -> {ok, Id};
                false -> {error, invalid_replica_identity}
            end;
        {ok, <<_Previous:64/binary, Id:32/binary>>} ->
            case valid_id(Id) of
                true -> regenerate(Path, Current, Publish);
                false -> {error, invalid_replica_identity}
            end;
        {ok, _} -> {error, invalid_replica_identity};
        {error, enoent} -> regenerate(Path, Current, Publish);
        Error -> Error
    end.

valid_id(Id) ->
    try binary:decode_hex(Id) of
        _ -> true
    catch
        error:badarg -> false
    end.

regenerate(Path, Current, Publish) ->
    Id = binary:encode_hex(crypto:strong_rand_bytes(16), lowercase),
    case Publish(Path, <<Current/binary, Id/binary>>) of
        ok -> {ok, Id};
        Error -> Error
    end.
