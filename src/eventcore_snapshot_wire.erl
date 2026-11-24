-module(eventcore_snapshot_wire).
-export([encode/1, decode/1]).

encode({snapshot, Checkpoints}) ->
    Data = iolist_to_binary([<<1>>, [encode_checkpoint(Entry) || Entry <- Checkpoints]]),
    <<(crypto:hash(sha256, Data))/binary, Data/binary>>.

encode_checkpoint({checkpoint, {id, Name}, {version, Version}, Bytes}) ->
    VersionBytes = binary:encode_unsigned(Version),
    Length = bit_size(Bytes),
    Padding = (8 - Length rem 8) rem 8,
    <<(byte_size(Name)):16, Name/binary, (byte_size(VersionBytes)):32, VersionBytes/binary,
      Length:64, Bytes/bitstring, 0:Padding>>.

decode(<<Digest:32/binary, 1, Checkpoints/binary>> = Body) ->
    case crypto:hash(sha256, binary:part(Body, 32, byte_size(Body) - 32)) of
        Digest -> decode_checkpoints(Checkpoints, []);
        _ -> {error, nil}
    end;
decode(_) -> {error, nil}.

decode_checkpoints(<<>>, Entries) -> {ok, {snapshot, lists:reverse(Entries)}};
decode_checkpoints(<<NameSize:16, Name:NameSize/binary, VersionSize:32, Version:VersionSize/binary,
                     Length:64, Rest/binary>>, Entries)
        when NameSize > 0, NameSize =< 1020, VersionSize > 0,
             (Length + 7) div 8 =< byte_size(Rest) ->
    Padding = (8 - Length rem 8) rem 8,
    case {validated_id(Name), Rest} of
        {{ok, Id}, <<Bytes:Length/bitstring, 0:Padding, Tail/binary>>} ->
            Entry = {checkpoint, Id, {version, binary:decode_unsigned(Version)}, Bytes},
            decode_checkpoints(Tail, [Entry | Entries]);
        _ -> {error, nil}
    end;
decode_checkpoints(_, _) -> {error, nil}.

validated_id(Name) ->
    case unicode:characters_to_binary(Name) of
        Name ->
            case eventcore@stream:id(Name) of
                {ok, {id, Name}} = Valid -> Valid;
                _ -> {error, nil}
            end;
        _ -> {error, nil}
    end.
