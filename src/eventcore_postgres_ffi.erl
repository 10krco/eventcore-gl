-module(eventcore_postgres_ffi).
-include_lib("kernel/include/logger.hrl").
-export([trace_read/2, trace_append/2, stop_pool/1, acquire_projection/2, projection_connection/1]).

trace_read(Stream, 0) ->
    ?LOG_INFO(#{operation => <<"postgres.read_stream">>, stream => Stream}),
    nil;
trace_read(Stream, After) ->
    ?LOG_INFO(#{operation => <<"postgres.read_stream_after">>, stream => Stream,
                after_version => After}),
    nil.

trace_append(StreamCount, EventCount) ->
    ?LOG_INFO(#{operation => <<"postgres.append_events">>, stream_count => StreamCount,
                event_count => EventCount}),
    nil.

stop_pool(Pid) ->
    try gen_server:stop(Pid)
    catch exit:noproc -> ok
    end,
    nil.


acquire_projection({pool, Pool}, Name) ->
    case get({?MODULE, Pool}) of
        undefined -> checkout_projection(Pool, Name);
        _ -> {error, already_leading}
    end;
acquire_projection(_, _) ->
    {error, coordinator_unavailable}.

checkout_projection(Pool, Name) ->
    try pgo:checkout(Pool, [{queue, true}, {timeout, infinity}]) of
        {ok, Ref, Connection} ->
            lock_session(Pool, Ref, Connection, lock_key(Name));
        {error, _} ->
            {error, coordinator_unavailable}
    catch
        exit:_ -> {error, coordinator_unavailable}
    end.

lock_session(Pool, Ref, Connection, Key) ->
    case advisory_query(Connection, "SELECT pg_try_advisory_lock($1)", Key) of
        #{rows := [{true}]} ->
            Token = make_ref(),
            Owner = self(),
            put({?MODULE, Pool}, {Token, Ref, Connection, Key}),
            {ok, fun() -> release_session(Owner, Pool, Token) end};
        #{rows := [{false}]} ->
            pgo:checkin(Ref, Connection),
            {error, already_leading};
        _ ->
            disconnect_session(Ref, Connection),
            {error, coordinator_unavailable}
    end.

release_session(Owner, Pool, Token) when Owner =:= self() ->
    case get({?MODULE, Pool}) of
        {Token, Ref, Connection, Key} ->
            erase({?MODULE, Pool}),
            case advisory_query(Connection, "SELECT pg_advisory_unlock($1)", Key) of
                #{rows := [{true}]} -> pgo:checkin(Ref, Connection);
                _ -> disconnect_session(Ref, Connection)
            end;
        _ -> ok
    end,
    nil;
release_session(_, _, _) ->
    nil.

projection_connection({pool, Pool} = Original) ->
    case get({?MODULE, Pool}) of
        {_, _, Connection, _} -> {single_connection, Connection};
        undefined -> Original
    end;
projection_connection(Original) ->
    Original.

advisory_query(Connection, Sql, Key) ->
    try pgo_handler:extended_query(Connection, Sql, [Key], #{queue_time => undefined})
    catch
        _:_ -> {error, advisory_query_failed}
    end.

disconnect_session(Ref, Connection) ->
    pgo_pool:disconnect(Ref, {error, projection_session_lost}, Connection, []).

lock_key(Name) ->
    Hash = lock_hash(Name, 16#cbf29ce484222325),
    <<Key:64/signed>> = <<Hash:64>>,
    Key.

lock_hash(<<>>, Hash) ->
    Hash;
lock_hash(<<Byte, Rest/binary>>, Hash) ->
    lock_hash(Rest, ((Hash bxor Byte) * 16#100000001b3) band 16#ffffffffffffffff).
