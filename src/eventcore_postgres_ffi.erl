-module(eventcore_postgres_ffi).
-include_lib("kernel/include/logger.hrl").
-export([trace_read/2, trace_append/2, stop_pool/1, acquire_projection/2, projection_connection/1, checkpoint_connection/2,
         with_transactional_leader/3, transactional_command/2,
         transactional_scope/1, transactional_connection/1]).

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


acquire_projection(Connection, Name) ->
    acquire_session(Connection, lock_key(Name)).

acquire_session({pool, Pool}, Key) ->
    case lists:keymember(Key, 1, held_sessions(Pool)) of
        true -> {error, already_leading};
        false -> checkout_projection(Pool, Key, erlang:monotonic_time(millisecond) + 100)
    end;
acquire_session(_, _) ->
    {error, coordinator_unavailable}.

checkout_projection(Pool, Key, Deadline) ->
    %% Pog's checkout timeout also expires the held session. Bound only the
    %% acquisition attempt, leaving a successful checkout valid until release.
    try pgo:checkout(Pool, [{queue, false}, {timeout, infinity}]) of
        {ok, Ref, Connection} ->
            lock_session(Pool, Ref, Connection, Key);
        {error, "connection not available and queuing is disabled"} ->
            case erlang:monotonic_time(millisecond) < Deadline of
                true ->
                    timer:sleep(5),
                    checkout_projection(Pool, Key, Deadline);
                false -> {error, coordinator_unavailable}
            end;
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
            put({?MODULE, Pool}, [{Key, Token, Ref, Connection} | held_sessions(Pool)]),
            {ok, fun() -> release_session(Owner, Pool, Key, Token) end};
        #{rows := [{false}]} ->
            pgo:checkin(Ref, Connection),
            {error, already_leading};
        _ ->
            disconnect_session(Ref, Connection),
            {error, coordinator_unavailable}
    end.

release_session(Owner, Pool, Key, Token) when Owner =:= self() ->
    _ = unlock_session(Pool, Key, Token),
    nil;
release_session(_, _, _, _) ->
    nil.

unlock_session(Pool, Key, Token) ->
    case lists:keytake(Key, 1, held_sessions(Pool)) of
        {value, {Key, Token, Ref, Connection}, Remaining} ->
            save_held_sessions(Pool, Remaining),
            case advisory_query(Connection, "SELECT pg_advisory_unlock($1)", Key) of
                #{rows := [{true}]} ->
                    pgo:checkin(Ref, Connection),
                    ok;
                _ ->
                    disconnect_session(Ref, Connection),
                    error
            end;
        _ -> error
    end.

save_held_sessions(Pool, []) ->
    erase({?MODULE, Pool});
save_held_sessions(Pool, Remaining) ->
    put({?MODULE, Pool}, Remaining).

held_sessions(Pool) ->
    case get({?MODULE, Pool}) of
        undefined -> [];
        Sessions -> Sessions
    end.

projection_connection({pool, Pool} = Original) ->
    case held_sessions(Pool) of
        [{_, _, _, Connection} | _] -> {single_connection, Connection};
        [] -> Original
    end;
projection_connection(Original) ->
    Original.

checkpoint_connection({pool, Pool} = Original, Name) ->
    case lists:keyfind(lock_key(Name), 1, held_sessions(Pool)) of
        {_, _, _, Connection} -> {single_connection, Connection};
        false -> Original
    end;
checkpoint_connection(Original, _) ->
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

%% The leader's owning process holds the Pog checkout throughout the run.
%% Run all writes on that exact session so pool reconnects cannot restore
%% authority to a process that has lost its lock.
with_transactional_leader({pool, Pool} = Database, Name, Work) ->
    Key = lock_key(<<"eventcore:transactional-projection:", Name/binary>>),
    case acquire_session(Database, Key) of
        {ok, _} ->
            {Key, Token, _, _} = lists:keyfind(Key, 1, held_sessions(Pool)),
            try Work({self(), Pool, Key, Token}) of
                {ok, _} = Outcome ->
                    case finish_transactional_session(Pool, Key, Token) of
                        ok -> {ok, Outcome};
                        error -> {error, release_failed}
                    end;
                {error, _} = Outcome ->
                    finish_transactional_session(Pool, Key, Token),
                    {ok, Outcome}
            catch
                Class:Reason:Stack ->
                    finish_transactional_session(Pool, Key, Token),
                    erlang:raise(Class, Reason, Stack)
            end;
        {error, Error} -> {error, {acquisition_failed, Error}}
    end;
with_transactional_leader(_, _, _) ->
    {error, {acquisition_failed, coordinator_unavailable}}.

finish_transactional_session(Pool, Key, Token) ->
    case erase({?MODULE, transaction, Token}) of
        Scope when is_reference(Scope) ->
            case lists:keyfind(Key, 1, held_sessions(Pool)) of
                {Key, Token, Ref, Conn} -> discard_session(Pool, Key, Ref, Conn);
                _ -> ok
            end,
            error;
        _ ->
            unlock_session(Pool, Key, Token)
    end.

transactional_command({Owner, Pool, Key, Token}, Command) when Owner =:= self() ->
    StateKey = {?MODULE, transaction, Token},
    case {lists:keyfind(Key, 1, held_sessions(Pool)), Command, get(StateKey)} of
        {{Key, Token, Ref, Connection}, check, undefined} ->
            check_transactional_lock(Pool, Key, Ref, Connection);
        {{Key, Token, Ref, Connection}, 'begin', undefined} ->
            run_transaction_command(Pool, Key, Ref, Connection, StateKey, 'begin', "BEGIN");
        {{Key, Token, Ref, Connection}, commit, Scope} when is_reference(Scope) ->
            run_transaction_command(Pool, Key, Ref, Connection, StateKey, commit, "COMMIT");
        {{Key, Token, Ref, Connection}, rollback, Scope} when is_reference(Scope) ->
            run_transaction_command(Pool, Key, Ref, Connection, StateKey, rollback, "ROLLBACK");
        _ -> {error, session_failed}
    end;
transactional_command(_, _) ->
    {error, session_failed}.

check_transactional_lock(Pool, Key, Ref, Connection) ->
    Sql = "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' "
          "AND pid = pg_backend_pid() AND granted AND mode = 'ExclusiveLock' "
          "AND objsubid = 1 AND classid::bigint = ($1::bigint >> 32 & 4294967295) "
          "AND objid::bigint = ($1::bigint & 4294967295))",
    case advisory_query(Connection, Sql, Key) of
        #{rows := [{true}]} -> {ok, {single_connection, Connection}};
        _ ->
            discard_session(Pool, Key, Ref, Connection),
            {error, session_failed}
    end.

transactional_scope({Owner, _, _, Token} = Leader) when Owner =:= self() ->
    {Leader, get({?MODULE, transaction, Token})}.

transactional_connection({{Owner, Pool, Key, Token}, Scope})
        when Owner =:= self(), is_reference(Scope) ->
    case {get({?MODULE, transaction, Token}), lists:keyfind(Key, 1, held_sessions(Pool))} of
        {Scope, {Key, Token, _, Connection}} -> {ok, {single_connection, Connection}};
        _ -> {error, nil}
    end;
transactional_connection(_) ->
    {error, nil}.

run_transaction_command(Pool, Key, Ref, Connection, StateKey, Command, Sql) ->
    case advisory_query_no_parameter(Connection, Sql) of
        #{command := 'begin'} when Command =:= 'begin' ->
            put(StateKey, make_ref()),
            {ok, {single_connection, Connection}};
        #{command := Command} ->
            erase(StateKey),
            {ok, {single_connection, Connection}};
        #{command := rollback} when Command =:= commit ->
            erase(StateKey),
            {error, rolled_back};
        {error, {pgsql_error, #{code := Code}}} when Command =:= commit ->
            case commit_rejected(Code) of
                true ->
                    erase(StateKey),
                    {error, rolled_back};
                false ->
                    failed_transaction_command(Pool, Key, Ref, Connection, StateKey)
            end;
        _ ->
            failed_transaction_command(Pool, Key, Ref, Connection, StateKey)
    end.

%% Integrity violations, the default RAISE EXCEPTION from a deferred trigger,
%% and transaction rollback SQLSTATEs prove that PostgreSQL rejected COMMIT.
%% 40003 explicitly leaves completion unknown.
commit_rejected(<<"23", _/binary>>) -> true;
commit_rejected(<<"P0001">>) -> true;
commit_rejected(<<"40003">>) -> false;
commit_rejected(<<"40", _/binary>>) -> true;
commit_rejected(_) -> false.

failed_transaction_command(Pool, Key, Ref, Connection, StateKey) ->
    erase(StateKey),
    discard_session(Pool, Key, Ref, Connection),
    {error, session_failed}.

advisory_query_no_parameter(Connection, Sql) ->
    try pgo_handler:extended_query(Connection, Sql, [], #{queue_time => undefined})
    catch
        _:_ -> {error, transactional_command_failed}
    end.

discard_session(Pool, Key, Ref, Connection) ->
    save_held_sessions(Pool, lists:keydelete(Key, 1, held_sessions(Pool))),
    disconnect_session(Ref, Connection).
