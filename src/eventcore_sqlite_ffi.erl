-module(eventcore_sqlite_ffi).
-behaviour(gen_server).

-export([start_file/2, start_memory/0, start_supplied/1, stop/1, migrate/2,
         read/3, append/3, load_snapshot/2, save_snapshot/4, checkpoint/2, save_checkpoint/3, reset_checkpoint/2,
         projection_frontier/1, projection_fetch/3, unpack_projection/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

start_file(Path, Encryption) -> start({file, Path, Encryption}).
start_memory() -> start(memory).
start_supplied(Connection) -> start({supplied, Connection}).

start(Source) ->
    case gen_server:start(?MODULE, {self(), Source}, []) of
        {ok, Server} -> {ok, Server};
        {error, {shutdown, Reason}} -> {error, Reason};
        {error, _} -> {error, cannot_open}
    end.

stop(Server) -> call(Server, stop, {error, nil}).
migrate(Server, Mode) -> call(Server, {migrate, Mode}, {error, migration_failed}).
read(Server, Stream, After) -> call(Server, {read, Stream, After}, {error, nil}).
append(Server, Expected, Events) ->
    call(Server, {append, Expected, Events}, {error, {store_failure, append_events}}).
load_snapshot(Server, Name) ->
    call(Server, {load_snapshot, Name}, {error, nil}).
save_snapshot(Server, Name, Observed, Value) ->
    call(Server, {save_snapshot, Name, Observed, Value}, {error, nil}).
checkpoint(Server, Name) ->
    call(Server, {checkpoint, Name}, {error, checkpoint_failed}).
save_checkpoint(Server, Name, Position) ->
    call(Server, {save_checkpoint, Name, Position}, {error, checkpoint_failed}).
reset_checkpoint(Server, Name) ->
    call(Server, {reset_checkpoint, Name}, {error, checkpoint_failed}).

projection_frontier(Server) ->
    call(Server, projection_frontier, {error, {store_failure, read_projection}}).

projection_fetch(Server, First, Last) ->
    call(Server, {projection_fetch, First, Last}, {error, {store_failure, read_projection}}).

unpack_projection({event, Stream, Kind, Bytes}) ->
    read_event(Stream, Kind, Bytes).

call(Server, Request, Failure) ->
    try gen_server:call(Server, Request, infinity)
    catch
        exit:_ -> Failure
    end.

init({Owner, Source}) ->
    case open(Source) of
        {ok, Connection} -> {ok, {Connection, monitor(process, Owner), #{}}};
        {error, Reason} -> {stop, {shutdown, Reason}}
    end.

open({supplied, Connection}) ->
    guard_query(fun() ->
        #{} = sqlite:status(Connection),
        {ok, Connection}
    end, {error, cannot_open});
open(memory) ->
    configure_open(fun() -> sqlite:open(":memory:", #{flags => [memory]}) end, none, <<"memory">>);
open({file, Path, Encryption}) ->
    configure_open(fun() -> sqlite:open(Path, #{busy_timeout => 5000}) end, Encryption, <<"wal">>).

configure_open(Open, Encryption, JournalMode) ->
    guard_query(fun() ->
        Connection = Open(),
        Result = guard_query(fun() -> configure(Connection, Encryption, JournalMode) end,
                             {error, cannot_open}),
        case Result of
            ok -> {ok, Connection};
            {error, _} ->
                close_connection(Connection),
                Result
        end
    end, {error, cannot_open}).

configure(Connection, Encryption, JournalMode) ->
    case apply_key(Connection, Encryption) of
        ok ->
            [{JournalMode}] = sqlite:query(Connection, ["PRAGMA journal_mode = ", JournalMode]),
            ok;
        Error -> Error
    end.

apply_key(_Connection, none) -> ok;
apply_key(Connection, {some, Password}) ->
    case valid_password(Password) of
        false -> {error, invalid_encryption_key};
        true ->
            Escaped = binary:replace(Password, <<"'">>, <<"''">>, [global]),
            KeyResult = sqlite:query(Connection, ["PRAGMA key = '", Escaped, "'"]),
            case {KeyResult, sqlite:query(Connection, "PRAGMA cipher_version")} of
                {_, [{Version}]} when is_binary(Version), byte_size(Version) > 0 ->
                    guard_query(fun() ->
                        [{_}] = sqlite:query(Connection, "SELECT COUNT(*) FROM sqlite_master"),
                        ok
                    end, {error, invalid_encryption_key});
                _ -> {error, encryption_unavailable}
            end
    end.

valid_password(Password) ->
    byte_size(Password) > 0 andalso binary:match(Password, <<0>>) =:= nomatch.

handle_call({acquire, Name}, {Holder, _}, {Connection, Owner, Leases}) ->
    {Reply, Updated} = eventcore_projection_leases:acquire(Name, Holder, Leases),
    {reply, Reply, {Connection, Owner, Updated}};
handle_call({release, Name, Token}, _From, {Connection, Owner, Leases}) ->
    Updated = eventcore_projection_leases:release(Name, Token, Leases),
    {reply, nil, {Connection, Owner, Updated}};
handle_call(projection_frontier, _From, {Connection, _, _} = State) ->
    Reply = guard_query(fun() ->
        [{High}] = sqlite:query(Connection, "SELECT COALESCE(MAX(position), 0) FROM eventcore_events"),
        {ok, High}
    end, {error, {store_failure, read_projection}}),
    {reply, Reply, State};
handle_call({projection_fetch, First, Last}, _From, {Connection, _, _} = State) ->
    Reply = guard_query(fun() ->
        Rows = sqlite:query(Connection, "SELECT position, stream, kind, payload FROM eventcore_events "
            "WHERE position BETWEEN ? AND ? ORDER BY position", [First, Last]),
        {ok, [{Position, {event, {id, Stream}, Kind, Bytes}} || {Position, Stream, Kind, Bytes} <- Rows]}
    end, {error, {store_failure, read_projection}}),
    {reply, Reply, State};
handle_call(stop, _From, State) ->
    {stop, normal, {ok, nil}, State};
handle_call({migrate, Mode}, _From, {Connection, _, _} = State) ->
    Reply = transaction(Connection, fun() -> create_tables(Connection, Mode) end,
                        {error, migration_failed}),
    reply_or_stop(Reply, State);
handle_call({read, _Stream, After}, _From, State) when After > 9223372036854775807 ->
    {reply, {ok, []}, State};
handle_call({read, Stream, After}, _From, {Connection, _, _} = State) ->
    Reply = guard_query(fun() -> read_events(Connection, Stream, After) end, {error, nil}),
    {reply, Reply, State};
handle_call({append, Expected, Events}, _From, {Connection, _, _} = State) ->
    Reply = transaction(Connection, fun() ->
        case stream_versions(Connection, Expected, #{}) of
            {ok, Versions} -> insert_events(Connection, Events, Versions);
            Error -> Error
        end
    end, {error, {store_failure, append_events}}),
    reply_or_stop(Reply, State);
handle_call({load_snapshot, Name}, _From, {Connection, _, _} = State) ->
    Reply = guard_query(fun() -> read_snapshot(Connection, Name) end, {error, nil}),
    {reply, Reply, State};
handle_call({save_snapshot, Name, Observed, Value}, _From, {Connection, _, _} = State) ->
    Reply = transaction(Connection, fun() -> write_snapshot(Connection, Name, Observed, Value) end,
                        {error, nil}),
    reply_or_stop(Reply, State);
handle_call({checkpoint, Name}, _From, {Connection, _, _} = State) ->
    Reply = guard_query(fun() ->
        case sqlite:query(Connection, "SELECT position FROM eventcore_checkpoints WHERE name = ?", [Name]) of
            [] -> {ok, none};
            [{Position}] when is_integer(Position), Position >= 0 -> {ok, {some, Position}};
            _ -> {error, checkpoint_failed}
        end
    end, {error, checkpoint_failed}),
    {reply, Reply, State};
handle_call({save_checkpoint, Name, Position}, _From, {Connection, _, _} = State) ->
    Reply = guard_query(fun() ->
        ok = write_one(Connection, "INSERT INTO eventcore_checkpoints(name, position) VALUES(?, ?) "
                         "ON CONFLICT(name) DO UPDATE SET position = excluded.position", [Name, Position]),
        {ok, nil}
    end, {error, checkpoint_failed}),
    {reply, Reply, State};
handle_call({reset_checkpoint, Name}, _From, {Connection, _, _} = State) ->
    Reply = guard_query(fun() ->
        ok = write_query(Connection, "DELETE FROM eventcore_checkpoints WHERE name = ?", [Name]),
        {ok, nil}
    end, {error, checkpoint_failed}),
    {reply, Reply, State}.

reply_or_stop({rollback_failed, Failure}, State) -> {stop, normal, Failure, State};
reply_or_stop(Reply, State) -> {reply, Reply, State}.

handle_cast(_, State) -> {noreply, State}.
handle_info({'DOWN', Owner, process, _, _}, {_, Owner, _} = State) ->
    {stop, normal, State};
handle_info({'DOWN', Holder, process, _, _}, {Connection, Owner, Leases}) ->
    {noreply, {Connection, Owner, eventcore_projection_leases:down(Holder, Leases)}};
handle_info(_, State) -> {noreply, State}.

terminate(_, {Connection, _, _}) -> close_connection(Connection).
close_connection(Connection) -> guard_query(fun() -> sqlite:close(Connection) end, ok).

guard_query(Work, Failure) ->
    try Work()
    catch
        error:_ -> Failure
    end.

transaction(Connection, Work, Failure) ->
    case guard_query(fun() -> sqlite:query(Connection, "BEGIN IMMEDIATE") end, Failure) of
        [] ->
            Result = guard_query(fun() ->
                case Work() of
                    {ok, _} = Success ->
                        [] = sqlite:query(Connection, "COMMIT"),
                        Success;
                    Error -> Error
                end
            end, Failure),
            case Result of
                {ok, _} -> Result;
                {error, _} ->
                    case guard_query(fun() -> sqlite:query(Connection, "ROLLBACK") end, Failure) of
                        [] -> Result;
                        _ -> {rollback_failed, Failure}
                    end
            end;
        _ -> Failure
    end.

create_tables(Connection, event_schema) ->
    [] = sqlite:query(Connection, "CREATE TABLE IF NOT EXISTS eventcore_events ("
        "position INTEGER PRIMARY KEY, stream TEXT NOT NULL, version INTEGER NOT NULL, "
        "kind TEXT NOT NULL, payload BLOB NOT NULL, UNIQUE(stream, version))"),
    [] = sqlite:query(Connection, "CREATE TABLE IF NOT EXISTS eventcore_command_snapshots ("
        "name TEXT PRIMARY KEY NOT NULL, body BLOB NOT NULL)"),
    create_tables(Connection, checkpoint_schema);
create_tables(Connection, checkpoint_schema) ->
    [] = sqlite:query(Connection, "CREATE TABLE IF NOT EXISTS eventcore_checkpoints ("
        "name TEXT PRIMARY KEY NOT NULL, position INTEGER NOT NULL CHECK(position >= 0))"),
    {ok, nil}.

read_snapshot(Connection, Name) ->
    case sqlite:query(Connection, "SELECT body FROM eventcore_command_snapshots WHERE name = ?", [Name]) of
        [] -> {ok, none};
        [{Body}] ->
            case eventcore_snapshot_wire:decode(Body) of
                {ok, Value} -> {ok, {some, Value}};
                Error -> Error
            end
    end.

write_snapshot(Connection, Name, Observed, Value) ->
    case read_snapshot(Connection, Name) of
        {ok, Current} ->
            case eventcore@snapshot:can_replace(Current, Observed, Value) of
                true -> replace_snapshot(Connection, Name, Value);
                false -> {ok, nil}
            end;
        Error -> Error
    end.

replace_snapshot(Connection, Name, Value) ->
    Body = eventcore_snapshot_wire:encode(Value),
    ok = write_one(Connection, "INSERT INTO eventcore_command_snapshots(name, body) VALUES(?, ?) "
                     "ON CONFLICT(name) DO UPDATE SET body = excluded.body", [Name, {blob, Body}]),
    {ok, nil}.

read_events(Connection, {id, Name} = Stream, After) ->
    Rows = sqlite:query(Connection, "SELECT kind, payload FROM eventcore_events "
                        "WHERE stream = ? AND version > ? ORDER BY version", [Name, After]),
    {ok, [read_event(Stream, Kind, Bytes) || {Kind, Bytes} <- Rows]}.

read_event(Stream, Kind, <<Length:64, Body/binary>>) when byte_size(Body) =:= (Length + 7) div 8 ->
    Padding = bit_size(Body) - Length,
    case Body of
        <<Payload:Length/bitstring, 0:Padding>> -> {ok, {event, Stream, Kind, Payload}};
        _ -> invalid_payload(Stream)
    end;
read_event(Stream, _, _) -> invalid_payload(Stream).

invalid_payload(Stream) ->
    {error, {deserialization_failed, Stream, <<"Invalid SQLite payload">>}}.

stream_versions(_Connection, [], Versions) -> {ok, Versions};
stream_versions(Connection, [{{id, Name} = Stream, {version, Expected}} | Rest], Versions) ->
    [{Actual}] = sqlite:query(Connection, "SELECT COALESCE(MAX(version), 0) FROM eventcore_events "
                            "WHERE stream = ?", [Name]),
    case Expected =:= Actual of
        true -> stream_versions(Connection, Rest, Versions#{Stream => Actual});
        false -> {error, {version_conflict, Stream, {version, Expected}, {version, Actual}}}
    end.

insert_events(_Connection, [], _) -> {ok, nil};
insert_events(Connection, [{event, {id, Name} = Stream, Kind, Payload} | Rest], Versions) ->
    Version = maps:get(Stream, Versions) + 1,
    Length = bit_size(Payload),
    Padding = (8 - Length rem 8) rem 8,
    Bytes = <<Length:64, Payload/bitstring, 0:Padding>>,
    ok = write_one(Connection, "INSERT INTO eventcore_events(stream, version, kind, payload) "
                     "VALUES(?, ?, ?, ?)", [Name, Version, Kind, {blob, Bytes}]),
    insert_events(Connection, Rest, Versions#{Stream := Version}).

write_one(Connection, Sql, Parameters) ->
    [{1}] = sqlite:query(Connection, [Sql, " RETURNING 1"], Parameters),
    ok.

write_query(Connection, Sql, Parameters) ->
    case sqlite:query(Connection, Sql, Parameters) of
        [] -> ok;
        [{Count}] when is_integer(Count), Count >= 0 -> ok
    end.
