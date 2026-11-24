-module(eventcore_sqlite_test_ffi).
-export([temp_database/0, delete_database/1, two_writers/2, open_supplied/0, pragmas/1,
         reject_kind/1, sqlite_test_backend/0, insert_invalid_payloads/1, file_journal_mode/1]).

sqlite_test_backend() ->
    unicode:characters_to_binary(os:getenv("EVENTCORE_SQLITE_TEST_BACKEND", "plain")).

temp_database() ->
    Tmp = os:getenv("TMPDIR", "/tmp"),
    Name = binary:encode_hex(crypto:strong_rand_bytes(16), lowercase),
    filename:join(unicode:characters_to_binary(Tmp), <<"eventcore-sqlite-", Name/binary, ".db">>).

delete_database(Path) ->
    ok = file:delete(Path),
    nil.

file_journal_mode(Path) ->
    Db = sqlite:open(Path),
    try
        [{Mode}] = sqlite:query(Db, "PRAGMA journal_mode"),
        Mode
    after
        sqlite:close(Db)
    end.

open_supplied() ->
    Db = sqlite:open(":memory:"),
    [] = sqlite:query(Db, "PRAGMA foreign_keys = ON"),
    [] = sqlite:query(Db, "PRAGMA count_changes = ON"),
    Db.

pragmas(Db) ->
    [{ForeignKeys}] = sqlite:query(Db, "PRAGMA foreign_keys"),
    [{JournalMode}] = sqlite:query(Db, "PRAGMA journal_mode"),
    [{CountChanges}] = sqlite:query(Db, "PRAGMA count_changes"),
    {ForeignKeys, JournalMode, CountChanges}.

insert_invalid_payloads(Db) ->
    Sql = "INSERT INTO eventcore_events(stream, version, kind, payload) VALUES('malformed', ?, 'created', ?)",
    [{1}] = sqlite:query(Db, Sql, [1, {blob, <<16#FFFFFFFFFFFFFFFF:64>>}]),
    [{1}] = sqlite:query(Db, Sql, [2, {blob, <<3:64, 5:3, 1:5>>}]),
    nil.

two_writers(First, Second) ->
    Parent = self(),
    Tag = make_ref(),
    Writer = fun(Work) ->
        receive Tag -> Parent ! {Tag, self(), Work()} end
    end,
    A = spawn(fun() -> Writer(First) end),
    B = spawn(fun() -> Writer(Second) end),
    A ! Tag,
    B ! Tag,
    {await_write(Tag, A), await_write(Tag, B)}.

await_write(Tag, Writer) ->
    receive
        {Tag, Writer, Result} -> Result
    after 15000 ->
        error(writer_timed_out)
    end.


reject_kind(Db) ->
    [] = sqlite:query(Db, "CREATE TRIGGER reject_kind BEFORE INSERT ON eventcore_events "
                         "WHEN NEW.kind = 'refused' BEGIN SELECT RAISE(ABORT, 'refused'); END"),
    nil.
