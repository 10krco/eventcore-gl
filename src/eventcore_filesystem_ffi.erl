-module(eventcore_filesystem_ffi).
-behaviour(gen_server).
-export([start/2, start/3, stop/1, status/1, read/3, append/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    owner, lock, directory, sync,
    streams = #{}, tips = [],
    integrity_failures = [], dangling = []
}).

start(Root, Sync) ->
    start(Root, Sync, fun(Directory) -> sync_directory(Directory, Sync) end).

% Accept a sync function to exercise open failures in the Erlang tests.
start(Root, Sync, SyncAncestor) ->
    case gen_server:start(?MODULE, {self(), Root, Sync, SyncAncestor}, []) of
        {ok, Pid} -> {ok, Pid};
        {error, {shutdown, store_locked}} -> {error, store_locked};
        {error, {shutdown, lock_executable_missing}} -> {error, lock_executable_missing};
        {error, {shutdown, conflicting_history}} -> {error, conflicting_history};
        {error, _} -> {error, cannot_open}
    end.

stop(Pid) ->
    try gen_server:stop(Pid) of
        ok -> {ok, nil}
    catch
        exit:_ -> {error, nil}
    end.

status(Pid) -> call(Pid, status, {error, nil}).
read(Pid, Id, After) -> call(Pid, {read, Id, After}, {error, nil}).
append(Pid, Expected, Events) ->
    call(Pid, {append, Expected, Events}, {error, {store_failure, append_events}}).

call(Pid, Request, Failure) ->
    try gen_server:call(Pid, Request, infinity)
    catch
        exit:_ -> Failure
    end.

init({Owner, RequestedRoot, Sync, SyncAncestor}) ->
    process_flag(trap_exit, true),
    Root = filename:absname(RequestedRoot),
    Directory = filename:join(Root, <<"events">>),
    case ensure_directory(Directory, Sync, SyncAncestor) of
        ok ->
            case acquire_lock(filename:join(Root, <<".lock">>)) of
                {ok, Lock} ->
                    State = #state{owner = monitor(process, Owner), lock = Lock,
                                   directory = Directory, sync = Sync},
                    case prepare(Root, State) of
                        {ok, Loaded} -> {ok, Loaded};
                        {error, Reason} ->
                            release_lock(Lock),
                            {stop, {shutdown, Reason}}
                    end;
                {error, Reason} -> {stop, {shutdown, Reason}}
            end;
        {error, Reason} -> {stop, {shutdown, Reason}}
    end.

ensure_directory(Path, Sync, SyncAncestor) ->
    case filelib:ensure_path(Path) of
        ok when Sync =:= unsynced -> ok;
        ok -> sync_ancestors(Path, SyncAncestor);
        Error -> Error
    end.

% An earlier open may have created directories and then failed to sync their
% entries. Flush the parent chain even when all directories already exist.
sync_ancestors(Path, SyncAncestor) ->
    case filename:dirname(Path) of
        Path -> ok;
        Parent ->
            case SyncAncestor(Parent) of
                ok -> sync_ancestors(Parent, SyncAncestor);
                Error -> Error
            end
    end.

prepare(Root, #state{sync = Sync} = State) ->
    case create_metadata(Root) of
        ok ->
            case sync_directory(Root, Sync) of
                ok -> scan(State);
                Error -> Error
            end;
        Error -> Error
    end.

create_metadata(Root) ->
    case write_if_absent(filename:join(Root, <<".gitignore">>),
                         <<"/.lock\n/.eventcore/\n/events/*.tmp\n/checkpoints/\n/locks/\n">>) of
        ok -> write_if_absent(filename:join(Root, <<".gitattributes">>),
                              <<"events/*.txn binary\n">>);
        Error -> Error
    end.

write_if_absent(Path, Bytes) ->
    case file:write_file(Path, Bytes, [exclusive]) of
        {error, eexist} -> ok;
        Result -> Result
    end.

acquire_lock(Path) ->
    case os:find_executable("flock") of
        false -> {error, lock_executable_missing};
        Executable ->
            % Pass the lock path as an argument. The fixed shell program only
            % acknowledges the lock and holds stdin until shutdown.
            Port = open_port({spawn_executable, Executable},
                             [binary, exit_status, use_stdio,
                              {args, ["-F", "-n", "-E", "75", "--", Path,
                                      "sh", "-c", "printf R; read -r stop"]}]),
            receive
                {Port, {data, <<"R">>}} -> {ok, Port};
                {Port, {exit_status, 75}} -> {error, store_locked};
                {Port, {exit_status, _}} -> {error, lock_failed};
                {'EXIT', Port, Reason} -> {error, Reason}
            after 5000 ->
                catch port_close(Port),
                {error, lock_timeout}
            end
    end.

release_lock(Port) ->
    case catch port_command(Port, <<"stop\n">>) of
        true ->
            receive
                {Port, {exit_status, _}} -> ok;
                {'EXIT', Port, _} -> ok
            after 5000 ->
                catch port_close(Port),
                ok
            end;
        _ -> ok
    end.

terminate(_Reason, #state{lock = Port}) -> release_lock(Port).
handle_cast(_Request, State) -> {noreply, State}.
handle_info({'DOWN', Owner, process, _, _}, #state{owner = Owner} = State) ->
    {stop, normal, State};
handle_info({Port, {exit_status, _}}, #state{lock = Port} = State) ->
    {stop, lock_lost, State};
handle_info({'EXIT', Port, _}, #state{lock = Port} = State) ->
    {stop, lock_lost, State};
handle_info(_Info, State) -> {noreply, State}.

handle_call(status, _From, #state{integrity_failures = Bad, dangling = Dangling} = State) ->
    {reply, {ok, {status, Bad, Dangling}}, State};
handle_call({read, Id, After}, _From, #state{streams = Streams} = State) ->
    {Version, Reversed} = maps:get(Id, Streams, {0, []}),
    Tail = lists:reverse(lists:sublist(Reversed, max(0, Version - After))),
    {reply, {ok, Tail}, State};
handle_call({append, _, _}, _From, #state{dangling = [_ | _]} = State) ->
    {reply, {error, {store_failure, append_events}}, State};
handle_call({append, Expected, Events}, _From, #state{streams = Streams} = State) ->
    case check_versions(Expected, Streams) of
        ok when Events =:= [] -> {reply, {ok, nil}, State};
        ok ->
            {Reply, Updated} = persist(Expected, Events, State),
            {reply, Reply, Updated};
        Error -> {reply, {error, Error}, State}
    end.

check_versions([], _Streams) -> ok;
check_versions([{Id, {version, Expected}} | Rest], Streams) ->
    case maps:get(Id, Streams, {0, []}) of
        {Expected, _} -> check_versions(Rest, Streams);
        {Actual, _} -> {version_conflict, Id, {version, Expected}, {version, Actual}}
    end.

persist(Expected, Events, #state{directory = Directory, sync = Sync,
                                streams = Streams, tips = Parents} = State) ->
    Transaction = {1, os:system_time(nanosecond), crypto:strong_rand_bytes(16),
                   Parents, lists:sort(Expected), Events},
    Bytes = term_to_binary(Transaction),
    Id = checksum(Bytes),
    Path = filename:join(Directory, <<Id/binary, ".txn">>),
    Temp = filename:join(Directory, <<Id/binary, ".tmp">>),
    Failure = {error, {store_failure, append_events}},
    case write_transaction(Temp, Path, Bytes, Sync) of
        ok ->
            Updated = State#state{streams = index_events(Events, Streams), tips = [Id]},
            case sync_directory(Directory, Sync) of
                ok -> {{ok, nil}, Updated};
                {error, _} -> {Failure, Updated}
            end;
        {error, _} -> {Failure, State}
    end.

write_transaction(Temp, Path, Bytes, Sync) ->
    case file:open(Temp, [write, raw, binary, exclusive]) of
        {ok, File} ->
            Written = case file:write(File, Bytes) of
                ok -> sync_file(File, Sync);
                Error -> Error
            end,
            Closed = file:close(File),
            Result = case {Written, Closed} of
                {ok, ok} -> file:rename(Temp, Path);
                {{error, _} = Error1, _} -> Error1;
                {ok, Error2} -> Error2
            end,
            case Result of
                ok -> ok;
                Error3 ->
                    file:delete(Temp),
                    Error3
            end;
        Error -> Error
    end.

sync_file(File, full) -> file:sync(File);
sync_file(_File, unsynced) -> ok.

sync_directory(_Directory, unsynced) -> ok;
sync_directory(Directory, full) ->
    case file:open(Directory, [read, raw, directory]) of
        {ok, File} ->
            Result = file:sync(File),
            file:close(File),
            Result;
        Error -> Error
    end.

checksum(Bytes) -> binary:encode_hex(crypto:hash(sha256, Bytes), lowercase).

scan(#state{directory = Directory} = State) ->
    case file:list_dir(Directory) of
        {ok, Files} ->
            case scan_files(lists:sort(Files), Directory, #{}, []) of
                {ok, Transactions, Failures} -> rebuild(Transactions, Failures, State);
                Error -> Error
            end;
        Error -> Error
    end.

scan_files([], _Directory, Transactions, Failures) ->
    {ok, Transactions, lists:reverse(Failures)};
scan_files([File | Rest], Directory, Transactions, Failures) ->
    case filename:extension(File) of
        ".txn" ->
            Id = unicode:characters_to_binary(filename:rootname(File)),
            case file:read_file(filename:join(Directory, File)) of
                {ok, Bytes} ->
                    case decode_transaction(Id, Bytes) of
                        {ok, Transaction} ->
                            scan_files(Rest, Directory, Transactions#{Id => Transaction}, Failures);
                        error -> scan_files(Rest, Directory, Transactions, [Id | Failures])
                    end;
                Error -> Error
            end;
        _ -> scan_files(Rest, Directory, Transactions, Failures)
    end.

decode_transaction(Id, Bytes) ->
    case checksum(Bytes) =:= Id of
        false -> error;
        true ->
            try binary_to_term(Bytes, [safe, used]) of
                {{1, Time, Nonce, Parents, Bases, Events} = Transaction, Used}
                  when Used =:= byte_size(Bytes), is_integer(Time),
                       is_binary(Nonce), byte_size(Nonce) =:= 16,
                       is_list(Parents), is_list(Bases), is_list(Events) ->
                    case valid_list(fun valid_id/1, Parents) andalso
                         valid_list(fun valid_base/1, Bases) andalso
                         valid_list(fun valid_event/1, Events) of
                        true -> {ok, Transaction};
                        false -> error
                    end;
                _ -> error
            catch
                error:badarg -> error
            end
    end.

valid_list(_Check, []) -> true;
valid_list(Check, [Item | Rest]) -> Check(Item) andalso valid_list(Check, Rest);
valid_list(_Check, _ImproperTail) -> false.

valid_id(Id) -> is_binary(Id) andalso byte_size(Id) =:= 64.
valid_base({{id, Name}, {version, Version}}) ->
    valid_text(Name) andalso is_integer(Version) andalso Version >= 0;
valid_base(_) -> false.
valid_event({event, {id, Name}, Kind, Payload}) ->
    valid_text(Name) andalso valid_text(Kind) andalso is_bitstring(Payload);
valid_event(_) -> false.
valid_text(Text) -> is_binary(Text) andalso unicode:characters_to_binary(Text) =:= Text.

rebuild(Transactions, Failures, State) ->
    Ids = lists:sort(maps:keys(Transactions)),
    {Ranked, _Depths} = lists:mapfoldl(
        fun(Id, Depths) ->
            {Depth, Updated} = depth(Id, Transactions, Depths),
            {1, Time, Nonce, _, _, Events} = maps:get(Id, Transactions),
            {{Depth, Time, Nonce, Id, Events}, Updated}
        end, #{}, Ids),
    Dangling = lists:filtermap(
        fun(Id) ->
            {1, _, _, Parents, _, _} = maps:get(Id, Transactions),
            case [Parent || Parent <- Parents, not maps:is_key(Parent, Transactions)] of
                [] -> false;
                Missing -> {true, {dangling, Id, Missing}}
            end
        end, Ids),
    Referenced = maps:fold(
        fun(_, {1, _, _, Parents, _, _}, Set) ->
            lists:foldl(fun(Id, Acc) -> Acc#{Id => true} end, Set, Parents)
        end, #{}, Transactions),
    Tips = [Id || Id <- Ids, not maps:is_key(Id, Referenced)],
    case rebuild_streams(lists:sort(Ranked), Transactions, Dangling =:= [], #{}) of
        {ok, Streams} ->
            {ok, State#state{streams = Streams, tips = Tips, dangling = Dangling,
                             integrity_failures = Failures}};
        Error -> Error
    end.

rebuild_streams([], _, _, Streams) -> {ok, Streams};
rebuild_streams([{_, _, _, Id, Events} | Rest], Transactions, Complete, Streams) ->
    {1, _, _, _, Bases, _} = maps:get(Id, Transactions),
    case Complete andalso check_versions(Bases, Streams) =/= ok of
        true -> {error, conflicting_history};
        false -> rebuild_streams(Rest, Transactions, Complete, index_events(Events, Streams))
    end.

depth(Id, Transactions, Depths) ->
    case maps:find(Id, Depths) of
        {ok, Depth} -> {Depth, Depths};
        error ->
            {1, _, _, Parents, _, _} = maps:get(Id, Transactions),
            {Depth, Memo} = lists:foldl(
                fun(Parent, {Best, Acc}) ->
                    case maps:is_key(Parent, Transactions) of
                        true ->
                            {ParentDepth, Updated} = depth(Parent, Transactions, Acc),
                            {max(Best, ParentDepth + 1), Updated};
                        false -> {Best, Acc}
                    end
                end, {0, Depths}, Parents),
            {Depth, Memo#{Id => Depth}}
    end.

index_events(Events, Streams) ->
    lists:foldl(
        fun({event, Id, _, _} = Event, Index) ->
            {Version, Reversed} = maps:get(Id, Index, {0, []}),
            Index#{Id => {Version + 1, [Event | Reversed]}}
        end, Streams, Events).
