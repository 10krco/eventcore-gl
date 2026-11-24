-module(eventcore_filesystem_ffi).
-behaviour(gen_server).
-export([start/2, start/3, start_with_replica/3, stop/1, status/1, read/3, append/3,
         event_prefix_token/2,
         replica_id/1, forks/1, topology_generation/1, fork_snapshot/1, resolve/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    owner, lock, directory, sync, replica,
    transactions = #{}, ordered = [], revision = 0,
    streams = #{}, tips = [], contested = #{},
    integrity_failures = [], dangling = []
}).

start(Root, Sync) ->
    start_with_replica(Root, Sync, none).

start_with_replica(Root, Sync, Replica) ->
    start_server(Root, Sync, Replica, fun(Directory) -> sync_directory(Directory, Sync) end).

% Accept a sync function to exercise open failures in the Erlang tests.
start(Root, Sync, SyncAncestor) ->
    start_server(Root, Sync, none, SyncAncestor).

start_server(Root, Sync, Replica, SyncAncestor) ->
    case gen_server:start(?MODULE, {self(), Root, Sync, Replica, SyncAncestor}, []) of
        {ok, Pid} -> {ok, Pid};
        {error, {shutdown, store_locked}} -> {error, store_locked};
        {error, {shutdown, lock_executable_missing}} -> {error, lock_executable_missing};
        {error, _} -> {error, cannot_open}
    end.

stop(Pid) ->
    try gen_server:stop(Pid) of
        ok -> {ok, nil}
    catch
        exit:_ -> {error, nil}
    end.

status(Pid) -> call(Pid, status, {error, journal_unavailable}).
replica_id(Pid) -> call(Pid, replica_id, {error, journal_unavailable}).
forks(Pid) -> call(Pid, forks, {error, journal_unavailable}).
topology_generation(Pid) -> call(Pid, topology_generation, {error, journal_unavailable}).
event_prefix_token(Pid, Count) -> call(Pid, {event_prefix_token, Count}, {error, journal_unavailable}).
fork_snapshot(Pid) -> call(Pid, fork_snapshot, {error, journal_unavailable}).
resolve(Pid, Revision, Fork, Events) ->
    call(Pid, {resolve, Revision, Fork, Events}, {error, journal_unavailable}).
read(Pid, Id, After) -> call(Pid, {read, Id, After}, {error, nil}).
append(Pid, Expected, Events) ->
    call(Pid, {append, Expected, Events}, {error, {store_failure, append_events}}).

call(Pid, Request, Failure) ->
    try gen_server:call(Pid, Request, infinity)
    catch
        exit:_ -> Failure
    end.

init({Owner, RequestedRoot, Sync, Replica, SyncAncestor}) ->
    process_flag(trap_exit, true),
    Root = filename:absname(RequestedRoot),
    Directory = filename:join(Root, <<"events">>),
    case ensure_directory(Directory, Sync, SyncAncestor) of
        ok ->
            case acquire_lock(filename:join(Root, <<".lock">>)) of
                {ok, Lock} ->
                    State = #state{owner = monitor(process, Owner), lock = Lock,
                                   directory = Directory, sync = Sync},
                    case prepare(Root, Replica, State) of
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

prepare(Root, Replica, #state{sync = Sync} = State) ->
    case create_metadata(Root) of
        ok ->
            Publish = fun(Path, Bytes) -> publish_identity(Path, Bytes, Sync) end,
            case eventcore_filesystem_replica:load(Root, Replica, Publish) of
                {ok, Id} ->
                    case sync_metadata(Root, Replica, Sync) of
                        ok -> scan(State#state{replica = Id});
                        Error -> Error
                    end;
                Error -> Error
            end;
        Error -> Error
    end.

publish_identity(Path, Bytes, Sync) ->
    Nonce = binary:encode_hex(crypto:strong_rand_bytes(16), lowercase),
    Temp = <<Path/binary, ".", Nonce/binary, ".tmp">>,
    write_transaction(Temp, Path, Bytes, Sync).

sync_metadata(Root, {some, _}, Sync) -> sync_directory(Root, Sync);
sync_metadata(Root, none, Sync) ->
    case sync_directory(filename:join(Root, <<".eventcore">>), Sync) of
        ok -> sync_directory(Root, Sync);
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

handle_call(status, _From, #state{integrity_failures = Bad, dangling = [_ | _] = Dangling,
                                 transactions = Transactions, ordered = Reversed} = State) ->
    Forks = [Fork || {Fork, _} <- eventcore_filesystem_graph:stream_forks(Transactions, lists:reverse(Reversed))],
    {reply, {ok, {status, Bad, Dangling, Forks}}, State};
handle_call(status, _From, #state{integrity_failures = Bad} = State) ->
    Reply = case detected_forks(State) of
        {ok, Forks} -> {ok, {status, Bad, [], Forks}};
        Error -> Error
    end,
    {reply, Reply, State};
handle_call(replica_id, _From, #state{replica = Replica} = State) ->
    {reply, {ok, Replica}, State};
handle_call(forks, _From, State) ->
    {reply, detected_forks(State), State};
handle_call(topology_generation, _From, #state{transactions = Transactions} = State) ->
    {reply, {ok, eventcore_filesystem_graph:topology_generation(Transactions)}, State};
handle_call({event_prefix_token, Count}, _From,
            #state{transactions = Transactions, ordered = Reversed} = State) ->
    Token = eventcore_filesystem_graph:event_prefix_token(Count, lists:reverse(Reversed), Transactions),
    {reply, Token, State};
handle_call(fork_snapshot, _From, #state{revision = Revision} = State) ->
    Reply = case fork_contexts(State) of
        {ok, Contexts} -> {ok, {Revision, Contexts}};
        Error -> Error
    end,
    {reply, Reply, State};
handle_call({resolve, Revision, _, _}, _From, #state{revision = Current} = State)
  when Revision =/= Current ->
    {reply, {error, reconciliation_changed}, State};
handle_call({resolve, _, _, _}, _From, #state{dangling = [_ | _]} = State) ->
    {reply, {error, journal_unavailable}, State};
handle_call({resolve, _, {fork, Stream, Base, Heads} = Fork, Events}, _From,
            #state{streams = Streams, contested = Contested, transactions = Transactions} = State) ->
    case fork_contexts(State) of
        {ok, Contexts} ->
            case lists:keymember(Fork, 1, Contexts) of
                false -> {reply, {error, reconciliation_changed}, State};
                true ->
                    Bases = resolution_bases(Events, Streams),
                    OtherStreams = maps:from_keys([Id || {Id, _} <- Bases, Id =/= Stream], true),
                    Dependencies = stream_dependencies(OtherStreams, Transactions),
                    Parents = lists:usort(Heads ++ Dependencies ++ latest_local_transaction(State)),
                    OtherContested = maps:remove(Stream, Contested),
                    case check_contested(Events, OtherContested) of
                        ok -> reply_to_resolution(persist(Bases, Events, Parents, {{Stream, Base}, Heads}, State));
                        Error -> {reply, {error, {journal_store_error, Error}}, State}
                    end
            end;
        Error -> {reply, Error, State}
    end;
handle_call({read, Id, After}, _From, #state{streams = Streams} = State) ->
    {Version, Reversed} = maps:get(Id, Streams, {0, []}),
    Tail = lists:reverse(lists:sublist(Reversed, max(0, Version - After))),
    {reply, {ok, Tail}, State};
handle_call({append, _, _}, _From, #state{dangling = [_ | _]} = State) ->
    {reply, {error, {store_failure, append_events}}, State};
handle_call({append, Expected, Events}, _From,
            #state{streams = Streams, tips = Parents, contested = Contested} = State) ->
    case check_versions(Expected, Streams) of
        ok when Events =:= [] -> {reply, {ok, nil}, State};
        ok ->
            case check_contested(Events, Contested) of
                ok ->
                    {Reply, Updated} = persist(Expected, Events, Parents, none, State),
                    {reply, Reply, Updated};
                Error -> {reply, {error, Error}, State}
            end;
        Error -> {reply, {error, Error}, State}
    end.

check_contested([], _) -> ok;
check_contested([{event, Stream, _, _} | Rest], Contested) ->
    case maps:is_key(Stream, Contested) of
        true -> {unresolved_stream_fork, Stream};
        false -> check_contested(Rest, Contested)
    end.

reply_to_resolution({{ok, nil}, Updated}) ->
    {reply, {ok, Updated#state.revision}, Updated};
reply_to_resolution({{error, Failure}, Updated}) ->
    {reply, {error, {journal_store_error, Failure}}, Updated}.

latest_local_transaction(#state{replica = Replica, ordered = Reversed, transactions = Transactions}) ->
    case lists:search(fun(Id) -> maps:get(replica, maps:get(Id, Transactions)) =:= Replica end, Reversed) of
        {value, Id} -> [Id];
        false -> []
    end.

check_versions([], _Streams) -> ok;
check_versions([{Id, {version, Expected}} | Rest], Streams) ->
    case maps:get(Id, Streams, {0, []}) of
        {Expected, _} -> check_versions(Rest, Streams);
        {Actual, _} -> {version_conflict, Id, {version, Expected}, {version, Actual}}
    end.

stream_dependencies(Streams, _Transactions) when map_size(Streams) =:= 0 -> [];
stream_dependencies(Streams, Transactions) ->
    [Id || Id := #{events := Events, resolution := Resolution} <- Transactions,
           lists:any(fun({event, Stream, _, _}) -> maps:is_key(Stream, Streams) end, Events)
           orelse resolves_stream(Resolution, Streams)].

resolves_stream(none, _) -> false;
resolves_stream({{Stream, _}, _}, Streams) -> maps:is_key(Stream, Streams).

resolution_bases(Events, Streams) ->
    Ids = lists:usort([Id || {event, Id, _, _} <- Events]),
    [{Id, {version, element(1, maps:get(Id, Streams, {0, []}))}} || Id <- Ids].

detected_forks(State) ->
    case fork_contexts(State) of
        {ok, Contexts} -> {ok, [Fork || {Fork, _} <- Contexts]};
        Error -> Error
    end.

fork_contexts(#state{transactions = Transactions, ordered = Reversed}) ->
    eventcore_filesystem_graph:forks(Transactions, lists:reverse(Reversed)).

persist(Expected, Events, Parents, Resolution, #state{directory = Directory, sync = Sync,
                                         replica = Replica, transactions = Transactions} = State) ->
    Time = os:system_time(nanosecond),
    Nonce = crypto:strong_rand_bytes(16),
    Bases = lists:sort(Expected),
    Transaction = {3, Time, Replica, Nonce, Parents, Bases, Events, Resolution},
    Bytes = term_to_binary(Transaction),
    Id = checksum(Bytes),
    Path = filename:join(Directory, <<Id/binary, ".txn">>),
    Temp = filename:join(Directory, <<Id/binary, ".tmp">>),
    Failure = {error, {store_failure, append_events}},
    case write_transaction(Temp, Path, Bytes, Sync) of
        ok ->
            Record = transaction_record(Time, Replica, Nonce, Parents, Bases, Events, Resolution),
            Updated = index_new_transaction(Id, Record, Transactions#{Id => Record}, State),
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
                {{1, Time, Nonce, Parents, Bases, Events}, Used} when Used =:= byte_size(Bytes) ->
                    validate_transaction(Time, undefined, Nonce, Parents, Bases, Events, none);
                {{2, Time, Replica, Nonce, Parents, Bases, Events}, Used}
                  when Used =:= byte_size(Bytes), is_binary(Replica), byte_size(Replica) > 0 ->
                    validate_transaction(Time, Replica, Nonce, Parents, Bases, Events, none);
                {{3, Time, Replica, Nonce, Parents, Bases, Events, Resolution}, Used}
                  when Used =:= byte_size(Bytes), is_binary(Replica), byte_size(Replica) > 0 ->
                    validate_transaction(Time, Replica, Nonce, Parents, Bases, Events, Resolution);
                _ -> error
            catch
                error:badarg -> error
            end
    end.

validate_transaction(Time, Replica, Nonce, Parents, Bases, Events, Resolution)
  when is_integer(Time), is_binary(Nonce), byte_size(Nonce) =:= 16 ->
    case valid_list(fun valid_id/1, Parents) andalso
         valid_list(fun valid_base/1, Bases) andalso
         valid_list(fun valid_event/1, Events) andalso valid_resolution(Resolution, Parents) of
        true -> {ok, transaction_record(Time, Replica, Nonce, Parents, Bases, Events, Resolution)};
        false -> error
    end;
validate_transaction(_, _, _, _, _, _, _) -> error.

transaction_record(Time, Replica, Nonce, Parents, Bases, Events, Resolution) ->
    #{time => Time, replica => Replica, nonce => Nonce, parents => Parents,
      bases => Bases, events => Events, resolution => Resolution}.

valid_resolution(none, _) -> true;
valid_resolution({Key, [First, Second | _] = Heads}, Parents) when First =/= Second ->
    valid_base(Key) andalso valid_list(fun valid_id/1, Heads) andalso
        lists:all(fun(Head) -> lists:member(Head, Parents) end, Heads);
valid_resolution(_, _) -> false.

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
    {ok, index_transactions(Transactions, State#state{integrity_failures = Failures})}.

index_new_transaction(Id, #{parents := Tips, events := Events, resolution := none}, Transactions,
                      #state{tips = Tips, streams = Streams, ordered = Reversed} = State) ->
    State#state{transactions = Transactions, revision = map_size(Transactions),
                ordered = [Id | Reversed], tips = [Id], streams = index_events(Events, Streams)};
index_new_transaction(_Id, _Record, Transactions, State) ->
    index_transactions(Transactions, State).

index_transactions(Transactions, State) ->
    Ordered = eventcore_filesystem_graph:order(Transactions),
    Forks = eventcore_filesystem_graph:stream_forks(Transactions, Ordered),
    Streams = lists:foldl(fun(Id, Index) ->
        #{events := Events} = maps:get(Id, Transactions),
        index_events(Events, Index)
    end, #{}, Ordered),
    Contested = maps:from_keys([Stream || {{fork, Stream, _, _}, _} <- Forks], true),
    State#state{transactions = Transactions, revision = map_size(Transactions), contested = Contested,
                ordered = lists:reverse(Ordered), streams = Streams, tips = eventcore_filesystem_graph:tips(Transactions),
                dangling = eventcore_filesystem_graph:dangling(Transactions)}.

index_events(Events, Streams) ->
    lists:foldl(
        fun({event, Id, _, _} = Event, Index) ->
            {Version, Reversed} = maps:get(Id, Index, {0, []}),
            Index#{Id => {Version + 1, [Event | Reversed]}}
        end, Streams, Events).
