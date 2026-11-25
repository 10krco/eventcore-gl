-module(eventcore_filesystem_ffi).
-behaviour(gen_server).
-export([start/2, start/3, start_with_replica/3, start_with_reconciliation/4,
         stop/1, status/1, read/3, append/3,
         event_prefix_token/2, history_revision/1, read/4, append/4, load_snapshot/3, save_snapshot/5,
         replica_id/1, forks/1, topology_generation/1, fork_snapshot/1, resolve/4,
         projection_frontier/1, projection_fetch/3, projection_load/2, projection_save/3, projection_fenced_save/4,
         open_projection_checkpoints/2, standalone_projection_load/2, standalone_projection_save/4,
         standalone_projection_fenced_save/5,
         open_projection_coordinator/1, standalone_projection_acquire/2, projection_acquire/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    owner, lock, directory, snapshot_directory, sync, replica,
    transactions = #{}, ordered = [], revision = 0, history_revision = 0,
    streams = #{}, tips = [], contested = #{},
    integrity_failures = [], dangling = [],
    projection_directory, ingestion, sync_journal, reconciliation
}).

start(Root, Sync) ->
    start_with_replica(Root, Sync, none).

start_with_replica(Root, Sync, Replica) ->
    start_with_reconciliation(Root, Sync, Replica, full_branches).

start_with_reconciliation(Root, Sync, Replica, Reconciliation) ->
    SyncDirectory = fun(Directory) -> sync_directory(Directory, Sync) end,
    start_server(Root, Sync, Replica, SyncDirectory, Reconciliation).

% Accept a directory sync function to exercise failures in tests.
start(Root, Sync, SyncAncestor) ->
    start_server(Root, Sync, none, SyncAncestor, full_branches).

start_server(Root, Sync, Replica, SyncAncestor, Reconciliation) ->
    case gen_server:start(?MODULE, {self(), Root, Sync, Replica, SyncAncestor, Reconciliation}, []) of
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
history_revision(Pid) -> call(Pid, history_revision, {error, {store_failure, read_stream}}).

read(Pid, Id, After) -> read(Pid, none, Id, After).
read(Pid, Revision, Id, After) ->
    guarded_call(Pid, Revision, {read, Id, After}, read_stream).

append(Pid, Expected, Events) -> append(Pid, none, Expected, Events).
append(Pid, Revision, Expected, Events) ->
    guarded_call(Pid, Revision, {append, Expected, Events}, append_events).

projection_frontier(Pid) ->
    guarded_call(Pid, none, projection_frontier, read_projection).

projection_fetch(Pid, First, Last) ->
    guarded_call(Pid, none, {projection_fetch, First, Last}, read_projection).

projection_load(Pid, Name) ->
    guarded_call(Pid, none, {projection_load, Name}, load_checkpoint).

projection_save(Pid, Name, Position) ->
    guarded_call(Pid, none, {projection_save, Name, Position}, save_checkpoint).

open_projection_checkpoints(Root, Sync) ->
    open_projection_directory(Root, <<"checkpoints">>, Sync).

open_projection_coordinator(Root) ->
    open_projection_directory(Root, <<"locks">>, unsynced).

open_projection_directory(Root, Name, Sync) ->
    Directory = filename:join([filename:absname(Root), <<".eventcore">>, <<"projection">>, Name]),
    case ensure_directory(Directory, Sync, fun(Parent) -> sync_directory(Parent, Sync) end) of
        ok -> {ok, Directory};
        _ -> {error, cannot_open}
    end.

standalone_projection_load(Directory, Name) ->
    case read_projection_checkpoint(Directory, Name) of
        {error, nil} -> {error, {store_failure, load_checkpoint}};
        Reply -> Reply
    end.

standalone_projection_save(Directory, Sync, Name, Position) ->
    case write_projection_checkpoint(Directory, Sync, Name, Position) of
        {error, nil} -> {error, {store_failure, save_checkpoint}};
        Reply -> Reply
    end.

standalone_projection_acquire(Directory, Name) ->
    acquire_projection_lock(Directory, Name, none).

projection_acquire(Pid, _Name) when node(Pid) =/= node() ->
    {error, remote_holder};
projection_acquire(Pid, Name) ->
    case call(Pid, projection_lock_directory, {error, coordinator_unavailable}) of
        {ok, Directory} -> acquire_projection_lock(Directory, Name, {some, Pid});
        Error -> Error
    end.

acquire_projection_lock(Directory, Name, Journal) ->
    Path = filename:join(Directory, <<(checksum(Name))/binary, ".lock">>),
    case open_projection_lock(Directory, Path, Journal) of
        {ok, Worker, Port, Token} ->
            Release = fun() -> release_projection_lock(Worker, Token) end,
            Check = fun() -> check_projection_lock(Worker, Port, Journal) end,
            Fence = {Name, <<Path/binary, ".checkpoint">>, Check},
            {ok, {checked_lease, Release, Check, Fence}};
        Error -> Error
    end.

open_projection_lock(Directory, Path, Journal) ->
    Owner = self(),
    Reference = make_ref(),
    {Worker, Monitor} = spawn_monitor(fun() ->
        projection_lock_worker(Owner, Reference, Directory, Path, Journal)
    end),
    receive
        {Reference, Reply} ->
            erlang:demonitor(Monitor, [flush]),
            Reply;
        {'DOWN', Monitor, process, Worker, _} ->
            {error, coordinator_unavailable}
    end.

projection_lock_worker(Owner, Reference, Directory, Path, Journal) ->
    process_flag(trap_exit, true),
    OwnerMonitor = monitor(process, Owner),
    JournalMonitor = case Journal of
        none -> none;
        {some, JournalPid} -> monitor(process, JournalPid)
    end,
    Lock = case filelib:ensure_path(Directory) of
        ok -> acquire_lock(Path);
        Error -> Error
    end,
    case Lock of
        {ok, Port} ->
            Worker = self(),
            Token = make_ref(),
            Owner ! {Reference, {ok, Worker, Port, Token}},
            receive
                {release, Token} -> release_lock(Port);
                {'DOWN', OwnerMonitor, process, _, _} -> release_lock(Port);
                {'DOWN', JournalMonitor, process, _, _} -> release_lock(Port);
                {Port, {exit_status, _}} -> ok;
                {'EXIT', Port, _} -> ok
            end;
        {error, store_locked} -> Owner ! {Reference, {error, already_leading}};
        _ -> Owner ! {Reference, {error, coordinator_unavailable}}
    end.

projection_fenced_save(Pid, _Name, _Position, _Fence) when node(Pid) =/= node() ->
    {error, lease_lost};
projection_fenced_save(Pid, Name, Position, Fence) ->
    case call(Pid, projection_checkpoint_config, {error, lease_lost}) of
        {ok, Directory, Sync} ->
            standalone_projection_fenced_save(Directory, Sync, Name, Position, Fence);
        Error -> Error
    end.

standalone_projection_fenced_save(Directory, Sync, Name, {position, Sequence},
                                  {Name, Lock, Check})
    when Sequence > 0, Sequence < (1 bsl 64) ->
    case ensure_directory(Directory, Sync, fun(Parent) -> sync_directory(Parent, Sync) end) of
        ok -> publish_fenced_checkpoint(Directory, Sync, Name, Sequence, Lock, Check);
        Error -> checkpoint_write_result(Error)
    end;
standalone_projection_fenced_save(_, _, _, _, _) -> {error, lease_lost}.

publish_fenced_checkpoint(Directory, Sync, Name, Sequence, Lock, Check) ->
    Path = projection_checkpoint_path(Directory, Name),
    Nonce = binary:encode_hex(crypto:strong_rand_bytes(16), lowercase),
    Temp = <<Path/binary, ".", Nonce/binary, ".tmp">>,
    try
        case prepare_rename(Temp, <<Sequence:64>>, Sync) of
            ok ->
                Deadline = erlang:monotonic_time(millisecond) + 5000,
                case fenced_rename(Temp, Path, Lock, Check, Deadline) of
                    ok -> checkpoint_write_result(sync_directory(Directory, Sync));
                    {error, lease_lost} = Error -> Error;
                    Error -> checkpoint_write_result(Error)
                end;
            Error -> checkpoint_write_result(Error)
        end
    after
        file:delete(Temp)
    end.

checkpoint_write_result(ok) -> {ok, nil};
checkpoint_write_result(_) -> {error, {write_failed, {store_failure, save_checkpoint}}}.

fenced_rename(Temp, Path, Lock, Check, Deadline) ->
    case os:find_executable("flock") of
        false -> {error, lease_lost};
        Executable ->
            % The process holding flock performs the rename itself via exec.
            % A terminated lock holder cannot resume a pending rename.
            Port = open_port({spawn_executable, Executable},
                             [binary, exit_status, use_stdio,
                              {args, ["-F", "-n", "-E", "75", "--", Lock,
                                      "sh", "-c",
                                      "printf R; IFS= read -r action && [ \"$action\" = commit ] && exec mv -fT -- \"$1\" \"$2\"",
                                      "checkpoint", Temp, Path]}]),
            try
                receive
                    {Port, {data, <<"R">>}} ->
                        case Check() of
                            {ok, nil} -> commit_fenced_rename(Port);
                            _ -> {error, lease_lost}
                        end;
                    {Port, {exit_status, 75}} ->
                        case erlang:monotonic_time(millisecond) < Deadline of
                            true ->
                                timer:sleep(10),
                                fenced_rename(Temp, Path, Lock, Check, Deadline);
                            false -> {error, lease_lost}
                        end;
                    {Port, {exit_status, _}} -> {error, lease_lost}
                after 5000 -> {error, lease_lost}
                end
            after
                catch port_close(Port)
            end
    end.

commit_fenced_rename(Port) ->
    case catch port_command(Port, <<"commit\n">>) of
        true ->
            receive
                {Port, {exit_status, 0}} -> ok;
                {Port, {exit_status, 1}} -> {error, rename_failed};
                {Port, {exit_status, _}} -> {error, lease_lost}
            after 5000 -> {error, lease_lost}
            end;
        _ -> {error, lease_lost}
    end.

check_projection_lock(Worker, Port, Journal) ->
    case is_process_alive(Worker) andalso erlang:port_info(Port) =/= undefined
         andalso journal_alive(Journal) of
        true -> {ok, nil};
        false -> {error, coordinator_unavailable}
    end.

journal_alive(none) -> true;
journal_alive({some, Pid}) -> is_process_alive(Pid).

release_projection_lock(Worker, Token) ->
    Monitor = monitor(process, Worker),
    Worker ! {release, Token},
    receive
        {'DOWN', Monitor, process, Worker, _} -> nil
    end.

load_snapshot(Pid, Revision, Name) ->
    guarded_call(Pid, {some, Revision}, {load_snapshot, Name}, load_snapshot).

save_snapshot(Pid, Revision, Name, Observed, Value) ->
    guarded_call(Pid, {some, Revision}, {save_snapshot, Name, Observed, Value}, save_snapshot).

guarded_call(Pid, Revision, Request, Operation) ->
    Reply = call(Pid, {with_history, Revision, Request}, {error, {store_failure, Operation}}),
    case Reply of
        {error, nil} -> {error, {store_failure, Operation}};
        _ -> Reply
    end.

call(Pid, Request, Failure) ->
    try gen_server:call(Pid, Request, infinity)
    catch
        exit:_ -> Failure
    end.

init({Owner, RequestedRoot, Sync, Replica, SyncAncestor, Reconciliation}) ->
    process_flag(trap_exit, true),
    Root = filename:absname(RequestedRoot),
    Directory = filename:join(Root, <<"events">>),
    case ensure_directory(Directory, Sync, SyncAncestor) of
        ok ->
            case acquire_lock(filename:join(Root, <<".lock">>)) of
                {ok, Lock} ->
                    State = #state{owner = monitor(process, Owner), lock = Lock,
                                   directory = Directory, sync = Sync, reconciliation = Reconciliation,
                                   sync_journal = fun() -> SyncAncestor(Directory) end,
                                   projection_directory = filename:join([Root, <<".eventcore">>, <<"projection">>]),
                                   snapshot_directory = filename:join([Root, <<".eventcore">>, <<"snapshots">>])},
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
                        ok -> open_ingestion(scan(State#state{replica = Id}));
                        Error -> Error
                    end;
                Error -> Error
            end;
        Error -> Error
    end.

open_ingestion({ok, #state{projection_directory = Directory, transactions = Transactions,
                           ordered = Reversed} = State}) ->
    IngestionDirectory = filename:join(Directory, <<"ingestion">>),
    case eventcore_filesystem_ingestion:open(IngestionDirectory, lists:reverse(Reversed), Transactions) of
        {ok, Index} ->
            case sync_and_flush_ingestion(State#state{ingestion = Index}) of
                {ok, _, Updated} -> {ok, Updated};
                _ -> {error, ingestion_unavailable}
            end;
        Error -> Error
    end;
open_ingestion(Error) -> Error.

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

handle_call(projection_lock_directory, _From, #state{projection_directory = Root} = State) ->
    {reply, {ok, filename:join(Root, <<"locks">>)}, State};
handle_call(projection_checkpoint_config, _From,
            #state{projection_directory = Root, sync = Sync} = State) ->
    {reply, {ok, filename:join(Root, <<"checkpoints">>), Sync}, State};
handle_call(projection_frontier, _From, State) ->
    case sync_and_flush_ingestion(State) of
        {ok, High, Updated} -> {reply, {ok, High}, Updated};
        _ -> {reply, {error, nil}, State}
    end;
handle_call({projection_fetch, First, Last}, _From, #state{ingestion = Index} = State) ->
    {reply, eventcore_filesystem_ingestion:fetch(First, Last, Index), State};
handle_call({projection_load, Name}, _From, #state{projection_directory = Root} = State) ->
    {reply, read_projection_checkpoint(filename:join(Root, <<"checkpoints">>), Name), State};
handle_call({projection_save, Name, Position}, _From,
            #state{projection_directory = Root, sync = Sync} = State) ->
    {reply, write_projection_checkpoint(filename:join(Root, <<"checkpoints">>), Sync, Name, Position), State};
handle_call(status, _From, #state{reconciliation = rust_release,
                                 integrity_failures = Bad, dangling = [_ | _] = Dangling,
                                 transactions = Transactions} = State) ->
    Forks = [Fork || {Fork, _} <- eventcore_filesystem_graph:release_stream_forks(Transactions)],
    {reply, {ok, {status, Bad, Dangling, Forks}}, State};
handle_call(status, _From, #state{reconciliation = full_branches,
                                 integrity_failures = Bad, dangling = [_ | _] = Dangling,
                                 transactions = Transactions, ordered = Reversed} = State) ->
    Forks = [Fork || {Fork, _} <- eventcore_filesystem_graph:stream_forks(Transactions, lists:reverse(Reversed))],
    {reply, {ok, {status, Bad, Dangling, Forks}}, State};
handle_call(status, _From, #state{integrity_failures = Bad, dangling = Dangling} = State) ->
    Reply = case detected_forks(State) of
        {ok, Forks} -> {ok, {status, Bad, Dangling, Forks}};
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
handle_call({resolve, _, {fork, _, _, Heads}, Events}, _From,
            #state{reconciliation = rust_release, streams = Streams} = State) ->
    reply_to_resolution(persist(resolution_bases(Events, Streams), Events, Heads, none, State));
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
handle_call(history_revision, _From, #state{history_revision = Revision} = State) ->
    {reply, {ok, Revision}, State};
handle_call({with_history, {some, Revision}, _}, _From, #state{history_revision = Current} = State)
  when Revision =/= Current ->
    {reply, {error, journal_history_changed}, State};
handle_call({with_history, _, Request}, From, State) ->
    handle_call(Request, From, State);
handle_call({load_snapshot, Name}, _From, State) ->
    {reply, read_snapshot(snapshot_path(Name, State), State), State};
handle_call({save_snapshot, Name, Observed, Value}, _From, State) ->
    {reply, persist_snapshot(snapshot_path(Name, State), Observed, Value, State), State};
handle_call({read, Id, After}, _From, #state{streams = Streams} = State) ->
    {Version, Reversed} = maps:get(Id, Streams, {0, []}),
    Tail = lists:reverse(lists:sublist(Reversed, max(0, Version - After))),
    {reply, {ok, Tail}, State};
handle_call({append, _, []}, _From, #state{reconciliation = rust_release} = State) ->
    {reply, {ok, nil}, State};
handle_call({append, _, _}, _From,
            #state{reconciliation = full_branches, dangling = [_ | _]} = State) ->
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

fork_contexts(#state{reconciliation = rust_release, transactions = Transactions, ordered = Reversed}) ->
    eventcore_filesystem_graph:release_forks(Transactions, lists:reverse(Reversed));
fork_contexts(#state{transactions = Transactions, ordered = Reversed}) ->
    eventcore_filesystem_graph:forks(Transactions, lists:reverse(Reversed)).

persist(Expected, Events, Parents, Resolution, #state{directory = Directory, sync = Sync,
                                         replica = Replica, transactions = Transactions,
                                         sync_journal = SyncJournal} = State) ->
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
            #state{ingestion = Index} = State,
            Ingested = State#state{ingestion = eventcore_filesystem_ingestion:add(Id, Events, Index)},
            Updated = index_new_transaction(Id, Record, Transactions#{Id => Record}, Ingested),
            case SyncJournal() of
                ok ->
                    case flush_ingestion(Updated) of
                        {ok, _, Flushed} -> {{ok, nil}, Flushed};
                        _ -> {Failure, Updated}
                    end;
                {error, _} -> {Failure, Updated}
            end;
        {error, _} -> {Failure, State}
    end.

sync_and_flush_ingestion(#state{ingestion = Index, sync_journal = SyncJournal} = State) ->
    case eventcore_filesystem_ingestion:needs_flush(Index) of
        false -> flush_ingestion(State);
        true ->
            case SyncJournal() of
                ok -> flush_ingestion(State);
                Error -> Error
            end
    end.

flush_ingestion(#state{ingestion = Index, sync = Sync} = State) ->
    Publish = fun(Directory, Path, Data) -> publish_snapshot(Directory, Path, Data, Sync) end,
    case eventcore_filesystem_ingestion:flush(Publish, Index) of
        {ok, High, Updated} -> {ok, High, State#state{ingestion = Updated}};
        Error -> Error
    end.

read_projection_checkpoint(Directory, Name) ->
    Path = projection_checkpoint_path(Directory, Name),
    case file:read_file(Path) of
        {ok, <<Sequence:64>>} when Sequence > 0 -> {ok, {some, {position, Sequence}}};
        {error, enoent} -> {ok, none};
        _ -> {error, nil}
    end.

write_projection_checkpoint(Directory, Sync, Name, {position, Sequence})
    when Sequence > 0, Sequence < (1 bsl 64) ->
    publish_snapshot(Directory, projection_checkpoint_path(Directory, Name), <<Sequence:64>>, Sync);
write_projection_checkpoint(_, _, _, _) -> {error, nil}.

projection_checkpoint_path(Directory, Name) ->
    filename:join(Directory, <<(checksum(Name))/binary, ".checkpoint">>).

snapshot_path(Name, #state{snapshot_directory = Directory}) ->
    filename:join(Directory, <<(checksum(Name))/binary, ".snapshot">>).

read_snapshot(Path, State) ->
    case file:read_file(Path) of
        {ok, <<Count:64/unsigned, Token:64/binary, Bytes/binary>>} ->
            case snapshot_prefix(State, Count) of
                {ok, Token} ->
                    case eventcore_snapshot_wire:decode(Bytes) of
                        {ok, Value} -> {ok, {some, Value}};
                        Error -> Error
                    end;
                _ -> {ok, none}
            end;
        {ok, _} -> {error, nil};
        {error, enoent} -> {ok, none};
        {error, _} -> {error, nil}
    end.

snapshot_prefix(#state{transactions = Transactions, ordered = Reversed}, Count) ->
    eventcore_filesystem_graph:event_prefix_token(Count, lists:reverse(Reversed), Transactions).

persist_snapshot(Path, Observed, Value, #state{snapshot_directory = Directory, sync = Sync,
                                     streams = Streams} = State) ->
    Replace = case read_snapshot(Path, State) of
        {ok, Current} -> eventcore@snapshot:can_replace(Current, Observed, Value);
        {error, nil} -> error
    end,
    case Replace of
        false -> {ok, nil};
        error -> {error, nil};
        true ->
            Count = maps:fold(fun(_, {Version, _}, Sum) -> Sum + Version end, 0, Streams),
            case snapshot_prefix(State, Count) of
                {ok, Token} ->
                    Body = eventcore_snapshot_wire:encode(Value),
                    Bytes = <<Count:64/unsigned, Token/binary, Body/binary>>,
                    publish_snapshot(Directory, Path, Bytes, Sync);
                _ -> {error, nil}
            end
    end.

publish_snapshot(Directory, Path, Bytes, Sync) ->
    case ensure_directory(Directory, Sync, fun(Parent) -> sync_directory(Parent, Sync) end) of
        ok ->
            case publish_identity(Path, Bytes, Sync) of
                ok ->
                    case sync_directory(Directory, Sync) of
                        ok -> {ok, nil};
                        _ -> {error, nil}
                    end;
                _ -> {error, nil}
            end;
        _ -> {error, nil}
    end.

write_transaction(Temp, Path, Bytes, Sync) ->
    case prepare_rename(Temp, Bytes, Sync) of
        ok ->
            case file:rename(Temp, Path) of
                ok -> ok;
                Error ->
                    file:delete(Temp),
                    Error
            end;
        Error -> Error
    end.

prepare_rename(Temp, Bytes, Sync) ->
    case file:open(Temp, [write, raw, binary, exclusive]) of
        {ok, File} ->
            Written = case file:write(File, Bytes) of
                ok -> sync_file(File, Sync);
                Error -> Error
            end,
            Closed = file:close(File),
            case {Written, Closed} of
                {ok, ok} -> ok;
                {{error, _} = Error1, _} ->
                    file:delete(Temp),
                    Error1;
                {ok, Error2} ->
                    file:delete(Temp),
                    Error2
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
index_new_transaction(Id, #{parents := Parents, events := Events}, Transactions,
                      #state{reconciliation = rust_release, tips = Tips,
                             streams = Streams, ordered = Reversed} = State) ->
    % The release appends resolution events to a live index and reorders
    % by graph depth when the journal opens again.
    Remaining = ordsets:subtract(Tips, Parents),
    State#state{transactions = Transactions, revision = map_size(Transactions),
                ordered = [Id | Reversed], tips = ordsets:add_element(Id, Remaining),
                streams = index_events(Events, Streams)};
index_new_transaction(_Id, _Record, Transactions, State) ->
    index_transactions(Transactions, State#state{history_revision = State#state.history_revision + 1}).

index_transactions(Transactions, #state{reconciliation = Reconciliation} = State) ->
    Ordered = eventcore_filesystem_graph:order(Transactions),
    Streams = lists:foldl(fun(Id, Index) ->
        #{events := Events} = maps:get(Id, Transactions),
        index_events(Events, Index)
    end, #{}, Ordered),
    Contested = case Reconciliation of
        full_branches ->
            Forks = eventcore_filesystem_graph:stream_forks(Transactions, Ordered),
            maps:from_keys([Stream || {{fork, Stream, _, _}, _} <- Forks], true);
        rust_release -> #{}
    end,
    State#state{transactions = Transactions, revision = map_size(Transactions), contested = Contested,
                ordered = lists:reverse(Ordered), streams = Streams, tips = eventcore_filesystem_graph:tips(Transactions),
                dangling = eventcore_filesystem_graph:dangling(Transactions)}.

index_events(Events, Streams) ->
    lists:foldl(
        fun({event, Id, _, _} = Event, Index) ->
            {Version, Reversed} = maps:get(Id, Index, {0, []}),
            Index#{Id => {Version + 1, [Event | Reversed]}}
        end, Streams, Events).
