-module(eventcore_filesystem_graph).
-export([order/1, tips/1, dangling/1, forks/2, fork_contexts/2, topology_generation/1]).

order(Transactions) ->
    {Ranked, _} = maps:fold(
        fun(Id, #{time := Time, replica := Replica, nonce := Nonce}, {Keys, Memo}) ->
            {Depth, Updated} = depth(Id, Transactions, Memo),
            {[{Depth, Time, Replica, Nonce, Id} | Keys], Updated}
        end, {[], #{}}, Transactions),
    [Id || {_, _, _, _, Id} <- lists:sort(Ranked)].

depth(Id, Transactions, Memo) ->
    case maps:find(Id, Memo) of
        {ok, Depth} -> {Depth, Memo};
        error ->
            #{parents := Parents} = maps:get(Id, Transactions),
            {Depth, Visited} = lists:foldl(
                fun(Parent, {Best, Acc}) ->
                    case maps:is_key(Parent, Transactions) of
                        true ->
                            {ParentDepth, Updated} = depth(Parent, Transactions, Acc),
                            {max(Best, ParentDepth + 1), Updated};
                        false -> {Best, Acc}
                    end
                end, {0, Memo}, Parents),
            {Depth, Visited#{Id => Depth}}
    end.

tips(Transactions) ->
    Referenced = maps:fold(
        fun(_, #{parents := Parents}, Set) ->
            lists:foldl(fun(Id, Acc) -> Acc#{Id => true} end, Set, Parents)
        end, #{}, Transactions),
    lists:sort([Id || Id := _ <- Transactions, not maps:is_key(Id, Referenced)]).

dangling(Transactions) ->
    lists:sort(
        [{dangling, Id, Missing}
         || Id := #{parents := Parents} <- Transactions,
            Missing <- [[Parent || Parent <- Parents, not maps:is_key(Parent, Transactions)]],
            Missing =/= []]).

topology_generation(Transactions) ->
    maps:fold(fun
        (_, #{parents := [_, _ | _]}, Count) -> Count + 1;
        (_, _, Count) -> Count
    end, 0, Transactions).

forks(Transactions, Order) ->
    Replicas = maps:groups_from_list(
        fun(Id) -> maps:get(replica, maps:get(Id, Transactions)) end, Order),
    case replica_conflicts(lists:sort(maps:to_list(maps:remove(undefined, Replicas))),
                           Transactions) of
        ok -> {ok, stream_forks(Transactions)};
        Conflict -> {error, Conflict}
    end.

fork_contexts(Forks, Transactions) ->
    [{Fork, [{Id, branch_events(Stream, Id, Transactions)} || Id <- Ids]}
     || {fork, Stream, _, Ids} = Fork <- Forks].

branch_events(Stream, Id, Transactions) ->
    #{events := Events} = maps:get(Id, Transactions),
    [Event || {event, EventStream, _, _} = Event <- Events, EventStream =:= Stream].

replica_conflicts([], _) -> ok;
replica_conflicts([{Replica, Ids} | Rest], Transactions) ->
    case first_unrelated(Ids, Transactions) of
        none -> replica_conflicts(Rest, Transactions);
        {First, Second} -> {replica_identity_conflict, Replica, First, Second}
    end.

% The ordering puts ancestors first. If every consecutive pair is related
% then the whole replica history is a chain by transitivity.
first_unrelated([First, Second | Rest], Transactions) ->
    case ancestor(First, Second, Transactions) of
        true -> first_unrelated([Second | Rest], Transactions);
        false -> {First, Second}
    end;
first_unrelated(_, _) -> none.

stream_forks(Transactions) ->
    Claims = [{Base, Id} || Id := #{bases := Bases} <- Transactions, Base <- Bases],
    Groups = maps:groups_from_list(fun({Base, _}) -> Base end,
                                   fun({_, Id}) -> Id end, Claims),
    lists:filtermap(fun
        ({_, [_]}) -> false;
        ({{Stream, Version}, Ids}) ->
            case fork_heads(lists:usort(Ids), Transactions) of
                [] -> false;
                [_] -> false;
                Heads -> {true, {fork, Stream, Version, Heads}}
            end
    end, lists:sort(maps:to_list(Groups))).

fork_heads(Ids, Transactions) ->
    Heads = [Id || Id <- Ids,
                   not lists:any(fun(Other) -> ancestor(Id, Other, Transactions) end, Ids)],
    case lists:any(
        fun(Candidate) ->
            lists:all(fun(Head) -> ancestor(Head, Candidate, Transactions) end, Heads)
        end, maps:keys(Transactions)) of
        true -> [];
        false -> Heads
    end.

ancestor(Id, Id, _Transactions) -> false;
ancestor(Id, Descendant, Transactions) ->
    reachable(Id, [Descendant], Transactions, #{}).

reachable(_Target, [], _, _) -> false;
reachable(Target, [Target | _], _, _) -> true;
reachable(Target, [Current | Rest], Transactions, Visited) ->
    case {maps:is_key(Current, Visited), maps:find(Current, Transactions)} of
        {false, {ok, #{parents := Parents}}} ->
            reachable(Target, Parents ++ Rest, Transactions, Visited#{Current => true});
        _ -> reachable(Target, Rest, Transactions, Visited)
    end.
