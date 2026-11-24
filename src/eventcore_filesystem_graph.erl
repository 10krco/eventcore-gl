-module(eventcore_filesystem_graph).
-export([order/1, tips/1, dangling/1, forks/2, stream_forks/2,
         topology_generation/1, event_prefix_token/3]).

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
    CollisionCandidates = case dangling(Transactions) of
        [] -> Order;
        _ -> [Id || Id <- Order, not incomplete_ancestry(Id, Transactions)]
    end,
    Replicas = maps:groups_from_list(
        fun(Id) -> maps:get(replica, maps:get(Id, Transactions)) end, CollisionCandidates),
    case replica_conflicts(lists:sort(maps:to_list(maps:remove(undefined, Replicas))),
                           Transactions) of
        ok -> {ok, stream_forks(Transactions, Order)};
        Conflict -> {error, Conflict}
    end.

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

incomplete_ancestry(Id, Transactions) ->
    incomplete_ancestry([Id], Transactions, #{}).

incomplete_ancestry([], _, _) -> false;
incomplete_ancestry([Id | Rest], Transactions, Visited) ->
    case {maps:is_key(Id, Visited), maps:find(Id, Transactions)} of
        {false, error} -> true;
        {false, {ok, #{parents := Parents}}} ->
            incomplete_ancestry(Parents ++ Rest, Transactions, Visited#{Id => true});
        {true, _} -> incomplete_ancestry(Rest, Transactions, Visited)
    end.

event_prefix_token(Count, Order, Transactions) when Count >= 0 ->
    prefix(Count, Order, Transactions, crypto:hash_init(sha256)).

prefix(0, _, _, Hash) ->
    {ok, binary:encode_hex(crypto:hash_final(Hash), lowercase)};
prefix(_, [], _, _) -> {error, invalid_event_prefix};
prefix(Count, [Id | Rest], Transactions, Hash) ->
    #{events := Events} = maps:get(Id, Transactions),
    Taken = min(Count, length(Events)),
    case Taken of
        0 -> prefix(Count, Rest, Transactions, Hash);
        _ ->
            Next = crypto:hash_update(Hash, [Id, <<Taken:64/unsigned>>]),
            prefix(Count - Taken, Rest, Transactions, Next)
    end.

stream_forks(Transactions, Order) ->
    Groups = maps:fold(fun(Id, Record, Acc) ->
        #{bases := Bases, events := Events, resolution := Resolution} = Record,
        Written = maps:from_keys([Stream || {event, Stream, _, _} <- Events], true),
        OrdinaryWrites = case Resolution of
            none -> Written;
            {{ResolvedStream, _}, _} -> maps:remove(ResolvedStream, Written)
        end,
        Claims = [Key || {Stream, _} = Key <- Bases, maps:is_key(Stream, OrdinaryWrites)],
        WithWrites = lists:foldl(fun(Key, Groups) ->
            add_claim(Key, {Id, []}, Groups)
        end, Acc, Claims),
        case Resolution of
            none -> WithWrites;
            {Key, Heads} -> add_claim(Key, {Id, Heads}, WithWrites)
        end
    end, #{}, Transactions),
    lists:filtermap(fun
        ({_, [_]}) -> false;
        ({{Stream, Version}, Claims}) ->
            case branches(Stream, Claims, Transactions, Order) of
                [_, _ | _] = Branches ->
                    {true, {{fork, Stream, Version, [Id || {Id, _} <- Branches]}, Branches}};
                _ -> false
            end
    end, lists:sort(maps:to_list(Groups))).

add_claim(Key, Claim, Groups) ->
    maps:update_with(Key, fun(Claims) -> [Claim | Claims] end, [Claim], Groups).

branches(Stream, Claims, Transactions, Order) ->
    ClaimMap = maps:from_list(Claims),
    Owners = lists:foldl(fun(Id, Acc) ->
        case branch_owner(Id, Stream, ClaimMap, Transactions) of
            none -> Acc;
            Owner -> Acc#{Id => Owner}
        end
    end, #{}, Order),
    ByOwner = maps:groups_from_list(fun(Id) -> maps:get(Id, Owners) end, maps:keys(Owners)),
    ResolvedBy = lists:append(maps:values(ClaimMap)),
    Heads = lists:sort([Id || Members <- maps:values(ByOwner), Id <- Members,
        not lists:any(fun(Other) -> ancestor(Id, Other, Transactions) end, Members),
        not lists:any(fun(Head) -> covers(Id, Head, Transactions) end, ResolvedBy)]),
    case Heads of
        [_, _ | _] ->
            [{Id, branch_history(Id, Stream, ClaimMap, Owners, ByOwner, Transactions, Order)}
             || Id <- Heads];
        _ -> []
    end.

branch_owner(Id, Stream, Claims, Transactions) ->
    case maps:is_key(Id, Claims) of
        true -> Id;
        false ->
            case branch_events(Stream, Id, Transactions) of
                [] -> none;
                [_ | _] ->
                    case branch_owners(Id, Claims, Transactions) of
                        [Owner] -> Owner;
                        _ -> none
                    end
            end
    end.

branch_history(Head, Stream, Claims, Owners, ByOwner, Transactions, Order) ->
    Evidence = branch_evidence([Head], Claims, Owners, ByOwner, Transactions, #{}),
    lists:append([branch_events(Stream, Id, Transactions)
                  || Id <- Order, maps:is_key(Id, Evidence)]).

branch_evidence([], _, _, _, _, Evidence) -> Evidence;
branch_evidence([Head | Rest], Claims, Owners, ByOwner, Transactions, Evidence) ->
    case {maps:is_key(Head, Evidence), maps:find(Head, Owners)} of
        {false, {ok, Owner}} ->
            Members = [Id || Id <- maps:get(Owner, ByOwner), covers(Id, Head, Transactions)],
            Known = maps:merge(Evidence, maps:from_keys(Members, true)),
            % A resolution carries only the explicitly accepted branch evidence.
            Accepted = maps:get(Owner, Claims),
            branch_evidence(Accepted ++ Rest, Claims, Owners, ByOwner, Transactions, Known);
        _ -> branch_evidence(Rest, Claims, Owners, ByOwner, Transactions, Evidence)
    end.

% A resolution supersedes its accepted branches only for descendants that
% actually observed it. Imported extensions can therefore reopen the fork.
branch_owners(Id, Claims, Transactions) ->
    Ancestors = [{Claim, Heads} || Claim := Heads <- Claims, ancestor(Claim, Id, Transactions)],
    Accepted = lists:append([Heads || {_, Heads} <- Ancestors]),
    [Claim || {Claim, _} <- Ancestors,
              not lists:any(fun(Head) -> covers(Claim, Head, Transactions) end, Accepted)].

covers(Id, Id, _) -> true;
covers(Ancestor, Id, Transactions) -> ancestor(Ancestor, Id, Transactions).

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
