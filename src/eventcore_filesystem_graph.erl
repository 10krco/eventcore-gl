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
    AcceptedByClaim = maps:map(fun(_, AcceptedHeads) ->
        ordsets:from_list([Claim || Claim := _ <- ClaimMap,
            lists:any(fun(Head) -> covers(Claim, Head, Transactions) end, AcceptedHeads)])
    end, ClaimMap),
    Owners = branch_ownership(Stream, ClaimMap, AcceptedByClaim, Transactions, Order),
    ByOwner = maps:groups_from_list(fun(Id) -> maps:get(Id, Owners) end, maps:keys(Owners)),
    AcceptedHeads = lists:append(maps:values(ClaimMap)),
    Covered = lists:foldl(fun(Id, Acc) -> mark_ancestry([Id], Transactions, Acc) end,
                          #{}, AcceptedHeads),
    Heads = branch_frontier(lists:reverse(Order), Owners, Covered, Transactions),
    case Heads of
        [_, _ | _] ->
            [{Id, branch_history(Id, Stream, ClaimMap, Owners, ByOwner, Transactions, Order)}
             || Id <- Heads];
        _ -> []
    end.

% Each transaction inherits the claims observed by its parents. Subtract the
% claims accepted by its observed decisions before assigning ordinary writes.
branch_ownership(Stream, Claims, AcceptedByClaim, Transactions, Order) ->
    {Owners, _} = lists:foldl(fun(Id, {Current, Observed}) ->
        #{parents := Parents} = maps:get(Id, Transactions),
        Inherited = lists:foldl(fun(Parent, Acc) ->
            ordsets:union(Acc, maps:get(Parent, Observed, []))
        end, [], Parents),
        case maps:is_key(Id, Claims) of
            true -> {Current#{Id => Id}, Observed#{Id => ordsets:add_element(Id, Inherited)}};
            false ->
                case branch_events(Stream, Id, Transactions) of
                    [] -> {Current, Observed#{Id => Inherited}};
                    [_ | _] ->
                        Accepted = ordsets:union([maps:get(Claim, AcceptedByClaim) || Claim <- Inherited]),
                        case ordsets:subtract(Inherited, Accepted) of
                            [Owner] -> {Current#{Id => Owner}, Observed#{Id => Inherited}};
                            _ -> {Current, Observed#{Id => Inherited}}
                        end
                end
        end
    end, {#{}, #{}}, Order),
    Owners.

% Walk newest first. For each owner, mark its heads' ancestors once so that a
% long resolved suffix does not require pairwise ancestry checks.
branch_frontier(Reversed, Owners, Covered, Transactions) ->
    {Heads, _} = lists:foldl(fun(Id, {Heads, ByOwner} = Acc) ->
        case maps:find(Id, Owners) of
            error -> Acc;
            {ok, Owner} ->
                Seen = maps:get(Owner, ByOwner, #{}),
                case maps:is_key(Id, Seen) of
                    true -> Acc;
                    false ->
                        Updated = ByOwner#{Owner => mark_ancestry([Id], Transactions, Seen)},
                        case maps:is_key(Id, Covered) of
                            true -> {Heads, Updated};
                            false -> {[Id | Heads], Updated}
                        end
                end
        end
    end, {[], #{}}, Reversed),
    lists:sort(Heads).

mark_ancestry([], _, Seen) -> Seen;
mark_ancestry([Id | Rest], Transactions, Seen) ->
    case {maps:is_key(Id, Seen), maps:find(Id, Transactions)} of
        {false, {ok, #{parents := Parents}}} ->
            mark_ancestry(Parents ++ Rest, Transactions, Seen#{Id => true});
        _ -> mark_ancestry(Rest, Transactions, Seen)
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
