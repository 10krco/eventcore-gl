-module(eventcore_filesystem_ingestion).
-export([open/3, add/3, flush/2, fetch/3, needs_flush/1]).

-record(ingestion, {
    directory, frontier = 0, events = #{}, pending = []
}).

%% Segments contain immutable transaction IDs and their allocated event counts.
%% A missing transaction retains its position range even when unreadable.
open(Directory, Ordered, Transactions) ->
    case file:list_dir(Directory) of
        {ok, Names} ->
            Segments = lists:sort([Name || Name <- Names, filename:extension(Name) =:= ".ingestion"]),
            case load_segments(Segments, #ingestion{directory = Directory}, #{}, Transactions) of
                {ok, Index, Seen} -> {ok, add_unseen(Ordered, Transactions, Seen, Index)};
                Error -> Error
            end;
        {error, enoent} ->
            {ok, add_unseen(Ordered, Transactions, #{}, #ingestion{directory = Directory})};
        Error -> Error
    end.

add_unseen(Ordered, Transactions, Seen, Index) ->
    lists:foldl(fun(Id, Acc) ->
        case maps:is_key(Id, Seen) of
            true -> Acc;
            false -> add(Id, maps:get(events, maps:get(Id, Transactions)), Acc)
        end
    end, Index, Ordered).

add(_, [], Index) -> Index;
add(Id, Events, #ingestion{pending = Pending} = Index) ->
    Index#ingestion{pending = [{Id, Events} | Pending]}.

needs_flush(#ingestion{pending = Pending}) -> Pending =/= [].

flush(_Publish, #ingestion{frontier = High, pending = []} = Index) ->
    {ok, High, Index};
flush(Publish, #ingestion{directory = Directory, frontier = High, pending = Pending} = Index) ->
    Entries = lists:reverse(Pending),
    Data = iolist_to_binary([<<Id:64/binary, (length(Events)):64>> || {Id, Events} <- Entries]),
    Digest = crypto:hash(sha256, Data),
    Path = filename:join(Directory, segment_name(High + 1)),
    case Publish(Directory, Path, <<Digest/binary, Data/binary>>) of
        {ok, nil} ->
            Updated = lists:foldl(fun({_, Events}, Acc) ->
                index_events(Events, length(Events), Acc)
            end, Index#ingestion{pending = []}, Entries),
            {ok, Updated#ingestion.frontier, Updated};
        Error -> Error
    end.

fetch(First, Last, #ingestion{events = Events}) ->
    fetch(First, Last, Events, []).

fetch(Position, Last, _, Acc) when Position > Last -> {ok, lists:reverse(Acc)};
fetch(Position, Last, Events, Acc) ->
    case maps:find(Position, Events) of
        {ok, Event} -> fetch(Position + 1, Last, Events, [{Position, Event} | Acc]);
        error -> {error, nil}
    end.

segment_name(Start) ->
    iolist_to_binary(io_lib:format("~20..0B.ingestion", [Start])).

load_segments([], Index, Seen, _) -> {ok, Index, Seen};
load_segments([Name | Rest], #ingestion{directory = Directory, frontier = High} = Index,
              Seen, Transactions) ->
    case unicode:characters_to_binary(Name) =:= segment_name(High + 1) of
        false -> {error, invalid_ingestion_order};
        true ->
            case file:read_file(filename:join(Directory, Name)) of
                {ok, <<Digest:32/binary, Data/binary>>} when byte_size(Data) >= 72 ->
                    case crypto:hash(sha256, Data) of
                        Digest ->
                            case load_entries(Data, Index, Seen, Transactions) of
                                {ok, Updated, Visited} -> load_segments(Rest, Updated, Visited, Transactions);
                                Error -> Error
                            end;
                        _ -> {error, bad_ingestion_checksum}
                    end;
                _ -> {error, invalid_ingestion_segment}
            end
    end.

load_entries(<<>>, Index, Seen, _) -> {ok, Index, Seen};
load_entries(<<Id:64/binary, Count:64, Rest/binary>>, Index, Seen, Transactions)
  when Count > 0, not is_map_key(Id, Seen) ->
    case maps:find(Id, Transactions) of
        {ok, #{events := Events}} when length(Events) =:= Count ->
            load_entries(Rest, index_events(Events, Count, Index), Seen#{Id => true}, Transactions);
        error ->
            load_entries(Rest, index_events([], Count, Index), Seen#{Id => true}, Transactions);
        _ -> {error, invalid_ingestion_count}
    end;
load_entries(_, _, _, _) -> {error, invalid_ingestion_entry}.

index_events(Events, Count, #ingestion{frontier = High, events = ByPosition} = Index) ->
    {_, Updated} = lists:foldl(fun(Event, {Position, Acc}) ->
        {Position + 1, Acc#{Position => Event}}
    end, {High + 1, ByPosition}, Events),
    Index#ingestion{frontier = High + Count, events = Updated}.
