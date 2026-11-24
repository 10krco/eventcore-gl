-module(eventcore_projection_ffi).
-export([compile_glob/1, matches_glob/2]).

compile_glob(Pattern) ->
    try
        {ok, tokens(unicode:characters_to_list(Pattern), true, [])}
    catch
        throw:invalid_glob -> {error, nil}
    end.

tokens([], _, Acc) -> Acc;
tokens([$*, $*, $/ | Rest], true, Acc) ->
    tokens(Rest, true, [recursive | Acc]);
tokens([$*, $*], true, Acc) ->
    tokens([], false, [recursive_tail | Acc]);
tokens([$*, $* | _], _, _) -> throw(invalid_glob);
tokens([$* | Rest], _, Acc) -> tokens(Rest, false, [star | Acc]);
tokens([$? | Rest], _, Acc) -> tokens(Rest, false, [one | Acc]);
tokens([$[, $! | Rest], _, Acc) -> character_class(Rest, true, Acc);
tokens([$[ | Rest], _, Acc) -> character_class(Rest, false, Acc);
tokens([Char | Rest], _, Acc) ->
    tokens(Rest, Char =:= $/, [{literal, Char} | Acc]).

character_class([First | Rest], Negated, Acc) ->
    {Chars, After} = lists:splitwith(fun(Char) -> Char =/= $] end, Rest),
    case After of
        [$] | Tail] ->
            tokens(Tail, false, [{class, Negated, ranges([First | Chars])} | Acc]);
        [] -> throw(invalid_glob)
    end;
character_class([], _, _) -> throw(invalid_glob).

ranges([First, $-, Last | Rest]) -> [{First, Last} | ranges(Rest)];
ranges([Char | Rest]) -> [{Char, Char} | ranges(Rest)];
ranges([]) -> [].

matches_glob(ReverseTokens, Stream) ->
    Chars = lists:reverse(unicode:characters_to_list(Stream)),
    Initial = [true | lists:duplicate(length(Chars), false)],
    States = lists:foldl(fun(Token, Suffix) ->
        match_positions(Token, Chars, Suffix)
    end, Initial, ReverseTokens),
    lists:last(States).

%% Each row describes where a pattern suffix matches. Walking the stream
%% backwards lets a wildcard carry its result across characters in one pass.
match_positions(recursive_tail, Chars, _) ->
    lists:duplicate(length(Chars) + 1, true);
match_positions(star, Chars, [Empty | Rest]) ->
    [Empty | star_positions(Chars, Rest, Empty)];
match_positions(recursive, Chars, [Empty | Rest]) ->
    [Empty | recursive_positions(Chars, Rest, Empty, false)];
match_positions(Token, Chars, Suffix) ->
    [false | lists:zipwith(fun(Char, Tail) ->
        Tail andalso matches_character(Token, Char)
    end, Chars, lists:droplast(Suffix))].

star_positions([], [], _) -> [];
star_positions([_ | Chars], [Zero | Rest], Previous) ->
    Match = Zero orelse Previous,
    [Match | star_positions(Chars, Rest, Match)].

recursive_positions([], [], _, _) -> [];
recursive_positions([Char | Chars], [Zero | Rest], Previous, Carry) ->
    NextCarry = Carry orelse (Char =:= $/ andalso Previous),
    [Zero orelse NextCarry | recursive_positions(Chars, Rest, Zero, NextCarry)].

matches_character(one, _) -> true;
matches_character({literal, Expected}, Char) -> Expected =:= Char;
matches_character({class, Negated, Ranges}, Char) ->
    Negated =/= lists:any(fun({Low, High}) -> Low =< Char andalso Char =< High end, Ranges).
