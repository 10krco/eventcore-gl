-module(eventcore_projection_leases).
-export([request/2, acquire/3, release/3, down/2]).

request(Pid, Name) ->
    try gen_server:call(Pid, {acquire, Name}, infinity) of
        {ok, Token} ->
            {ok, fun() ->
                try gen_server:call(Pid, {release, Name, Token}, infinity)
                catch exit:_ -> nil
                end
            end};
        Error -> Error
    catch
        exit:_ -> {error, coordinator_unavailable}
    end.

acquire(_, Holder, Leases) when node(Holder) =/= node() ->
    {{error, remote_holder}, Leases};
acquire(Name, Holder, Leases) ->
    case live_lease(maps:find(Name, Leases)) of
        true -> {{error, already_leading}, Leases};
        false ->
            Token = erlang:monitor(process, Holder),
            {{ok, Token}, Leases#{Name => {Holder, Token}}}
    end.

release(Name, Token, Leases) ->
    case maps:find(Name, Leases) of
        {ok, {_, Token}} ->
            erlang:demonitor(Token, [flush]),
            maps:remove(Name, Leases);
        _ -> Leases
    end.

down(Dead, Leases) ->
    maps:filter(fun(_, {_, Token}) -> Token =/= Dead end, Leases).

live_lease(error) -> false;
% A live code upgrade can retain a remote holder from the previous version.
live_lease({ok, {Holder, _}}) when node(Holder) =/= node() -> true;
live_lease({ok, {Holder, Token}}) ->
    case erlang:is_process_alive(Holder) of
        true -> true;
        false ->
            erlang:demonitor(Token, [flush]),
            false
    end.
