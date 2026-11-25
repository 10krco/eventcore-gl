-module(eventcore_command_test_ffi).
-export([observe_retry/1, end_at_retry/1, notifications/0, record/2, drain/1]).

notifications() -> make_ref().

record(Ref, Value) ->
    self() ! {Ref, Value},
    nil.

drain(Ref) -> drain(Ref, []).

drain(Ref, Reversed) ->
    receive
        {Ref, Value} -> drain(Ref, [Value | Reversed])
    after 0 ->
        lists:reverse(Reversed)
    end.


observe_retry(Execute) ->
    try Execute() of _ -> none
    catch throw:{observed_retry, Context} -> {some, Context}
    end.

end_at_retry(Context) -> throw({observed_retry, Context}).
