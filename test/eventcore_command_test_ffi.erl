-module(eventcore_command_test_ffi).
-export([notifications/0, record/2, drain/1]).

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
