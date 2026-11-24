-module(eventcore_memory_test_ffi).
-export([stops_with_owner/1]).

stops_with_owner(Start) ->
    Parent = self(),
    Owner = spawn(fun() ->
        {ok, {memory, Server}} = Start(),
        Parent ! {server, Server},
        receive stop -> ok end
    end),
    try
        receive
            {server, Server} ->
                Ref = monitor(process, Server),
                Owner ! stop,
                try
                    receive
                        {'DOWN', Ref, process, Server, normal} -> true
                    after 1000 -> false
                    end
                after
                    demonitor(Ref, [flush])
                end
        after 1000 -> false
        end
    after
        Owner ! stop
    end.
