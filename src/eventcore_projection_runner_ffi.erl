-module(eventcore_projection_runner_ffi).
-export([sleep/1, with_release/2, log/3]).

sleep(Delay) ->
    nil = eventcore_command_ffi:sleep(Delay),
    true.

with_release(Release, Work) ->
    try Work()
    after Release()
    end.

log(Projector, Event, Detail) ->
    logger:warning(#{component => eventcore_projection, projector => Projector,
                     event => Event, detail => Detail}),
    nil.
