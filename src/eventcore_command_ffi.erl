-module(eventcore_command_ffi).
-export([sleep/1, trace/1]).

sleep(0) -> nil;
sleep(Milliseconds) ->
    Chunk = min(Milliseconds, 16#ffffffff),
    receive after Chunk -> sleep(Milliseconds - Chunk) end.

trace(execution_started) ->
    log(debug, #{event => execution_started});
trace({command_succeeded, Attempts}) ->
    log(info, #{event => command_succeeded, attempts => Attempts});
trace({retrying, {retry_context, Streams, Attempt, Delay}}) ->
    log(warning, #{event => retrying, streams => names(Streams),
                   attempt => Attempt, delay_ms => Delay});
trace({retries_exhausted, Streams, MaxRetries}) ->
    log(error, #{event => retries_exhausted, streams => names(Streams),
                 max_retries => MaxRetries});
trace(command_failed) ->
    log(debug, #{event => command_failed}).

log(Level, Report) ->
    logger:log(Level, Report#{component => eventcore_command}),
    nil.

names(Streams) -> [Name || {id, Name} <- Streams].
