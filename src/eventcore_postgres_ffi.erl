-module(eventcore_postgres_ffi).
-include_lib("kernel/include/logger.hrl").
-export([trace_read/2, trace_append/2, stop_pool/1]).

trace_read(Stream, 0) ->
    ?LOG_INFO(#{operation => <<"postgres.read_stream">>, stream => Stream}),
    nil;
trace_read(Stream, After) ->
    ?LOG_INFO(#{operation => <<"postgres.read_stream_after">>, stream => Stream,
                after_version => After}),
    nil.

trace_append(StreamCount, EventCount) ->
    ?LOG_INFO(#{operation => <<"postgres.append_events">>, stream_count => StreamCount,
                event_count => EventCount}),
    nil.

stop_pool(Pid) ->
    try gen_server:stop(Pid)
    catch exit:noproc -> ok
    end,
    nil.
