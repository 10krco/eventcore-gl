-module(eventcore_testing_faults_ffi).
-export([counter/0, random_seed/0, sample/3]).

counter() ->
    Counter = atomics:new(1, [{signed, false}]),
    fun() -> atomics:add_get(Counter, 1, 1) end.

random_seed() ->
    binary:decode_unsigned(crypto:strong_rand_bytes(8)).

sample(Seed, Sequence, Operation) ->
    Range = 1 bsl 32,
    erlang:phash2({Seed, Sequence, Operation}, Range) / Range.
