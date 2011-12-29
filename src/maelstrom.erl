-module(maelstrom).
-export([start/0, workers/0, map/2, stop/0]).

start() ->
    application:start(maelstrom).

workers() ->
    {{total, ml_server:limit()}, {unused, ml_server:unused()}}.

map(Fun, List) ->
    {{total, Limit}, _} = workers(),
    map(Fun, List, length(List), Limit, 0, []).

map(Fun, [H|T], Length, Limit, Pos, Acc) when Limit > Pos ->
    
    %% Get a worker
    Worker = ml_server:checkout(),
    
    ml_worker:work(Worker, self(), {Fun, [H]}),
    map(Fun, T, Length, Limit, Pos + 1, Acc);
map(Fun, [H|T], Length, Limit, Pos, Acc) when Limit == Pos->
    receive
        {done, Payload} ->
            map(Fun, [H|T], Length, Limit, Pos - 1, [Payload|Acc])
    after
        20000 ->
            map(Fun, [H|T], Length, Limit, Pos - 1, [none|Acc])
    end;
map(Fun, [], Length, Limit, Pos, Acc) when length(Acc) < Length ->
    receive
        {done, Payload} ->
            map(Fun, [], Length, Limit, Pos, [Payload|Acc])
    after
        20000 ->
            map(Fun, [], Length, Limit, Pos, [none|Acc])
    end;
map(_Fun, [], Length, _Limit, _Pos, Acc) when Length == length(Acc) ->
    Acc.

stop()  ->
    application:stop(maelstrom).
