-module(maelstrom).
-compile([{parse_transform, lager_transform}]).
-export([start/0, workers/0, map/2, stop/0]).

start() ->
    application:start(maelstrom).

workers() ->
    {{total, ml_server:limit()}, {unused, ml_server:unused()}}.

%% Map over a list only on the local node
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

%% Map over a list on multiple nodes
map(Fun, List, {Split, NodeSpec}) ->
    application:start(lager),
    
    %% Get a list of good nodes we can send work to
    {ok, Nodes} = check_nodes(NodeSpec),
    
    %% Hypothetical so I can visualize:
    map(Fun, List, {Split, Nodes}, length(List), length(Nodes), 0, [], []).

%% Assign workers on local host to make RPC calls to worker hosts
map(Fun, List, {Split, [Node|T]}, Length, Limit, Pos, Acc, NodAcc) when Limit > Pos ->
    
    %% Get a worker
    Worker = ml_server:checkout(),
    
    {Chunk, Rest} = lists:split(Split, List),
    
    ml_worker:work(Worker, self(), {fun(N,A,F)->rpc:call(N, maelstrom, map, [F, A]) end, [Node, Fun, Chunk]}),
    map(Fun, Rest, Length, Limit, Pos + 1, Acc, lists:append(NodAcc, [Node]));

%% If limit (length of nodespec) is equal to Pos (number of nodes assigned)
map(Fun, [H|T], {Split, [Node|T]}, Length, Limit, Pos, Acc, NodAcc) when Limit == Pos->
    receive
        {done, Payload} ->
            [N1, Rest] = NodAcc,
            map(Fun, [H|T], {Split, lists:append(T, [N1]}, Length, Limit, Pos - 1, [Payload|Acc], Rest)
    after
        20000 ->
            [N1, Rest] = NodAcc,
            map(Fun, [H|T], {Split, lists:append(T, [N1]), Length, Limit, Pos - 1, [none|Acc], Rest)
    end;

%% If passed list is empty, halt and wait for responses
map(Fun, [], Spec, Length, Limit, Pos, Acc, NodAcc) when length(Acc) < Length ->
    receive
        {done, Payload} ->
            map(Fun, [], Spec, Length, Limit, Pos, [Payload|Acc])
    after
        20000 ->
            map(Fun, [], Spec, Length, Limit, Pos, [none|Acc])
    end;

%% When completely done, return accumulated result
map(_Fun, [], Spec, Length, _Limit, _Pos, Acc) when Length == length(Acc) ->
    Acc.

%% Check the given nodes and return a list of those that responded
check_nodes(NodeList) ->
    check_nodes(NodeList, []).
check_nodes([H|T], Acc) ->
    case net:ping(H) of
        pong ->
            check_nodes(T, lists:append(Acc, [H]));
        pang ->
            check_nodes(T, Acc),
            lager:info("~p is not responding...", [H])
    end;
check_nodes([], Acc) ->
    {ok, Acc}.

stop()  ->
    application:stop(maelstrom).
