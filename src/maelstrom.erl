%%%-------------------------------------------------------------------------
%%% @author    Parnell Springmeyer <parnell@whooshtraffic.com>
%%% @copyright 2012 Whoosh Traffic
%%% @doc       Convenience interface module to maelstrom - the most common
%%%            use case would be to map a function over a list. This module
%%%            provides a map/2 and map/3.
%%%            
%%%            map/2 is a drop-in replacement for stdlib map/2 and will map
%%%            a given fun over a list of elements using maelstrom's worker
%%%            pool. NOTE: the pool must be started first!
%%%
%%%            If a worker crashes it will be restarted - but any data
%%%            passed to it cannot be recovered.
%%%
%%%            map/3 is a distributed implementation. If you have nodes with
%%%            maelstrom installed and started this function will distribute
%%%            the workload to those nodes, collect the results, and return.
%%%
%%%            It gracefully handles nodes going down and in the case of a
%%%            node going down before a batch is sent to it, the data is
%%%            recovered and retried once another available node has finished
%%%            its work.
%%%
%%%            If a node is working and crashes there is no route for
%%%            for recovering the data passed to that node (much like
%%%            a crashed worker).
%%%            
%%% @end
%%%-------------------------------------------------------------------------
%%%-------------------------------------------------------------------------
%%% NOTE: maelstrom:start()/stop() can be used in lieu of
%%% application:start()/stop().
%%%
%%% @example
%%%
%%%     1> maelstrom:start().
%%%     18:32:20.988 [info] Application lager started on node nonode@nohost
%%%     18:32:21.000 [info] Starting up... worker_0
%%%     18:32:21.000 [info] Starting up... worker_1
%%%     18:32:21.000 [info] Starting up... worker_2
%%%     18:32:21.000 [info] Starting up... worker_3
%%%     18:32:21.001 [info] Starting up... worker_4
%%%     18:32:21.001 [info] Starting up... worker_5
%%%     18:32:21.001 [info] Starting up... worker_6
%%%     18:32:21.001 [info] Starting up... worker_7
%%%     18:32:21.001 [info] Starting up... worker_8
%%%     18:32:21.001 [info] Starting up... worker_9
%%%     18:32:21.002 [info] Application maelstrom started on node nonode@nohost
%%%     ok
%%%     2> maelstrom:map(fun(X) -> X*2 end, [1,2,3,4,5]).
%%%     [10,8,6,4,2]
%%%     3>
%%%-------------------------------------------------------------------------
%%%-------------------------------------------------------------------------
%%% NOTE: if you want a "fallback" node, the host node is a good option and
%%% recommended! In this example, "host@Acuitas" is the host machine making
%%% the actual call.
%%%
%%% @example
%%%
%%%     (host@Acuitas)1> maelstrom:start().
%%%     18:37:11.306 [info] Application lager started on node host@Acuitas
%%%     18:37:11.314 [info] Starting up... worker_0
%%%     18:37:11.314 [info] Starting up... worker_1
%%%     18:37:11.315 [info] Starting up... worker_2
%%%     18:37:11.315 [info] Starting up... worker_3
%%%     18:37:11.315 [info] Starting up... worker_4
%%%     18:37:11.315 [info] Starting up... worker_5
%%%     18:37:11.316 [info] Starting up... worker_6
%%%     18:37:11.316 [info] Starting up... worker_7
%%%     18:37:11.316 [info] Starting up... worker_8
%%%     18:37:11.316 [info] Starting up... worker_9
%%%     18:37:11.316 [info] Application maelstrom started on node host@Acuitas
%%%     ok
%%%     (host@Acuitas)2> maelstrom:map(fun(X) -> X*2 end, [1,2,3,4,5,6,7,8], {4, [worker@zatharus, host@Acuitas]}).
%%%     [8,6,4,2,16,14,12,10]
%%%     (host@Acuitas)3>

-module(maelstrom).
-compile([{parse_transform, lager_transform}]).
-export([start/0, workers/0, map/2, map/3, check_nodes/1, stop/0]).

%%----------------------------------------------------------------------
%% @doc  Starts the maelstrom worker pool server.
%% 
%% @spec start() -> ok
%%
%% @end
%%----------------------------------------------------------------------
start() ->
    application:start(maelstrom).

%%----------------------------------------------------------------------
%% @doc  Returns the worker pool limit and the number of unused workers.
%% 
%% @spec workers() -> {{total, Integer}, {unused, Integer}}
%% where
%%  Integer = integer()
%% @end
%%----------------------------------------------------------------------
workers() ->
    {{total, ml_server:limit()}, {unused, ml_server:unused()}}.

%%----------------------------------------------------------------------
%% @doc  Map over a list applying a given fun and returning the
%%       collected results.
%% 
%% @spec map(Fun::fun(), List::list()) -> Result
%% where
%%  Result = list()
%% @end
%%----------------------------------------------------------------------
map(Fun, List) ->
    {{total, Limit}, _} = workers(),
    map(Fun, List, length(List), Limit, 0, []).

%% Private map API
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

%%----------------------------------------------------------------------
%% @doc  Map over a list splitting the list by Split::integer() and
%%       rpc:call()ing maelstrom:map/2 on a remote node given by
%%       NodeSpec::list() along with Fun::fun().
%%
%%       This function will intelligently handle crashing nodes, if a
%%       node goes down before rpc:call is called, the data will be
%%       recovered. If a node crashes while the work is being done no
%%       data will be recovered.
%%
%%       NodeSpec::list() should be a list of nodes:
%%       [node1|...]. map/3 will divide the work to be done amongst
%%       the nodes and redelegate to a node after it returns with
%%       results from its workload.
%% 
%% @spec map(Fun::fun(), List::list(), {Split::integer(), NodeSpec::list()}) -> Result
%% where
%%  Result = list()
%% @end
%%----------------------------------------------------------------------
map(Fun, List, {Split, NodeSpec}) when length(List) >= Split ->
    application:start(lager),
    
    %% Get a list of good nodes we can send work to
    {ok, Nodes} = check_nodes(NodeSpec),
    
    %% Hypothetical so I can visualize:
    map(Fun, List, {Split, Nodes}, length(List), length(Nodes), 0, [], []).

%% Assign workers on local host to make RPC calls to worker hosts
map(Fun, List, {Split, [Node|T]}, Length, Limit, Pos, Acc, NodAcc) when Limit > Pos ->
    
    %% Get a worker
    Worker = ml_server:checkout(),
    
    {Chunk, Rest} = case length(List) > 1 of
        true ->
            lists:split(Split, List);
        _Else ->
            [H|_T] = List,
            {[H], []}
    end,
    
    ml_worker:work(Worker, self(), {fun(N,F,A,P)-> rpc(N,F,A,P) end, [Node, Fun, Chunk, self()]}),
    map(Fun, Rest, {Split, T}, Length, Limit, Pos + 1, Acc, lists:append(NodAcc, [Node]));

%% If limit (length of nodespec) is equal to Pos (number of nodes assigned)
map(Fun, [H|T], {Split, Nodes}, Length, Limit, Pos, Acc, NodAcc) when Limit == Pos->
    receive
        {done, Payload} ->
            [N1|Rest] = NodAcc,
            map(Fun, [H|T], {Split, lists:append(Nodes, [N1])}, Length, Limit, Pos - 1, Payload++Acc, Rest);
        
        %% This is coming from the rpc/4 function so we can reclaim data
        {rpcnodedown, Node, Data} ->
            %% Put the data (it should be a list) back into the list
            Reunited = lists:append(T, Data),
            lager:info("Node ~p crashed", [Node]),
            
            %% Don't persist the dead node!
            D = lists:delete(Node, NodAcc),
            
            %% Pos must not be decremented because we still may need to receive a result from the other node!
            map(Fun, [H|Reunited], {Split, Nodes}, Length, Limit, Pos, Acc, D)
    after
        20000 ->
            [N1|Rest] = NodAcc,
            map(Fun, [H|T], {Split, lists:append(Nodes, [N1])}, Length, Limit, Pos - 1, [none|Acc], Rest)
    end;

%% If passed list is empty, halt and wait for responses
map(Fun, [], Spec, Length, Limit, Pos, Acc, NodAcc) when length(Acc) < Length ->
    receive
        {done, Payload} ->
            map(Fun, [], Spec, Length, Limit, Pos, Payload++Acc, NodAcc);
        
        %% This is coming from the rpc/4 function so we can reclaim data
        {rpcnodedown, Node, Data} ->
            lager:info("Node ~p crashed", [Node]),
            
            %% Don't persist the dead node!
            D = lists:delete(Node, NodAcc),
            
            %% Pos must not be decremented because we still may need to receive a result from the other node!
            map(Fun, Data, Spec, Length, Limit, Pos, Acc, D)
    after
        20000 ->
            map(Fun, [], Spec, Length, Limit, Pos, [none|Acc], NodAcc)
    end;

%% When completely done, return accumulated result
map(_Fun, [], _Spec, Length, _Limit, _Pos, Acc, _NodAcc) when Length == length(Acc) ->
    Acc.

%% Make RPC call - if node is down when call is made, exit so we intentionally crash the worker
rpc(Node, Fun, Args, Parent) ->
    Res = rpc:call(Node, maelstrom, map, [Fun, Args]),
    case Res of
        {badrpc, Reason} ->
            %% Crash the worker intentionally, work will be lost
            Parent ! {rpcnodedown, Node, Args},
            exit(Reason);
        _Else ->
            Res
    end.

%% Check the given nodes and return a list of those that responded
check_nodes(NodeList) ->
    check_nodes(NodeList, []).
check_nodes([H|T], Acc) ->
    case net_adm:ping(H) of
        pong ->
            
            check_nodes(T, lists:append(Acc, [H]));
        pang ->
            lager:info("~p is not responding...", [H]),
            check_nodes(T, Acc)
    end;
check_nodes([], Acc) ->
    {ok, Acc}.

stop()  ->
    application:stop(maelstrom).
