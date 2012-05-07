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

-module(maelstrom).
-export([start/0, workers/0, map/2, stop/0]).

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
    end;
map(Fun, [], Length, Limit, Pos, Acc) when length(Acc) < Length ->
    receive
        {done, Payload} ->
            map(Fun, [], Length, Limit, Pos, [Payload|Acc])
    end;
map(_Fun, [], Length, _Limit, _Pos, Acc) when Length == length(Acc) ->
    Acc.
