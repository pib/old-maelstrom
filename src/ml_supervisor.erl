%%%-------------------------------------------------------------------------
%%% @author    Parnell Springmeyer <parnell@whooshtraffic.com>
%%% @copyright 2012 Whoosh Traffic
%%% @doc       Primary supervisor for the maelstrom pool server and worker
%%%            supervisor.
%%% @end
%%%-------------------------------------------------------------------------

-module(ml_supervisor).
-behavior(supervisor).
-compile([{parse_transform, lager_transform}]).

-export([start_link/1]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link(Limit) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, [Limit]).

init([Limit]) ->
    PoolSpec = {ml_server,
        {ml_server, start_link, [Limit]}, permanent, 2000, worker, [ml_server]},
    
    WorkerSupSpec = {ml_worker_supervisor,
        {ml_worker_supervisor, start_link, [Limit]}, permanent, 2000, supervisor, [ml_worker_supervisor]},
    
    {ok, {{one_for_one, 5, 10}, [PoolSpec, WorkerSupSpec]}}.
