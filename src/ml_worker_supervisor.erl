-module(ml_worker_supervisor).

-behaviour(supervisor).

%% api
-export([start_link/1]).

%% supervisor callbacks
-export([init/1]).


%% ----------------------------------------------------------------------------
%% api
%% ----------------------------------------------------------------------------

%% @doc Start linked hot tub worker supervisor.
start_link(Limit) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Limit]).

%% ----------------------------------------------------------------------------
%% supervisor callbacks
%% ----------------------------------------------------------------------------

%% @private
init([Limit]) ->
    ChildSpecs = lists:map(
        fun (Id) ->
            Str = lists:flatten("worker_" ++ io_lib:format("~p", [Id])),
            WId = list_to_atom(Str),
            {WId,
                {ml_worker, start_link, [WId]},
                transient, 2000, worker, [ml_worker]}
        end, lists:seq(0, Limit-1)),
    
    {ok, {{one_for_one, 20, 60}, ChildSpecs}}.
