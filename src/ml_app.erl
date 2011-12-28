-module(ml_app).
-behavior(application).
-compile([{parse_transform, lager_transform}]).
-export([start/2, stop/1]).

start(_Type, [Limit]) ->
    application:start(lager),
    case ml_supervisor:start_link(Limit) of
        {ok, Pid} ->
            {ok, Pid};
        Other ->
            {error, Other}
    end.

stop(_State) ->
    ok.
