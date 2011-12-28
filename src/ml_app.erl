-module(ml_app).
-behavior(application).
-compile([{parse_transform, lager_transform}]).
-export([start/2, stop/1]).

start(_Type, [Limit]) ->
    
    %% I would put this in the .app file, but for some reason it can't
    %% be started from there...?
    application:start(lager),
    case ml_supervisor:start_link(Limit) of
        {ok, Pid} ->
            {ok, Pid};
        Other ->
            {error, Other}
    end.

stop(_State) ->
    ok.
