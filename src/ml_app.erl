%%%-------------------------------------------------------------------------
%%% @author    Parnell Springmeyer <parnell@whooshtraffic.com>
%%% @copyright 2012 Whoosh Traffic
%%% @doc       Interface for application:start/stop/takeover/&c...
%%% @end
%%%-------------------------------------------------------------------------

-module(ml_app).
-behavior(application).
-compile([{parse_transform, lager_transform}]).
-export([start/2, stop/1]).

start(_Type, _StartArgs) ->
    
    %% I would put this in the .app file, but for some reason it can't
    %% be started from there...?
    application:start(lager),
    Limit = get_env(limit, 10),
    case ml_supervisor:start_link(Limit) of
        {ok, Pid} ->
            {ok, Pid};
        Other ->
            {error, Other}
    end.

get_env(Key, Default) ->
    case application:get_env(maelstrom, Key) of
        undefined   -> Default;
        {ok, Value} -> Value
    end.

stop(_State) ->
    ok.
