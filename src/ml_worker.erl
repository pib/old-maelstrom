-module(ml_worker).
-compile([{parse_transform, lager_transform}]).
-behavior(gen_server).

%% API
-export([start_link/1, work/3, status/1, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-record(state, {id=undefined}).
-define(SERVER, ?MODULE).

%%%=====================================================================
%%% API
%%%=====================================================================

%%----------------------------------------------------------------------
%% @doc  Starts the server.
%% 
%% @spec start_link(Procs::integer()) -> {ok, Pid}
%% where
%%  Pid = pid()
%% @end
%%----------------------------------------------------------------------
start_link(Id) ->
    gen_server:start_link({local, Id}, ?MODULE, [Id], []).

%%----------------------------------------------------------------------
%% @doc  Work!
%% @spec work(WId, {Mod, Func, Args}) -> ok
%% @end
%%----------------------------------------------------------------------
work(WId, CPid, PeiceOfWork) ->
    gen_server:cast(WId, {work, CPid, PeiceOfWork}).

%%----------------------------------------------------------------------
%% @doc  Get the status.
%% @spec status() -> ok
%% @end
%%----------------------------------------------------------------------
status(WId) ->
    gen_server:call(WId, status).

%%----------------------------------------------------------------------
%% @doc  Stop the server.
%% @spec stop() -> ok
%% @end
%%----------------------------------------------------------------------
stop(WId) ->
    gen_server:cast(WId, stop).

%%%=====================================================================
%%% Private API
%%%=====================================================================
enqueue(Id) ->
    ml_server:enqueue(Id),
    ok.

%%%=====================================================================
%%% gen_server callbacks
%%%=====================================================================
init([Id]) ->
    lager:info("Starting up... ~p", [Id]),
    enqueue(Id),
    {ok, #state{id=Id}}.

%%----------------------------------------------------------------------
%% Handlers
%%----------------------------------------------------------------------
handle_call(status, _From, State) ->
    {reply, {ok, "Some sort of status response here"}, State}.

handle_cast({work, CPid, {F, A}}, State) ->
    %% Do the work
    Result = erlang:apply(F, A),
    
    %% Send finished work to the collector!
    CPid ! {done, Result},
    
    %% Checkin this worker
    ml_server:checkin(State#state.id),
    
    {noreply, State};

handle_cast(stop, State) ->
    ml_server:dequeue(State#state.id),
    {stop, normal, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
