%%%-------------------------------------------------------------------------
%%% @author    Parnell Springmeyer <parnell@whooshtraffic.com>
%%% @copyright 2012 Whoosh Traffic
%%% @doc       Maelstrom gen_server server - handles primary interactions
%%%            with a started maelstrom instance, like checking in/out
%%%            workers, their statuses, etc...
%%% @end
%%%-------------------------------------------------------------------------

-module(ml_server).
-behaviour(gen_server).
-compile([{parse_transform, lager_transform}]).

%% API
-export([start_link/1, limit/0, unused/0, status/0, queue/0, enqueue/1, dequeue/1, checkout/0, checkin/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {limit=undefined, unused=queue:new(), checkouts=queue:new()}).

%% ----------------------------------------------------------------------------
%% API
%% ----------------------------------------------------------------------------

%%----------------------------------------------------------------------
%% @doc  Starts the server.
%% 
%% @spec start_link(Limit::integer()) -> {ok, Pid}
%% where
%%  Pid = pid()
%% @end
%%----------------------------------------------------------------------
start_link(Limit) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Limit], []).

%%----------------------------------------------------------------------
%% @doc  Adds a worker that has been started to the accessible pool of
%%       of workers that can be checked in/out.
%% 
%% @spec enqueue(Id::worker_id()) -> ok
%%
%% @end
%%----------------------------------------------------------------------
enqueue(Id) ->
    gen_server:cast(?MODULE, {enqueue, Id}).

%%----------------------------------------------------------------------
%% @doc  Removes a worker that has been started from the accessible
%%       pool of workers.
%% 
%% @spec dequeue(Id::worker_id()) -> ok
%%
%% @end
%%----------------------------------------------------------------------
dequeue(Id) ->
    gen_server:cast(?MODULE, {dequeue, Id}).    

%%----------------------------------------------------------------------
%% @doc  Returns a list of the queue of waiting processes that
%%       attempted to checkout a worker but there were none available.
%% 
%% @spec queue() -> WaitList
%% where
%%  WaitList = list()
%% @end
%%----------------------------------------------------------------------
queue() ->
    gen_server:call(?MODULE, queue).

%%----------------------------------------------------------------------
%% @doc  Returns the current state of the maelstrom worker pool.
%% 
%% @spec status() -> State
%% where
%%  State = #state{}
%% @end
%%----------------------------------------------------------------------
status() ->
    gen_server:call(?MODULE, state).

%%----------------------------------------------------------------------
%% @doc  Returns the configured worker pool limit.
%% 
%% @spec limit() -> Int
%% where
%%  Int = integer()
%% @end
%%----------------------------------------------------------------------
limit() ->
    gen_server:call(?MODULE, limit).

%%----------------------------------------------------------------------
%% @doc  Returns a list of accessible workers that have not been
%%       checked out yet.
%% 
%% @spec queue() -> WorkerList
%% where
%%  WorkerList = list()
%% @end
%%----------------------------------------------------------------------
unused() ->
    gen_server:call(?MODULE, unused).

%%----------------------------------------------------------------------
%% @doc  Check a worker back into the pool.
%% 
%% @spec checkin(Id::worker_id()) -> ok
%%
%% @end
%%----------------------------------------------------------------------
checkin(Id) ->
    gen_server:cast(?MODULE, {checkin, Id}).

%%----------------------------------------------------------------------
%% @doc  Check a worker out of the pool. It has a thirty second timeout.
%% 
%% @spec checkout() -> WorkerId
%% where
%%  WorkerId = worker_id()
%% @end
%%----------------------------------------------------------------------
checkout() ->
    gen_server:call(?MODULE, checkout, 30000).

%%%=====================================================================
%%% Private API
%%%=====================================================================
%% ---------------------------------------------------------------------
%% Callback handlers
%% ---------------------------------------------------------------------
init([Limit]) ->
    {ok, #state{limit=Limit}}.

handle_call(state, _From, State) ->
    {reply, State, State};

handle_call(limit, _From, State) ->
    {reply, State#state.limit, State};

handle_call(unused, _From, State) ->
    {reply, length(queue:to_list(State#state.unused)), State};

handle_call(queue, _From, State) ->
    {reply, queue:to_list(State#state.checkouts), State};

handle_call(checkout, From, State) ->
    case queue:out(State#state.unused) of
        {{value, Worker}, Unused} ->
            {reply, Worker, State#state{unused=Unused}};
        {empty, _Unused} ->
            Checkouts = queue:in(From, State#state.checkouts),
            {reply, none, State#state{checkouts=Checkouts}}
    end.

handle_cast({checkin, Worker}, State) ->
    case queue:out(State#state.checkouts) of
        {{value, P}, Checkouts} ->
            gen_server:reply(P, Worker),
            {noreply, State#state{checkouts=Checkouts}};
        {empty, _Checkouts} ->
            Unused = queue:in(Worker, State#state.unused),
            {noreply, State#state{unused=Unused}}
        end;
handle_cast({enqueue, Worker}, State) ->
    erlang:monitor(process, whereis(Worker)),
    case queue:out(State#state.checkouts) of
        {{value, P}, Checkouts} ->
            gen_server:reply(P, Worker),
            {reply, State#state{checkouts=Checkouts}};
        {empty, _Checkouts} ->
            Unused = queue:in(Worker, State#state.unused),
            {noreply, State#state{unused=Unused}}
    end;
handle_cast({dequeue, Worker}, State) ->
    Unused = queue:from_list(lists:delete(Worker, queue:to_list(State#state.unused))),
    {noreply, State#state{unused=Unused}}.

handle_info({'DOWN', _, _, Worker, _}, State) ->
    Unused = queue:from_list(lists:delete(Worker, queue:to_list(State#state.unused))),
    {noreply, State#state{unused=Unused}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
