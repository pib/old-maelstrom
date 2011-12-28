-module(ml_server).
-behaviour(gen_server).
-compile([{parse_transform, lager_transform}]).

-export([start_link/1, limit/0, unused/0, status/0, queue/0, enqueue/1, dequeue/1, checkout/0, checkin/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {limit=undefined, unused=queue:new(), checkouts=queue:new()}).


%% ----------------------------------------------------------------------------
%% api
%% ----------------------------------------------------------------------------

%% @doc Start a linked pool manager.
start_link(Limit) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Limit], []).

%% @doc Called by ht_worker after the worker process has started.
enqueue(Id) ->
    gen_server:cast(?MODULE, {enqueue, Id}).

dequeue(Id) ->
    gen_server:cast(?MODULE, {dequeue, Id}).    

queue() ->
    gen_server:call(?MODULE, queue).

status() ->
    gen_server:call(?MODULE, state).

limit() ->
    gen_server:call(?MODULE, limit).

unused() ->
    gen_server:call(?MODULE, unused).

%% @doc Checkin a worker.
checkin(Id) ->
    gen_server:cast(?MODULE, {checkin, Id}).

%% @doc Checkout a worker.
checkout() ->
    gen_server:call(?MODULE, checkout).


%% ------------------------------------------------------------------
%% gen_server callbacks
%% ------------------------------------------------------------------

%% @private
init([Limit]) ->
    {ok, #state{limit=Limit}}.

handle_call(state, _From, State) ->
    {reply, State, State};

handle_call(limit, _From, State) ->
    {reply, State#state.limit, State};

handle_call(unused, _From, State) ->
    {reply, length(queue:to_list(State#state.unused)), State};

handle_call(queue, _From, State) ->
    {reply, queue:to_list(State#state.unused) ++ queue:to_list(State#state.checkouts), State};

handle_call(checkout, From, State) ->
    case queue:out(State#state.unused) of
        {{value, Worker}, Unused} ->
            {reply, Worker, State#state{unused=Unused}};
        {empty, _Unused} ->
            Checkouts = queue:in(From, State#state.checkouts),
            {noreply, State#state{checkouts=Checkouts}}
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
