%% -*- mode: Erlang; fill-column: 75; comment-column: 50; -*-
{application, maelstrom,
 [{description, "An abstract process pool gen_server"},
  {vsn,         "1.0"},
  {modules,     [maelstrom,
                 ml_app,
                 ml_server,
                 ml_supervisor,
                 ml_worker,
                 ml_worker_supervisor
                 ]},
  {registered, [ml_supervisor]},
  {applications, [kernel, stdlib]},
  {env, [{limit, 10}]},
  {mod, {ml_app, []}}
 ]}.
