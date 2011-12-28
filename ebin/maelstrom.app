%% -*- mode: Erlang; fill-column: 75; comment-column: 50; -*-
{application, maelstrom,
 [{description, "An abstract process pool gen_server"},
  {vsn,         "0.1"},
  {modules,     [ml_app,
                 ml_sup,
                 ml_server]},
  {registered, [ml_sup]},
  {applications, [kernel, stdlib]},
  {mod, {ml_app, [10]}}
 ]}.
