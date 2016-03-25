%%%-------------------------------------------------------------------
%%% @author Andrew Noskov
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Март 2016 03:25
%%%-------------------------------------------------------------------
-module(mapreduce).
-author("Andrew Noskov").

%% API
-export([mapreduce/3]).

mapreduce(Input, Map, Reduce) ->
  Client = self(),
  Pid = spawn(fun() -> master(Client, Map, Reduce, Input) end),
  receive
    {Pid, Result} -> Result
  end.

master(Parent, Map, Reduce, Input) ->
  process_flag(trap_exit, true),
  MasterPid = self(),
  spawn_workers(MasterPid, Map, Input),
  M = length(Input),
  Intermediate = collect_replies(M, dict:new()),
  spawn_workers(MasterPid, Reduce, dict:to_list(Intermediate)),
  R = dict:size(Intermediate),
  Output = collect_replies(R, dict:new()),
  Parent ! {self(), Output}.

spawn_workers(MasterPid, Fun, Pairs) ->
  lists:foreach(fun({K,V}) ->
    spawn_link(fun() -> worker(MasterPid, Fun, {K,V}) end)
  end, Pairs).

% Sends messages to master
worker(MasterPid, Fun, {K,V}) ->
  Fun(K, V, fun(K2,V2) -> MasterPid ! {K2,V2} end).

collect_replies(0, Dict) -> Dict;
collect_replies(N, Dict) ->
  receive
    {K, V} ->
      collect_replies(N, dict:append(K, V, Dict));
    {'EXIT', _Who, _Why} ->
      collect_replies(N-1, Dict)
  end.