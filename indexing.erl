%%%-------------------------------------------------------------------
%%% @author Andrew Noskov
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Март 2016 04:20
%%%-------------------------------------------------------------------
-module(indexing).
-author("Andrew Noskov").
-import(mapreduce, [mapreduce/3]).

%% API
-export([list_numbered_files/1]).


list_numbered_files(DirName) ->
  {ok, Files} = file:list_dir(DirName),
  FullFiles = [ filename:join(DirName, File) || File <- Files ],
  Indices = lists:seq(1, length(Files)),
  lists:zip(Indices, FullFiles).