%%%-------------------------------------------------------------------
%%% @author Andrew Noskov
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. Март 2016 23:17
%%%-------------------------------------------------------------------
-module(frequency).
-author("Andrew Noskov").

%% API
-export([freq/1]).

-import(mapreduce, [mapreduce/3]).

freq(DirName) ->
  NumberedFiles = indexing:list_numbered_files(DirName),
  mapreduce(NumberedFiles, fun enum_words/3, fun add_words/3).

enum_words(_Index, FileName, Emit) ->
  {ok, [Words]} = file:consult(FileName),
  lists:foreach(fun (Word) -> Emit(Word, 1) end, Words).

add_words(Word, Counts, Emit) ->
  Total = lists:foldl(fun (Count, Partial) -> Count + Partial end, 0, Counts),
  Emit(Word, Total).