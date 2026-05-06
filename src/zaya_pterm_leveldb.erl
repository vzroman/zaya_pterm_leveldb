-module(zaya_pterm_leveldb).

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  create/1,
  open/1,
  close/1,
  remove/1
]).

%%=================================================================
%%	LOW_LEVEL API
%%=================================================================
-export([
  read/2,
  write/2,
  delete/2
]).

%%=================================================================
%%	ITERATOR API
%%=================================================================
-export([
  first/1,
  last/1,
  next/2,
  prev/2
]).

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
-export([
  find/2,
  foldl/4,
  foldr/4
]).

%%=================================================================
%%	COPY API
%%=================================================================
-export([
  copy/3,
  dump_batch/2
]).

%%=================================================================
%%	TRANSACTION API
%%=================================================================
-export([
  commit/3,
  prepare_rollback/3,
  is_persistent/0
]).

%%=================================================================
%%	POOL API
%%=================================================================
-export([
  pool_batch/2
]).

%%=================================================================
%%	INFO API
%%=================================================================
-export([
  get_size/1
]).

-record(ref, {
  pterm,
  leveldb,
  pool
}).

-define(LOAD_BATCH_SIZE, 100000).

%%=================================================================
%%	SERVICE
%%=================================================================
create(Params)->
  case pipe(#ref{}, [
    fun(Ref)-> Ref#ref{pterm = zaya_pterm:create(type_params(pterm, Params))} end,
    fun(Ref)-> Ref#ref{leveldb = zaya_leveldb:create(type_params(leveldb, Params))} end,
    fun(#ref{pterm = PTermRef, leveldb = LeveldbRef} = Ref)->
      Ref#ref{pool = open_pool(PTermRef, LeveldbRef, Params)}
    end
  ]) of
    {ok, Ref}-> Ref;
    {error, Error, Ref}->
      catch close(Ref),
      catch remove(Params),
      throw(Error)
  end.

open(Params)->
  case pipe(#ref{}, [
    fun(Ref)-> Ref#ref{leveldb = zaya_leveldb:open(type_params(leveldb, Params))} end,
    fun(Ref)-> Ref#ref{pterm = zaya_pterm:open(type_params(pterm, Params))} end,
    fun(#ref{pterm = PTermRef, leveldb = LeveldbRef} = Ref)->
      Ref#ref{pool = open_pool(PTermRef, LeveldbRef, Params)}
    end,
    fun(#ref{pterm = PTermRef, leveldb = LeveldbRef} = Ref)->
      load_data(LeveldbRef, PTermRef),
      Ref
    end
  ]) of
    {ok, Ref}-> Ref;
    {error, Error, Ref}->
      catch close(Ref),
      throw(Error)
  end.

close(#ref{
  pterm = PTermRef,
  leveldb = LeveldbRef,
  pool = Pool
})->
  catch close_pool(Pool),
  catch zaya_pterm:close(PTermRef),
  catch zaya_leveldb:close(LeveldbRef),
  ok.

remove(Params)->
  zaya_leveldb:remove(type_params(leveldb, Params)),
  ok.

%%=================================================================
%%	LOW_LEVEL
%%=================================================================
read(#ref{pterm = PTermRef}, Keys)->
  zaya_pterm:read(PTermRef, Keys).

write(#ref{pterm = PTermRef, leveldb = LeveldbRef, pool = disabled}, KVs)->
  zaya_leveldb:write(LeveldbRef, KVs),
  zaya_pterm:write(PTermRef, KVs);
write(#ref{pool = Pool}, KVs)->
  zaya_pool:call(Pool, [{write, KVs}]).

delete(#ref{pterm = PTermRef, leveldb = LeveldbRef, pool = disabled}, Keys)->
  zaya_leveldb:delete(LeveldbRef, Keys),
  zaya_pterm:delete(PTermRef, Keys);
delete(#ref{pool = Pool}, Keys)->
  zaya_pool:call(Pool, [{delete, Keys}]).

%%=================================================================
%%	ITERATOR
%%=================================================================
first(#ref{pterm = PTermRef})->
  zaya_pterm:first(PTermRef).

last(#ref{pterm = PTermRef})->
  zaya_pterm:last(PTermRef).

next(#ref{pterm = PTermRef}, Key)->
  zaya_pterm:next(PTermRef, Key).

prev(#ref{pterm = PTermRef}, Key)->
  zaya_pterm:prev(PTermRef, Key).

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
find(#ref{pterm = PTermRef}, Query)->
  zaya_pterm:find(PTermRef, Query).

foldl(#ref{pterm = PTermRef}, Query, Fun, InAcc)->
  zaya_pterm:foldl(PTermRef, Query, Fun, InAcc).

foldr(#ref{pterm = PTermRef}, Query, Fun, InAcc)->
  Records = lists:reverse(zaya_pterm:find(PTermRef, foldr_query(Query))),
  try
    lists:foldl(fun(Rec, Acc)-> Fun(Rec, Acc) end, InAcc, Records)
  catch
    {stop, Acc}-> Acc
  end.

%%=================================================================
%%	COPY
%%=================================================================
copy(Ref, Fun, InAcc)->
  foldl(Ref, #{}, Fun, InAcc).

dump_batch(#ref{pterm = PTermRef, leveldb = LeveldbRef}, KVs)->
  zaya_leveldb:write(LeveldbRef, KVs),
  zaya_pterm:write(PTermRef, KVs).

%%=================================================================
%%	TRANSACTION API
%%=================================================================
commit(#ref{pterm = PTermRef, leveldb = LeveldbRef, pool = disabled}, Write, Delete)->
  zaya_leveldb:commit(LeveldbRef, Write, Delete),
  zaya_pterm:commit(PTermRef, Write, Delete),
  ok;
commit(#ref{pool = Pool}, Write, Delete)->
  zaya_pool:call(Pool, [{write, Write}, {delete, Delete}]).

prepare_rollback(#ref{pterm = PTermRef}, Write, Delete)->
  zaya_pterm:prepare_rollback(PTermRef, Write, Delete).

is_persistent()->
  true.

%%=================================================================
%%	POOL API
%%=================================================================
pool_batch({PTermRef, LeveldbRef}, Requests)->
  do_pool_batch(Requests, PTermRef, LeveldbRef, []).

do_pool_batch([{write, KVs} | Rest], PTermRef, LeveldbRef, Writes)->
  do_pool_batch(Rest, PTermRef, LeveldbRef, [KVs | Writes]);
do_pool_batch(Requests, PTermRef, LeveldbRef, [_ | _] = Writes)->
  KVs = lists:append(lists:reverse(Writes)),
  zaya_leveldb:write(LeveldbRef, KVs),
  zaya_pterm:write(PTermRef, KVs),
  do_pool_batch(Requests, PTermRef, LeveldbRef, []);
do_pool_batch([{delete, Keys} | Rest], PTermRef, LeveldbRef, Writes)->
  zaya_leveldb:delete(LeveldbRef, Keys),
  zaya_pterm:delete(PTermRef, Keys),
  do_pool_batch(Rest, PTermRef, LeveldbRef, Writes);
do_pool_batch([], _PTermRef, _LeveldbRef, [])->
  ok.

%%=================================================================
%%	INFO
%%=================================================================
get_size(#ref{pterm = PTermRef})->
  zaya_pterm:get_size(PTermRef).

%%=================================================================
%%	UTILITIES
%%=================================================================
load_data(LeveldbRef, PTermRef)->
  Tail = zaya_leveldb:foldl(LeveldbRef, #{}, fun(Rec, {Batch, Count})->
    Batch1 = [Rec | Batch],
    case Count + 1 of
      ?LOAD_BATCH_SIZE ->
        zaya_pterm:dump_batch(PTermRef, Batch1),
        {[], 0};
      Count1 ->
        {Batch1, Count1}
    end
  end, {[], 0}),
  case Tail of
    {[_ | _] = Rest, _} -> zaya_pterm:dump_batch(PTermRef, Rest);
    _ -> ok
  end.

open_pool(_PTermRef, _LeveldbRef, #{pool := disabled})->
  disabled;
open_pool(PTermRef, LeveldbRef, Params) when is_map(Params)->
  {ok, Pool} = zaya_pool:start_link(pool_params(PTermRef, LeveldbRef, Params)),
  Pool.

close_pool(disabled)->
  ok;
close_pool(Pool)->
  zaya_pool:stop(Pool).

pool_params(PTermRef, LeveldbRef, Params)->
  maps:merge(
    maps:get(pool, Params, #{}),
    #{
      ref => {PTermRef, LeveldbRef},
      module => ?MODULE
    }
  ).

pipe(Ref, [Step | Rest])->
  try Step(Ref) of
    Ref1 -> pipe(Ref1, Rest)
  catch _:Error ->
    {error, Error, Ref}
  end;
pipe(Ref, [])->
  {ok, Ref}.

foldr_query(Query)->
  Query0 = maps:without([start, stop, limit], Query),
  case {maps:find(start, Query), maps:find(stop, Query)} of
    {{ok, Start}, {ok, Stop}}->
      Query0#{start => Stop, stop => Start};
    {{ok, Start}, error}->
      Query0#{stop => Start};
    {error, {ok, Stop}}->
      Query0#{start => Stop};
    {error, error}->
      Query0
  end.

type_params(Type, Params)->
  TypeParams = maps:with([Type], Params),
  OtherParams = maps:without([pterm, leveldb, pool], Params),
  maps:merge(OtherParams#{pool => disabled}, TypeParams).
