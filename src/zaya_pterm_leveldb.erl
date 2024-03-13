
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
  transaction/1,
  t_write/3,
  commit/2,
  rollback/2
]).

%%=================================================================
%%	INFO API
%%=================================================================
-export([
  get_size/1
]).

-record(ref,{pterm,leveldb}).

%%=================================================================
%%	SERVICE
%%=================================================================
create( Params )->
  PTermParams = type_params(pterm,Params),
  PTermRef = zaya_pterm:create( PTermParams ),
  try
    LeveldbRef = zaya_leveldb:create( type_params(leveldb, Params) ),
    #ref{ pterm = PTermRef, leveldb = LeveldbRef }
  catch
    _:E->
      catch zaya_pterm:close(PTermRef),
      catch zaya_pterm:remove(PTermParams),
      throw(E)
  end.

open( Params )->
  LeveldbRef = zaya_leveldb:open( type_params(leveldb, Params ) ),
  PTermRef = zaya_pterm:open( type_params(pterm,Params) ),

  zaya_pterm:write( PTermRef, zaya_leveldb:find( LeveldbRef, #{})),

  #ref{ pterm = PTermRef, leveldb = LeveldbRef }.

close( #ref{pterm = PTermRef, leveldb = LeveldbRef} )->
  catch zaya_pterm:close( PTermRef ),
  zaya_leveldb:close( LeveldbRef ).

remove( Params )->
  zaya_leveldb:remove( type_params(leveldb, Params) ).

%%=================================================================
%%	LOW_LEVEL
%%=================================================================
read(#ref{pterm = PTermRef}, Keys)->
  zaya_pterm:read( PTermRef, Keys ).

write(#ref{pterm = PTermRef, leveldb = LeveldbRef}, KVs)->
  zaya_leveldb:write( LeveldbRef, KVs ),
  zaya_pterm:write( PTermRef, KVs ).

delete(#ref{pterm = PTermRef, leveldb = LeveldbRef}, Keys)->
  zaya_leveldb:delete( LeveldbRef, Keys ),
  zaya_pterm:delete( PTermRef, Keys ).

%%=================================================================
%%	ITERATOR
%%=================================================================
first( #ref{pterm = PTermRef} )->
  zaya_pterm:first( PTermRef ).

last( #ref{pterm = PTermRef} )->
  zaya_pterm:last( PTermRef ).

next( #ref{pterm = PTermRef}, Key )->
  zaya_pterm:next( PTermRef, Key ).

prev( #ref{pterm = PTermRef}, Key )->
  zaya_pterm:prev( PTermRef, Key ).

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
%----------------------FIND------------------------------------------
find(#ref{pterm = PTermRef}, Query)->
  zaya_pterm:find( PTermRef, Query ).

%----------------------FOLD LEFT------------------------------------------
foldl( #ref{pterm = PTermRef}, Query, Fun, InAcc )->
  zaya_pterm:foldl( PTermRef, Query, Fun, InAcc ).

%----------------------FOLD RIGHT------------------------------------------
foldr( #ref{pterm = PTermRef}, Query, Fun, InAcc )->
  zaya_pterm:foldr( PTermRef, Query, Fun, InAcc ).

%%=================================================================
%%	COPY
%%=================================================================
copy(Ref, Fun, InAcc)->
  foldl(Ref, #{}, Fun, InAcc).

dump_batch(Ref, KVs)->
  write(Ref, KVs).

%%=================================================================
%%	TRANSACTION API
%%=================================================================
transaction( _Ref )->
  ets:new(?MODULE,[
    private,
    ordered_set,
    {read_concurrency, true},
    {write_concurrency, auto}
  ]).

t_write( _Ref, TRef, KVs )->
  ets:insert( TRef, KVs ),
  ok.

commit(#ref{ pterm  = PTermRef, leveldb = LeveldbRef }, TRef)->
  KVs = ets:tab2list( TRef ),
  zaya_leveldb:write( LeveldbRef, KVs ),
  zaya_pterm:write( PTermRef, KVs ),
  ok.

rollback(_Ref, TRef )->
  ets:delete( TRef ),
  ok.

%%=================================================================
%%	INFO
%%=================================================================
get_size( #ref{pterm = PTermRef})->
  zaya_pterm:get_size( PTermRef ).

type_params( Type, Params )->
  TypeParams = maps:with([Type],Params),
  OtherParams = maps:without([pterm,leveldb], Params),
  maps:merge( OtherParams, TypeParams ).