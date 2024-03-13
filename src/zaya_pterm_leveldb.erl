
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
  t_delete/3,
  commit/2,
  commit1/2,
  commit2/2,
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
transaction( #ref{ pterm = PTermRef, leveldb = LeveldbRef } )->
  PTermTRef = zaya_pterm:transaction( PTermRef ),
  LeveldbTRef = zaya_leveldb:transaction( LeveldbRef ),
  { PTermTRef, LeveldbTRef }.

t_write( #ref{ pterm = PTermRef, leveldb = LeveldbRef }, {PTermTRef, LeveldbTRef}, KVs )->
  zaya_leveldb:t_write(LeveldbRef, LeveldbTRef, KVs ),
  zaya_pterm:t_write( PTermRef, PTermTRef, KVs ),
  ok.

t_delete( #ref{ pterm = PTermRef, leveldb = LeveldbRef }, {PTermTRef, LeveldbTRef}, Keys )->
  zaya_leveldb:t_delete(LeveldbRef, LeveldbTRef, Keys ),
  zaya_pterm:t_delete( PTermRef, PTermTRef, Keys ),
  ok.

commit(#ref{ pterm = PTermRef, leveldb = LeveldbRef }, {PTermTRef, LeveldbTRef})->
  zaya_leveldb:commit( LeveldbRef, LeveldbTRef ),
  zaya_pterm:commit( PTermRef, PTermTRef ),
  ok.

commit1(#ref{ pterm = PTermRef, leveldb = LeveldbRef }, {PTermTRef, LeveldbTRef})->
  zaya_leveldb:commit1( LeveldbRef, LeveldbTRef ),
  zaya_pterm:commit1( PTermRef, PTermTRef ),
  ok.

commit2(#ref{ pterm = PTermRef, leveldb = LeveldbRef }, {PTermTRef, LeveldbTRef})->
  zaya_leveldb:commit2( LeveldbRef, LeveldbTRef ),
  zaya_pterm:commit2( PTermRef, PTermTRef ),
  ok.

rollback(#ref{pterm = PTermRef, leveldb = LeveldbRef }, {PTermTRef, LeveldbTRef})->
  zaya_leveldb:rollback( LeveldbRef, LeveldbTRef ),
  zaya_pterm:rollback(PTermRef, PTermTRef ),
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