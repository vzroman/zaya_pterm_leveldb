
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
%%	INFO API
%%=================================================================
-export([
  get_size/1
]).

-record(ref,{ets,leveldb}).

%%=================================================================
%%	SERVICE
%%=================================================================
create( Params )->
  EtsParams = type_params(ets,Params),
  EtsRef = zaya_ets:create( EtsParams ),
  try
    LeveldbRef = zaya_leveldb:create( type_params(leveldb, Params) ),
    #ref{ ets = EtsRef, leveldb = LeveldbRef }
  catch
    _:E->
      catch zaya_ets:close(EtsRef),
      catch zaya_ets:remove(EtsParams),
      throw(E)
  end.

open( Params )->
  LeveldbRef = zaya_leveldb:open( type_params(leveldb, Params ) ),
  EtsRef = zaya_ets:open( type_params(ets,Params) ),

  zaya_leveldb:foldl(LeveldbRef,#{},fun(Rec,Acc)->
    zaya_ets:write( EtsRef, [Rec] ),
    Acc
  end,[]),

  #ref{ ets = EtsRef, leveldb = LeveldbRef }.

close( #ref{ets = EtsRef, leveldb = LeveldbRef} )->
  catch zaya_ets:close( EtsRef ),
  zaya_leveldb:close( LeveldbRef ).

remove( #{leveldb := Params} )->
  zaya_leveldb:remove( Params ).

%%=================================================================
%%	LOW_LEVEL
%%=================================================================
read(#ref{ets = EtsRef}, Keys)->
  zaya_ets:read( EtsRef, Keys ).

write(#ref{ets = EtsRef, leveldb = LeveldbRef}, KVs)->
  zaya_leveldb:write( LeveldbRef, KVs ),
  zaya_ets:write( EtsRef, KVs ).

delete(#ref{ets = EtsRef, leveldb = LeveldbRef}, Keys)->
  zaya_leveldb:delete( LeveldbRef, Keys ),
  zaya_ets:delete( EtsRef, Keys ).

%%=================================================================
%%	ITERATOR
%%=================================================================
first( #ref{ets = EtsRef} )->
  zaya_ets:first( EtsRef ).

last( #ref{ets = EtsRef} )->
  zaya_ets:last( EtsRef ).

next( #ref{ets = EtsRef}, Key )->
  zaya_ets:next( EtsRef, Key ).

prev( #ref{ets = EtsRef}, Key )->
  zaya_ets:prev( EtsRef, Key ).

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
%----------------------FIND------------------------------------------
find(#ref{ets = EtsRef}, Query)->
  zaya_ets:find( EtsRef, Query ).

%----------------------FOLD LEFT------------------------------------------
foldl( #ref{ets = EtsRef}, Query, Fun, InAcc )->
  zaya_ets:foldl( EtsRef, Query, Fun, InAcc ).

%----------------------FOLD RIGHT------------------------------------------
foldr( #ref{ets = EtsRef}, Query, Fun, InAcc )->
  zaya_ets:foldr( EtsRef, Query, Fun, InAcc ).

%%=================================================================
%%	INFO
%%=================================================================
get_size( #ref{ets = EtsRef})->
  zaya_ets:get_size( EtsRef ).

type_params( Type, Params )->
  TypeParams = maps:with([Type],Params),
  OtherParams = maps:without([ets,leveldb], Params),
  maps:merge( OtherParams, TypeParams ).