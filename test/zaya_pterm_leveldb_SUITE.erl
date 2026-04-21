-module(zaya_pterm_leveldb_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([prepare_rollback_roundtrip_test/1, is_persistent_test/1]).

all()->
  [prepare_rollback_roundtrip_test, is_persistent_test].

init_per_suite(Config)->
  {ok, _Apps} = application:ensure_all_started(zaya_pterm_leveldb),
  Config.

end_per_suite(_Config)->
  ok = application:stop(zaya_pterm_leveldb),
  ok.

init_per_testcase(TestCase, Config)->
  Dir =
    filename:join(
      ?config(priv_dir, Config),
      atom_to_list(TestCase) ++ "_" ++ integer_to_list(erlang:unique_integer([positive]))
    ),
  [{db_dir, Dir} | Config].

end_per_testcase(_TestCase, Config)->
  Dir = ?config(db_dir, Config),
  catch zaya_pterm_leveldb:remove(#{dir => Dir}),
  ok.

prepare_rollback_roundtrip_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_pterm_leveldb:write(Ref, [{item, original}, {drop, old}]),
      {RollbackWrite, RollbackDelete} =
        zaya_pterm_leveldb:prepare_rollback(Ref, [{item, updated}, {fresh, new}], [drop]),
      ok = zaya_pterm_leveldb:commit(Ref, [{item, updated}, {fresh, new}], [drop]),
      ok = zaya_pterm_leveldb:commit(Ref, RollbackWrite, RollbackDelete),
      ?assertEqual(
        #{item => original, drop => old},
        read_map(Ref, [item, drop])
      ),
      ?assertEqual([], zaya_pterm_leveldb:read(Ref, [fresh]))
    end
  ).

is_persistent_test(_Config)->
  ?assertEqual(true, zaya_pterm_leveldb:is_persistent()).

with_ref(Config, Fun)->
  Params = #{dir => ?config(db_dir, Config)},
  catch zaya_pterm_leveldb:remove(Params),
  Ref = zaya_pterm_leveldb:create(Params),
  try
    Fun(Ref)
  after
    ok = zaya_pterm_leveldb:close(Ref)
  end.

read_map(Ref, Keys)->
  maps:from_list(zaya_pterm_leveldb:read(Ref, Keys)).
