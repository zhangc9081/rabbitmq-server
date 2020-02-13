%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_stream2_queue_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

% -import(quorum_queue_utils, [wait_for_messages_ready/3,
%                              wait_for_messages_pending_ack/3,
%                              wait_for_messages_total/3,
%                              wait_for_messages/2,
%                              dirty_query/3,
%                              ra_name/1]).

-compile(export_all).

suite() ->
    [{timetrap, 5 * 60000}].

all() ->
    [
      {group, stream2}
    ].

groups() ->
    [
     {stream2, [], [
                    {single_node, [], all_tests()},
                    {clustered, [], [cluster_delete_queue,
                                     publish_confirm_with_replica_down]
                     ++ all_tests()}
                   ]}
    ].

all_tests() ->
    [
     roundtrip,
     time_travel,
     idempotent_declare_queue,
     delete_queue,
     zenflix,
     declare_queue,
     counters_initialised_to_zero
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config = rabbit_ct_helpers:merge_app_env(
               Config0, {rabbit, [{stream2_tick_interval, 100}]}),
    rabbit_ct_helpers:run_setup_steps(
      Config,
      [fun rabbit_ct_broker_helpers:enable_dist_proxy_manager/1]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(stream, Config) ->
    [{queue_type, <<"stream">>} | Config];
init_per_group(stream2, Config) ->
    [{queue_type, <<"stream2">>} | Config];
init_per_group(Group, Config) ->
    ClusterConf = case Group of
                      clustered ->
                          [{rmq_nodes_count, 3},
                           {rmq_nodes_clustered, true}];
                      single_node ->
                          [{rmq_nodes_count, 1},
                           {rmq_nodes_clustered, false}]
                  end,

    Config1 = rabbit_ct_helpers:set_config(Config,
                                           ClusterConf ++
                                           [{rmq_nodename_suffix, Group},
                                            {tcp_ports_base}]),
    Config1b = rabbit_ct_helpers:set_config(Config1, [{net_ticktime, 10}]),
    Config2 = rabbit_ct_helpers:run_steps(Config1b,
                                          [fun merge_app_env/1 ] ++
                                          rabbit_ct_broker_helpers:setup_steps()),
    case rabbit_ct_broker_helpers:enable_feature_flag(Config2, quorum_queue) of
        ok ->
            ok = rabbit_ct_broker_helpers:rpc(
                   Config2, 0, application, set_env,
                   [rabbit, channel_tick_interval, 100]),
            %% HACK: the larger cluster sizes benefit for a bit more time
            %% after clustering before running the tests.
            case Group of
                cluster_size_5 ->
                    timer:sleep(5000),
                    Config2;
                _ ->
                    Config2
            end;
        Skip ->
            end_per_group(Group, Config2),
            Skip
    end.

end_per_group(stream, Config) ->
    Config;
end_per_group(stream2, Config) ->
    Config;
end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    % rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config2 = rabbit_ct_helpers:set_config(Config1,
                                           [{queue_name, Q},
                                            {alt_queue_name, <<Q/binary, "_alt">>}
                                           ]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps()).

merge_app_env(Config) ->
    rabbit_ct_helpers:merge_app_env(
      rabbit_ct_helpers:merge_app_env(Config,
                                      {rabbit, [{core_metrics_gc_interval, 100}]}),
      {ra, [{min_wal_roll_over_interval, 30000}]}).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

roundtrip(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server),
    QName = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr,
                                      ?config(queue_type, Config)}])),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    CTag1 = <<"ctag1">>,
    subscribe(Ch2, CTag1, QName, 100, [{<<"x-stream-offset">>, long, 0}]),
    publish_confirm(Ch, QName, <<"msg1">>),
    receive
        {#'basic.deliver'{delivery_tag = DT1,
                          consumer_tag = CTag1,
                          redelivered  = false}, Msg} ->
            ct:pal("GOT ~w ~w", [DT1, Msg]),
            ok
    after 2000 ->
              flush(100),
              exit(basic_deliver_timeout_1)
    end,
    publish_confirm(Ch, QName, <<"msg2">>),
    receive
        {#'basic.deliver'{delivery_tag = DT2,
                          consumer_tag = CTag1,
                          redelivered  = false}, Msg2} ->
            ct:pal("GOT ~w ~w", [DT2, Msg2]),
            ok
    after 2000 ->
              flush(100),
              exit(basic_deliver_timeout_2)
    end,
    %% another consumer can read
    Ch3 = rabbit_ct_client_helpers:open_channel(Config, Server),
    CTag2 = <<"ctag2">>,
    subscribe(Ch3, CTag2, QName, 1, [{<<"x-stream-offset">>, long, 0}]),
    receive
        {#'basic.deliver'{delivery_tag = DTag3,
                          consumer_tag = CTag2,
                          redelivered  = false}, Msg3} ->
            ct:pal("~s GOT ~w", [CTag2, Msg3]),
            amqp_channel:cast(Ch3, #'basic.ack'{delivery_tag = DTag3,
                                                multiple = false}),
            ok
    after 2000 ->
              flush(10),
              exit(basic_deliver_timeout_3)
    end,
    receive
        {#'basic.deliver'{delivery_tag = DTag4,
                          consumer_tag = CTag2,
                          redelivered  = false}, Msg4} ->
            ct:pal("~s GOT ~w", [CTag2, Msg4]),
            amqp_channel:cast(Ch3, #'basic.ack'{delivery_tag = DTag4,
                                                multiple = false}),
            ok
    after 2000 ->
              flush(10),
              exit(basic_deliver_timeout_4)
    end,
    cancel(Ch3, CTag2),
    flush(100),
    ok.

time_travel(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server),
    QName = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr,
                                      ?config(queue_type, Config)}])),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    CTag1 = <<"ctag1">>,
    publish_many(Ch, QName, 100),
    publish_confirm(Ch, QName, <<"msg1">>),
    subscribe(Ch2, CTag1, QName, 1, [{<<"x-stream-offset">>, long, 50}]),
    receive
        {#'basic.deliver'{delivery_tag = DT1,
                          consumer_tag = _CTag1,
                          redelivered  = false},
         #amqp_msg{props = #'P_basic'{headers = Headers}} = Msg} ->
            {<<"x-stream-offset">>, long, Offs} =
                rabbit_basic:header(<<"x-stream-offset">>, Headers),
            %% assert offset is greater or equal to request
            ct:pal("GOT ~w ~w", [Offs, Msg]),
            ?assert(Offs >= 50),
            amqp_channel:cast(Ch2, #'basic.ack'{delivery_tag = DT1,
                                                multiple = false}),
            flush(100),
            ok
    after 2000 ->
              exit(basic_deliver_timeout_1)
    end,
    ok.

idempotent_declare_queue(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [MnesiaDir | _] = rabbit_ct_broker_helpers:get_node_configs(Config, mnesia_dir),

    StreamsDir = filename:join(MnesiaDir, "streams"),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),

    QName = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr,
                                      ?config(queue_type, Config)}])),
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr,
                                      ?config(queue_type, Config)}])),

    ?assertEqual([[QName]], lists:sort(rabbit_ct_broker_helpers:rabbitmqctl_list(
                                      Config, 0, ["list_queues", "name", "--no-table-headers"]))),
    ?assertMatch({ok, [_]}, file:list_dir(StreamsDir)),

    flush(100),
    ok.

delete_queue(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [MnesiaDir | _] = rabbit_ct_broker_helpers:get_node_configs(Config, mnesia_dir),

    StreamsDir = filename:join(MnesiaDir, "streams"),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),

    QName = ?config(queue_name, Config),
    QName2 = ?config(alt_queue_name, Config),

    %% Let's declare two queues
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr,
                                      ?config(queue_type, Config)}])),
    ?assertEqual({'queue.declare_ok', QName2, 0, 0},
                 declare(Ch, QName2, [{<<"x-queue-type">>, longstr,
                                      ?config(queue_type, Config)}])),
    publish_many(Ch, QName, 100),
    publish_many(Ch, QName2, 100),

    %% Check the queues are listed and have a data directory
    Queues = lists:sort([[QName], [QName2]]),
    ?assertEqual(Queues, lists:sort(rabbit_ct_broker_helpers:rabbitmqctl_list(
                                      Config, 0, ["list_queues", "name", "--no-table-headers"]))),
    ?assertMatch({ok, [_, _]}, file:list_dir(StreamsDir)),

    %% Delete one of the queues
    ?assertEqual({'queue.delete_ok', 0},
                 amqp_channel:call(Ch, #'queue.delete'{queue  = QName})),
 
    ?assertEqual([[QName2]], rabbit_ct_broker_helpers:rabbitmqctl_list(
                               Config, 0, ["list_queues", "name", "--no-table-headers"])),
    ?assertMatch({ok, [_]}, file:list_dir(StreamsDir)),

    %% Delete the other queue
    ?assertEqual({'queue.delete_ok', 0},
                 amqp_channel:call(Ch, #'queue.delete'{queue  = QName2})),
 
    ?assertEqual([], rabbit_ct_broker_helpers:rabbitmqctl_list(
                       Config, 0, ["list_queues", "name", "--no-table-headers"])),
    ?assertMatch({ok, []}, file:list_dir(StreamsDir)),

    flush(100),
    ok.

cluster_delete_queue(Config) ->
    [Server1, Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [MnesiaDir1, MnesiaDir2, MnesiaDir3] = rabbit_ct_broker_helpers:get_node_configs(Config, mnesia_dir),

    StreamsDir1 = filename:join(MnesiaDir1, "streams"),
    StreamsDir2 = filename:join(MnesiaDir2, "streams"),
    StreamsDir3 = filename:join(MnesiaDir3, "streams"),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server2),
    Ch3 = rabbit_ct_client_helpers:open_channel(Config, Server3),

    QName = ?config(queue_name, Config),
    Type = ?config(queue_type, Config),

    %% Declare and delete on the same node
    assert_declare(Ch1, QName, Type),
    assert_exists(QName, Config, StreamsDir1, StreamsDir2, StreamsDir3),

    ?assertEqual({'queue.delete_ok', 0},
                 amqp_channel:call(Ch1, #'queue.delete'{queue  = QName})),
    assert_deleted(Config, StreamsDir1, StreamsDir2, StreamsDir3),

    %% Declare and delete on different nodes
    assert_declare(Ch1, QName, Type),
    assert_exists(QName, Config, StreamsDir1, StreamsDir2, StreamsDir3),

    ?assertEqual({'queue.delete_ok', 0},
                 amqp_channel:call(Ch2, #'queue.delete'{queue  = QName})),
    assert_deleted(Config, StreamsDir1, StreamsDir2, StreamsDir3),

    %% Try to delete again
    ?assertEqual({'queue.delete_ok', 0},
                 amqp_channel:call(Ch2, #'queue.delete'{queue  = QName})),
    ?assertEqual({'queue.delete_ok', 0},
                 amqp_channel:call(Ch3, #'queue.delete'{queue  = QName})),

    %% Declare and delete on different nodes
    assert_declare(Ch3, QName, Type),
    assert_exists(QName, Config, StreamsDir1, StreamsDir2, StreamsDir3),

    ?assertEqual({'queue.delete_ok', 0},
                 amqp_channel:call(Ch1, #'queue.delete'{queue  = QName})),
    assert_deleted(Config, StreamsDir1, StreamsDir2, StreamsDir3),

    flush(100),
    ok.

zenflix(Config) ->
    %% Let's stream our own movie, no sequence should be out of order!
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server),
    QName = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch1, QName, [{<<"x-queue-type">>, longstr,
                                      ?config(queue_type, Config)}])),

    publish_stream(Ch1, QName, 1, 100),

    consume_stream(<<"ctag1">>, Ch2, QName, 0, 100),
    consume_stream(<<"ctag2">>, Ch2, QName, 35, 100),

    [rabbit_ct_broker_helpers:stop_node(Config, S) || S <- Servers],
    [rabbit_ct_broker_helpers:start_node(Config, S) || S <- lists:reverse(Servers)],

    Ch3 = rabbit_ct_client_helpers:open_channel(Config, Server),
    Ch4 = rabbit_ct_client_helpers:open_channel(Config, Server),

    consume_stream(<<"ctag3">>, Ch3, QName, 0, 100),
    consume_stream(<<"ctag4">>, Ch3, QName, 98, 100),
    consume_stream(<<"ctag5">>, Ch3, QName, 35, 100),
    consume_stream(<<"ctag6">>, Ch3, QName, 10, 100),
    consume_stream(<<"ctag7">>, Ch3, QName, 89, 100),
    consume_stream(<<"ctag8">>, Ch3, QName, 100, 100),

    publish_stream(Ch4, QName, 101, 200),

    consume_stream(<<"ctag8">>, Ch3, QName, 75, 200),

    flush(100),
    ok.

declare_queue(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),

    QName = <<"declare_queue">>,
    SimilarQName = <<"'declare_queue'">>,

    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr,
                                      ?config(queue_type, Config)}])),

    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 541, _}}}, _},
                declare(Ch, SimilarQName, [{<<"x-queue-type">>, longstr,
                                            ?config(queue_type, Config)}])),

    flush(100),
    ok.

counters_initialised_to_zero(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    QName = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch1, QName, [{<<"x-queue-type">>, longstr,
                                      ?config(queue_type, Config)}])),

    timer:sleep(500),

    ?assertEqual([[<<"0">>, <<"0">>, <<"0">>]],
                 rabbit_ct_broker_helpers:rabbitmqctl_list(
                   Config, 0, ["list_queues", "messages", "messages_ready",
                               "messages_unacknowledged", "--no-table-headers"])),

    flush(100),
    ok.

publish_confirm_with_replica_down(Config) ->
    [Server1, Server2, _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),
    QName = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr,
                                      ?config(queue_type, Config)}])),

    rabbit_ct_broker_helpers:stop_node(Config, Server2),

    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    publish(Ch, QName, <<"msg1">>),

    amqp_channel:register_confirm_handler(Ch, self()),
    ok = receive
             #'basic.nack'{} -> exit(unconfirmed);
             #'basic.ack'{} -> exit(confirmed)
         after 1000 ->
                 ok
         end,

    rabbit_ct_broker_helpers:start_node(Config, Server2),
    flush(100),
    ok.

%% HELPERS

assert_declare(Ch, QName, Type) ->
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr, Type}])),
    publish_many(Ch, QName, 100).

assert_exists(QName, Config, StreamsDir1, StreamsDir2, StreamsDir3) ->
    ?assertEqual([[QName]], rabbit_ct_broker_helpers:rabbitmqctl_list(
                              Config, 0, ["list_queues", "name", "--no-table-headers"])),
    ?assertMatch({ok, [_]}, file:list_dir(StreamsDir1)),
    ?assertMatch({ok, [_]}, file:list_dir(StreamsDir2)),
    ?assertMatch({ok, [_]}, file:list_dir(StreamsDir3)).

assert_deleted(Config, StreamsDir1, StreamsDir2, StreamsDir3) ->
    ?assertEqual([], rabbit_ct_broker_helpers:rabbitmqctl_list(
                       Config, 0, ["list_queues", "name", "--no-table-headers"])),
    ?assertMatch({ok, []}, file:list_dir(StreamsDir1)),
    ?assertMatch({ok, []}, file:list_dir(StreamsDir2)),
    ?assertMatch({ok, []}, file:list_dir(StreamsDir3)).

publish_confirm(Ch, QName, Msg) ->
    publish(Ch, QName, Msg),
    amqp_channel:register_confirm_handler(Ch, self()),
    %% ct:pal("waiting for confirms from ~s", [QName]),
    ok = receive
             #'basic.ack'{}  -> ok;
             #'basic.nack'{} -> fail
         after 2500 ->
                   exit({confirm_timeout, Msg})
         end,
    %% ct:pal("CONFIRMED! ~s", [QName]),
    ok.

publish_many(Ch, Queue, Count) ->
    [publish(Ch, Queue, <<I:16/integer>>) || I <- lists:seq(1, Count)].

publish(Ch, Queue, Msg) ->
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = Queue},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                     payload = Msg}).
declare(Ch, Q) ->
    declare(Ch, Q, []).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue  = Q,
                                           durable = true,
                                           auto_delete = false,
                                           arguments = Args}).

subscribe(Ch, CTag, Queue, Prefetch, Args) ->
    qos(Ch, Prefetch),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Queue,
                                                no_ack = true,
                                                arguments = Args,
                                                consumer_tag = CTag},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} ->
             ok
    end.

cancel(Ch, CTag) ->
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}).

qos(Ch, Prefetch) ->
    ?assertMatch(#'basic.qos_ok'{},
                 amqp_channel:call(Ch, #'basic.qos'{global = false,
                                                    prefetch_count = Prefetch})).

flush(T) ->
    receive X ->
                ct:pal("flushed ~w", [X]),
                flush(T)
    after T ->
              ok
    end.

delete_queues() ->
    [rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].

publish_stream(Ch, QName, From, To) ->
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    [begin
         Msg = <<S:64>>,
         publish_confirm(Ch, QName, Msg)
     end || S <- lists:seq(From, To)].

consume_stream(CTag, Ch, QName, Offset, Upto) ->
    subscribe(Ch, CTag, QName, 1, [{<<"x-stream-offset">>, long, Offset}]),
    receive_stream(Ch, CTag, Offset + 1, Upto + 1),
    cancel(Ch, CTag).

receive_stream(_, _, From, From) ->
    ok;
receive_stream(Ch, CTag, From, To) ->
    Msg = <<From:64>>,
    receive
        {#'basic.deliver'{delivery_tag = DT,
                          consumer_tag = CTag,
                          redelivered  = false},
         #amqp_msg{payload = Msg}} ->
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DT,
                                               multiple = false}),
            receive_stream(Ch, CTag, From + 1, To)
    after 2000 ->
            flush(100),
            exit({receive_stream_timeout, CTag, From})
    end.
