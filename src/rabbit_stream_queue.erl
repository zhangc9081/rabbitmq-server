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
%% Copyright (c) 2018-2020 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_stream_queue).

-behaviour(rabbit_queue_type).

-export([is_enabled/0,
         declare/2,
         delete/4,
         purge/1,
         policy_changed/1,
         recover/2,
         is_recoverable/1,
         consume/3,
         cancel/5,
         handle_event/2,
         deliver/2,
         settle/3,
         reject/4,
         credit/4,
         dequeue/4,
         info/2,
         init/1,
         update/2,
         state_info/1,
         stat/1]).

-include("rabbit.hrl").
-include("amqqueue.hrl").

-import(rabbit_queue_type_util, [args_policy_lookup/3,
                                 qname_to_internal_name/1]).

-spec is_enabled() -> boolean().
is_enabled() ->
    rabbit_feature_flags:is_enabled(stream_queue).

-spec declare(amqqueue:amqqueue(), node()) ->
    {'new' | 'existing', amqqueue:amqqueue()} |
    rabbit_types:channel_exit().
declare(Q0, Node) when ?amqqueue_is_stream(Q0) ->
    Arguments = amqqueue:get_arguments(Q0),
    QName = amqqueue:get_name(Q0),
    check_invalid_arguments(QName, Arguments),
    rabbit_queue_type_util:check_auto_delete(Q0),
    rabbit_queue_type_util:check_exclusive(Q0),
    rabbit_queue_type_util:check_non_durable(Q0),
    Opts = amqqueue:get_options(Q0),
    ActingUser = maps:get(user, Opts, ?UNKNOWN_USER),
    Conf0 = make_stream_conf(Node, Q0),
    case osiris:start_cluster(Conf0) of
        {ok, #{leader_pid := LeaderPid} = Conf} ->
            Q1 = amqqueue:set_type_state(amqqueue:set_pid(Q0, LeaderPid), Conf),
            case rabbit_amqqueue:internal_declare(Q1, false) of
                {created, Q} ->
                    rabbit_event:notify(queue_created,
                                        [{name, QName},
                                         {durable, true},
                                         {auto_delete, false},
                                         {arguments, Arguments},
                                         {user_who_performed_action,
                                          ActingUser}]),
                    {new, Q};
                {error, Error} ->
                    _ = rabbit_amqqueue:internal_delete(QName, ActingUser),
                    rabbit_misc:protocol_error(
                      internal_error,
                      "Cannot declare a queue '~s' on node '~s': ~255p",
                      [rabbit_misc:rs(QName), node(), Error]);
                {existing, _} = Ex ->
                    Ex
            end;
        {error, {already_started, _}} ->
            rabbit_misc:protocol_error(precondition_failed,
                                       "safe queue name already in use '~s'",
                                       [Node])
    end.

-spec delete(amqqueue:amqqueue(), boolean(),
             boolean(), rabbit_types:username()) ->
    rabbit_types:ok(non_neg_integer()) |
    rabbit_types:error(in_use | not_empty).
delete(Q, _IfUnused, _IfEmpty, ActingUser) ->
    osiris:delete_cluster(amqqueue:get_type_state(Q)),
    _ = rabbit_amqqueue:internal_delete(amqqueue:get_name(Q), ActingUser),
    %% TODO return number of ready messages
    {ok, 0}.

-spec purge(amqqueue:amqqueue()) ->
    {'ok', non_neg_integer()}.
purge(_Q) ->
    {ok, 0}.

-spec policy_changed(amqqueue:amqqueue()) -> 'ok'.
policy_changed(_Q) ->
    ok.

stat(_) ->
    {ok, 0, 0}.

consume(_, _, _) ->
    ok.

cancel(_, _, _, _, _) ->
    ok.

credit(_, _, _, _) ->
    ok.

deliver(_, _) ->
    ok.

dequeue(_, _, _, _) ->
    ok.

handle_event(_, _) ->
    ok.

is_recoverable(_) ->
    ok.

recover(_, _) ->
    ok.

reject(_, _, _, _) ->
    ok.

settle(_, _, _) ->
    ok.

info(_, _) ->
    ok.

init(_) ->
    ok.

update(_, State) ->
    State.

state_info(_) ->
    ok.

make_stream_conf(Node, Q) ->
    QName = amqqueue:get_name(Q),
    Name = qname_to_internal_name(QName),
    LName = atom_to_list(Name),
    N = ra_lib:derive_safe_string(LName, length(LName)),
    MaxLength = args_policy_lookup(<<"max-length">>, fun min/2, Q),
    MaxBytes = args_policy_lookup(<<"max-length-bytes">>, fun min/2, Q),
    %% TODO max age units
    MaxAge = args_policy_lookup(<<"max-age">>, fun max_age/2, Q),
    Replicas = rabbit_mnesia:cluster_nodes(all) -- [Node],
    #{reference => QName,
      name => list_to_atom(N),
      max_length => MaxLength,
      max_bytes => MaxBytes,
      max_age => MaxAge,
      leader_node => Node,
      replica_nodes => Replicas}.

max_age(Age1, Age2) ->
    min(max_age_in_ms(Age1), max_age_in_ms(Age2)).

max_age_in_ms(Age) ->
    {match, [Value, Unit]} = re:run(Age, "(^[0-9]*)(.*)", [{capture, all_but_first, list}]),
    Int = list_to_integer(Value),
    Int * unit_value_in_ms(Unit).

unit_value_in_ms("Y") ->
    365 * unit_value_in_ms("D");
unit_value_in_ms("M") ->
    30 * unit_value_in_ms("D");
unit_value_in_ms("D") ->
    24 * unit_value_in_ms("h");
unit_value_in_ms("h") ->
    3600 * unit_value_in_ms("s");
unit_value_in_ms("m") ->
    60 * unit_value_in_ms("s");
unit_value_in_ms("s") ->
    1000.

check_invalid_arguments(QueueName, Args) ->
    Keys = [<<"x-expires">>, <<"x-message-ttl">>,
            <<"x-max-priority">>, <<"x-queue-mode">>, <<"x-overflow">>,
            <<"x-max-in-memory-length">>, <<"x-max-in-memory-bytes">>,
            <<"x-quorum-initial-group-size">>, <<"x-cancel-on-ha-failover">>],
    rabbit_queue_type_util:check_invalid_arguments(QueueName, Args, Keys).
