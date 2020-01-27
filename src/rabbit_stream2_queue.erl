-module(rabbit_stream2_queue).

-include_lib("rabbit.hrl").
-include("amqqueue.hrl").

-export([
         %% client
         begin_stream/4,
         end_stream/2,
         credit/3,
         append/3,
         handle_written/2,
         handle_offset/2,

         init_client/1,
         queue_name/1,
         pending_size/1,

         %% mgmt
         declare/1,
         delete/2,
         infos/1,
         reductions/1,

         %% other
         open_files/1,
         cluster_state/1,
         
         recover/1
         ]).

-define(DELETE_TIMEOUT, 5000).
-define(STATISTICS_KEYS,
        [
         policy,
         % operator_policy,
         % effective_policy_definition,
         consumers,
         memory,
         % state,
         garbage_collection,
         leader,
         online,
         members,
         open_files
         % single_active_consumer_pid,
         % single_active_consumer_ctag,
         % messages_ram,
         % message_bytes_ram
        ]).

% -type stream_offset() :: non_neg_integer() | undefined.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% -type ctag() :: binary().

%% stream,undefined,53,0,97,-1,1,
-record(stream, {name :: rabbit_types:r('queue'),
                 credit :: integer(),
                 max = 1000 :: non_neg_integer(),
                 start_offset = 0 :: non_neg_integer(),
                 listening_offset = 0 :: non_neg_integer(),
                 log :: undefined | ra_log_reader:state()}).

stream_entries(Name, Id, Str) ->
    stream_entries(Name, Id, Str, []).

stream_entries(Name, LeaderPid,
               #stream{credit = Credit,
                       start_offset = StartOffs,
                       listening_offset = LOffs,
                       log = Seg0} = Str0, MsgIn)
  when Credit > 0 ->
    % rabbit_log:info("stream2 entries credit ~b to ~w",
    %                 [Credit, Name]),
    case osiris_segment:read_chunk_parsed(Seg0) of
        {end_of_stream, Seg} ->
            NextOffset = osiris_segment:next_offset(Seg),
            case NextOffset > LOffs of
                true ->
                    % rabbit_log:info("stream2 end_of_stream register listener ~w ~w",
                    %                 [LeaderPid, NextOffset]),
                    % osiris_writer:register_offset_listener(LeaderPid, NextOffset),
                    {Str0#stream{log = Seg,
                                 listening_offset = NextOffset}, MsgIn};
                false ->
                    {Str0#stream{log = Seg}, MsgIn}
            end;
        {Records, Seg} ->
            Msgs = [begin
                        Msg0 = binary_to_term(B),
                        Msg = rabbit_basic:add_header(<<"x-stream-offset">>,
                                                      long, O, Msg0),
                        {Name, LeaderPid, O, false, Msg}
                    end || {O, B} <- Records,
                           O >= StartOffs],
            % rabbit_log:info("stream2 msgs out ~p", [Msgs]),
            % rabbit_log:info("stream entries got entries", [Entries0]),
            NumMsgs = length(Msgs),

            Str = Str0#stream{credit = Credit - NumMsgs,
                              log = Seg},
            case Str#stream.credit < 1 of
                true ->
                    %% we are done here
                    % rabbit_log:info("stream entries out ~w ~w", [Msgs, Msgs]),
                    {Str, MsgIn ++ Msgs};
                false ->
                    %% if there are fewer Msgs than Entries0 it means there were non-events
                    %% in the log and we should recurse and try again
                    stream_entries(Name, LeaderPid, Str, MsgIn ++ Msgs)
            end
    end;
stream_entries(_Name, _Id, Str, Msgs) ->
    % rabbit_log:info("stream entries none ~w", [Str#stream.credit]),
    {Str, Msgs}.

%% CLIENT

-type appender_seq() :: non_neg_integer().

-record(stream2_client, {name :: term(),
                         leader = ra:server_id(),
                         next_seq = 1 :: non_neg_integer(),
                         correlation = #{} :: #{appender_seq() => term()},
                         readers = #{} :: #{term() => #stream{}}
                        }).

init_client(Q) when ?is_amqqueue(Q) ->
    % rabbit_log:info("init_client", []),
    Leader = amqqueue:get_pid(Q),
    #stream2_client{name = amqqueue:get_name(Q),
                    leader = Leader}.

queue_name(#stream2_client{name = Name}) ->
    Name.

pending_size(#stream2_client{correlation = Correlation}) ->
    maps:size(Correlation).

append(#stream2_client{leader = LeaderPid,
                       next_seq = Seq,
                       correlation = Correlation0} = State, MsgId, Event) ->
    % rabbit_log:info("stream2 append none ~w", [MsgId]),
    ok = osiris:write(LeaderPid, Seq, term_to_binary(Event)),
    Correlation = case MsgId of
                      undefined ->
                          Correlation0;
                      _ when is_number(MsgId) ->
                          Correlation0#{Seq => MsgId}
                  end,
    State#stream2_client{next_seq = Seq + 1,
                         correlation = Correlation}.

handle_written(#stream2_client{correlation = Correlation0} = State, Corrs) ->
    %% TODO: detect if any correlations were not written
    MsgIds = maps:values(maps:with(Corrs, Correlation0)),
    Correlation = maps:without(Corrs, Correlation0),
    {MsgIds, State#stream2_client{correlation = Correlation}}.

%% consumers


begin_stream(#stream2_client{leader = Leader,
                             readers = Readers0} = State,
             Tag, Offset, Max)
  when is_number(Max) ->
    case node(Leader) == node() of
        true ->
            Seg0 = osiris_writer:init_reader(Leader, Offset),
            NextOffset = osiris_segment:next_offset(Seg0) - 1,
            osiris_writer:register_offset_listener(Leader),
            %% TODO: avoid double calls to the same process
            StartOffset = case Offset of
                              undefined -> NextOffset;
                              _ ->
                                  Offset
                          end,
            Str0 = #stream{credit = Max,
                           start_offset = StartOffset,
                           listening_offset = NextOffset,
                           log = Seg0,
                           max = Max},
            State#stream2_client{readers = Readers0#{Tag => Str0}};
        false ->
            exit(non_local_stream2_readers_not_supported)
    end.

end_stream(#stream2_client{readers = Readers0} = State, Tag) ->
    Readers = maps:remove(Tag, Readers0),
    State#stream2_client{readers = Readers}.

handle_offset(#stream2_client{name = Name,
                              leader = Leader,
                              readers = Readers0} = State, _Offs) ->
    %% offset isn't actually needed as we use the atomic to read the
    %% current committed
    {Readers, TagMsgs} = maps:fold(
                           fun (Tag, Str0, {Acc, TM}) ->
                                   % rabbit_log:info("handle_offset for  ~w", [Tag]),
                                   {Str, Msgs} = stream_entries(Name, Leader, Str0),
                                   %% HACK for now, better to just return but
                                   %% tricky with acks credits
                                   %% that also evaluate the stream
                                   % gen_server:cast(self(), {stream_delivery, Tag, Msgs}),
                                   {Acc#{Tag => Str}, [{Tag, Leader, Msgs} | TM]}
                           end, {#{}, []}, Readers0),
    % rabbit_log:info("handle_offset ~w num msgs ~w", [_Offs, length(TagMsgs)]),
    {State#stream2_client{readers = Readers}, TagMsgs}.


credit(#stream2_client{name = Name,
                       leader = Leader,
                       readers = Readers0} = State, Tag, Credit) ->
    % rabbit_log:info("stream2 credit ~w ~w", [Credit, Tag]),
    {Readers, TagMsgs} = case Readers0 of
                          #{Tag := #stream{credit = Credit0} = Str0} ->
                              % rabbit_log:info("stream2 credit yeah ~w ~w", [Credit, Tag]),
                              Str1 = Str0#stream{credit = Credit0 + Credit},
                              {Str, Msgs0} = stream_entries(Name, Leader, Str1),
                              {Readers0#{Tag => Str}, {Tag, Leader, Msgs0}};
                          _ ->
                              {Readers0, []}
                      end,
    {ok, TagMsgs, State#stream2_client{readers = Readers}}.

%% MGMT

declare(Q0) ->
    QName = amqqueue:get_name(Q0),
    Name = qname_to_rname(QName),
    Arguments = amqqueue:get_arguments(Q0),
    Opts = amqqueue:get_options(Q0),
    ActingUser = maps:get(user, Opts, ?UNKNOWN_USER),
    Replicas = rabbit_mnesia:cluster_nodes(all) -- [node()],
    N = ra_lib:derive_safe_string(atom_to_list(Name), 8),
    Conf = #{reference => QName,
             name => list_to_atom(N)},
    {ok, LeaderPid, ReplicaPids} = osiris:start_cluster(N, Replicas, Conf),
    Q1 = amqqueue:set_pid(Q0, LeaderPid),
    NewQ1 = amqqueue:set_type_state(Q1, maps:put(replicas, ReplicaPids, Conf)),
    case rabbit_amqqueue:internal_declare(NewQ1, false) of
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
    end.

recover(Q0) ->
    Node = node(),
    Conf = amqqueue:get_type_state(Q0),
    Name = maps:get(name, Conf),
    QName = amqqueue:get_name(Q0),
    Pid = amqqueue:get_pid(Q0),
    case node(Pid) of
        Node ->
            Replicas = rabbit_mnesia:cluster_nodes(all) -- [node()],
            NewConf = maps:put(replicas, [], Conf),
            Q1 = amqqueue:set_type_state(Q0, NewConf),
            {ok, _} = rabbit_amqqueue:ensure_rabbit_queue_record_is_initialized(Q1),
            {ok, LeaderPid} = osiris:restart_server(Name, Replicas, NewConf),
            Q2 = amqqueue:set_pid(Q1, LeaderPid),
            {ok, _} = rabbit_amqqueue:ensure_rabbit_queue_record_is_initialized(Q2),
            Slaves = restart_replicas_on_nodes(Name, LeaderPid, Replicas, Conf),
            update_slaves(QName, Slaves);
        _ ->
            restart_replica(Name, Pid, Node, Conf, QName)
    end.

restart_replica(Name, LeaderPid, Node, Conf, QName) ->
    case rabbit_misc:is_process_alive(LeaderPid) of
        true ->
            case osiris:restart_replica(Name, LeaderPid, Node, Conf) of
                {ok, ReplicaPid} ->
                    update_slaves(QName, [ReplicaPid]);
                {error, already_present} ->
                    ok;
                {error, {already_started, _}} ->
                    ok;
                Error ->
                    rabbit_log:warning("Error starting stream ~p replica: ~p",
                                       [Name, Error]),
                    {error, replica_not_started}
            end;
        false ->
            rabbit_log:debug("Stream ~p writer is down, the replica on this node"
                             " will be started by the writer", [Name]),
            {error, replica_not_started}
    end.

restart_replicas_on_nodes(Name, LeaderPid, Replicas, Conf) ->
    lists:foldl(
      fun(Replica, Pids) ->
              try
                  case osiris:restart_replica(Name, LeaderPid, Replica, Conf) of
                      {ok, Pid} ->
                          [Pid | Pids];
                      {error, already_present} ->
                          Pids;
                      {error, {already_started, _}} ->
                          Pids;
                      Error ->
                          rabbit_log:warning("Error starting stream ~p replica on node ~p: ~p",
                                             [Name, Replica, Error]),
                          Pids
                  end
              catch
                  _:_ ->
                      %% Node is not yet up, this is normal
                      Pids
              end
      end, [], Replicas).

update_slaves(_QName, []) ->
    ok;
update_slaves(QName, Slaves0) ->
    Fun = fun (Q) ->
                  Conf = amqqueue:get_type_state(Q),
                  Slaves = filter_alive(maps:get(replicas, Conf)) ++ Slaves0,
                  amqqueue:set_type_state(Q, maps:put(replicas, Slaves, Conf))
          end,
    rabbit_misc:execute_mnesia_transaction(
      fun() -> rabbit_amqqueue:update(QName, Fun) end),
    ok.

filter_alive(Pids) ->
    lists:filter(fun(Pid) when node(Pid) == node() ->
                         rabbit_misc:is_process_alive(Pid);
                    (_) ->
                         true
                 end, Pids).

delete(Q, ActingUser) when ?amqqueue_is_stream2(Q) ->
    Conf = amqqueue:get_type_state(Q),
    Name = maps:get(name, Conf),
    osiris:delete_cluster(Name, amqqueue:get_pid(Q), maps:get(replicas, Conf)),
    delete_queue_data(amqqueue:get_name(Q), ActingUser),
    %% TODO return number of ready messages
    {ok, 0}.

qname_to_rname(#resource{virtual_host = <<"/">>, name = Name}) ->
    erlang:binary_to_atom(<<"%2F_", Name/binary>>, utf8);
qname_to_rname(#resource{virtual_host = VHost, name = Name}) ->
    erlang:binary_to_atom(<<VHost/binary, "_", Name/binary>>, utf8).

reductions(Name) ->
    try
        {reductions, R} = process_info(whereis(Name), reductions),
        R
    catch
        error:badarg ->
            0
    end.

infos(QName) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            info(Q, ?STATISTICS_KEYS);
        {error, not_found} ->
            []
    end.

-spec info(amqqueue:amqqueue(), rabbit_types:info_keys()) -> rabbit_types:infos().

info(Q, Items) ->
    [{Item, i(Item, Q)} || Item <- Items].

i(name,        Q) when ?is_amqqueue(Q) -> amqqueue:get_name(Q);
i(durable,     Q) when ?is_amqqueue(Q) -> amqqueue:is_durable(Q);
i(auto_delete, Q) when ?is_amqqueue(Q) -> amqqueue:is_auto_delete(Q);
i(arguments,   Q) when ?is_amqqueue(Q) -> amqqueue:get_arguments(Q);
i(pid, Q) when ?is_amqqueue(Q) ->
    amqqueue:get_pid(Q);
i(messages, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, _, _, M, _}] ->
            M;
        [] ->
            0
    end;
i(messages_ready, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, MR, _, _, _}] ->
            MR;
        [] ->
            0
    end;
i(messages_unacknowledged, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, _, MU, _, _}] ->
            MU;
        [] ->
            0
    end;
i(policy, Q) ->
    case rabbit_policy:name(Q) of
        none   -> '';
        Policy -> Policy
    end;
i(operator_policy, Q) ->
    case rabbit_policy:name_op(Q) of
        none   -> '';
        Policy -> Policy
    end;
i(effective_policy_definition, Q) ->
    case rabbit_policy:effective_definition(Q) of
        undefined -> [];
        Def       -> Def
    end;
i(consumers, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_metrics, QName) of
        [{_, M, _}] ->
            proplists:get_value(consumers, M, 0);
        [] ->
            0
    end;
i(memory, Q) when ?is_amqqueue(Q) ->
    Pid = amqqueue:get_pid(Q),
    try
        {memory, M} = process_info(Pid, memory),
        M
    catch
        error:badarg ->
            0
    end;
% i(state, Q) when ?is_amqqueue(Q) ->
%     {Name, Node} = amqqueue:get_pid(Q),
%     %% Check against the leader or last known leader
%     case rpc:call(Node, ?MODULE, cluster_state, [Name], 1000) of
%         {badrpc, _} -> down;
%         State -> State
%     end;
% i(local_state, Q) when ?is_amqqueue(Q) ->
%     {Name, _} = amqqueue:get_pid(Q),
%     case ets:lookup(ra_state, Name) of
%         [{_, State}] -> State;
%         _ -> not_member
%     end;
i(garbage_collection, Q) when ?is_amqqueue(Q) ->
    Pid = amqqueue:get_pid(Q),
    try
        rabbit_misc:get_gc_info(Pid)
    catch
        error:badarg ->
            []
    end;
i(members, Q) when ?is_amqqueue(Q) ->
    get_nodes(Q);
i(online, Q) ->
    get_nodes(Q);
i(leader, Q) ->
    Pid = amqqueue:get_pid(Q),
    node(Pid);
% i(open_files, Q) when ?is_amqqueue(Q) ->
%     Pid = amqqueue:get_pid(Q),
%     Nodes = get_nodes(Q),
%     {Data, _} = rpc:multicall(Nodes, ?MODULE, open_files, [Name]),
%     lists:flatten(Data);
i(type, _) -> ?MODULE;
i(messages_ram, Q) when ?is_amqqueue(Q) ->
    0;
i(message_bytes_ram, Q) when ?is_amqqueue(Q) ->
    0;
i(_K, _Q) -> ''.

get_nodes(Q) when ?is_amqqueue(Q) ->
    [node(amqqueue:get_pid(Q)) |
     [node(P) || P <- maps:get(replicas, amqqueue:get_type_state(Q))]].

open_files(_Name) ->
    %% TODO: this is a lie
    {node(), 1}.

cluster_state(Name) ->
    case whereis(Name) of
        undefined -> down;
        _ ->
            case ets:lookup(ra_state, Name) of
                [{_, recover}] -> recovering;
                _ -> running
            end
    end.
delete_queue_data(QName, ActingUser) ->
    _ = rabbit_amqqueue:internal_delete(QName, ActingUser),
    ok.
