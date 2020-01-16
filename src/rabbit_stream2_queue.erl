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
         cluster_state/1


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
                    osiris_writer:register_offset_listener(LeaderPid, NextOffset),
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
                    % rabbit_log:info("stream entries out ~w ~w", [Entries0, Msgs]),
                    {Str, MsgIn ++ Msgs};
                false ->
                    %% if there are fewer Msgs than Entries0 it means there were non-events
                    %% in the log and we should recurse and try again
                    stream_entries(Name, LeaderPid, Str, MsgIn ++ Msgs)
            end
    end;
stream_entries(_Name, _Id, Str, Msgs) ->
    % rabbit_log:info("stream entries none ~w", [Str]),
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
            osiris_writer:register_offset_listener(Leader, NextOffset),
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
    % rabbit_log:info("handle_offset ~w", [_Offs]),
    %% offset isn't actually needed as we use the atomic to read the
    %% current committed
    Readers = maps:map(
                fun (Tag, Str0) ->
                        {Str, Msgs} = stream_entries(Name, Leader, Str0),
                        %% HACK for now, better to just return but
                        %% tricky with acks credits
                        %% that also evaluate the stream
                        gen_server:cast(self(), {stream_delivery, Tag, Msgs}),
                        Str
                end, Readers0),
    State#stream2_client{readers = Readers}.


credit(#stream2_client{name = Name,
                       leader = Leader,
                       readers = Readers0} = State, Tag, Credit) ->
    % rabbit_log:info("stream2 credit ~w ~w", [Credit, Tag]),
    Readers = case Readers0 of
                  #{Tag := #stream{credit = Credit0} = Str0} ->
                      % rabbit_log:info("stream2 credit yeah ~w ~w", [Credit, Tag]),
                      Str1 = Str0#stream{credit = Credit0 + Credit},
                      {Str, Msgs} = stream_entries(Name, Leader, Str1),
                      %% HACK for now, better to just return but
                      %% tricky with acks credits
                      %% that also evaluate the stream
                      gen_server:cast(self(), {stream_delivery, Tag, Msgs}),
                      Readers0#{Tag => Str};
                  _ ->
                      Readers0
              end,
    {ok, State#stream2_client{readers = Readers}}.

%% MGMT

declare(Q0) ->
    QName = amqqueue:get_name(Q0),
    Name = qname_to_rname(QName),
    Arguments = amqqueue:get_arguments(Q0),
    Opts = amqqueue:get_options(Q0),
    ActingUser = maps:get(user, Opts, ?UNKNOWN_USER),
    Replicas = rabbit_mnesia:cluster_nodes(all) -- [node()],
    N = ra_lib:derive_safe_string(atom_to_list(Name), 8),
    Dir = filename:join(rabbit_mnesia:dir(), "streams"),
    file:make_dir(Dir),
    % rabbit_log:info("Declare stream2 in ~s", [Dir]),
    Conf = #{dir => Dir,
             reference => QName},
    {ok, LeaderPid, ReplicaPids} = osiris:start_cluster(N, Replicas, Conf),
    Q1 = amqqueue:set_slave_pids(
           amqqueue:set_pid(Q0, LeaderPid), ReplicaPids),
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
    end.

delete(Q, ActingUser) when ?amqqueue_is_stream2(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    QName = amqqueue:get_name(Q),
    QNodes = get_nodes(Q),
    Timeout = ?DELETE_TIMEOUT,
    {ok, ReadyMsgs, _} = stat(Q),
    Servers = [{Name, Node} || Node <- QNodes],
    case ra:delete_cluster(Servers, Timeout) of
        {ok, {_, LeaderNode} = Leader} ->
            MRef = erlang:monitor(process, Leader),
            receive
                {'DOWN', MRef, process, _, _} ->
                    ok
            after Timeout ->
                    ok = rabbit_ra_queue:force_delete(Servers)
            end,
            ok = delete_queue_data(QName, ActingUser),
            rpc:call(LeaderNode, rabbit_core_metrics, queue_deleted, [QName],
                     1000),
            {ok, ReadyMsgs};
        {error, {no_more_servers_to_try, Errs}} ->
            case lists:all(fun({{error, noproc}, _}) -> true;
                              (_) -> false
                           end, Errs) of
                true ->
                    %% If all ra nodes were already down, the delete
                    %% has succeed
                    delete_queue_data(QName, ActingUser),
                    {ok, ReadyMsgs};
                false ->
                    %% attempt forced deletion of all servers
                    rabbit_log:warning(
                      "Could not delete quorum queue '~s', not enough nodes "
                       " online to reach a quorum: ~255p."
                       " Attempting force delete.",
                      [rabbit_misc:rs(QName), Errs]),
                    ok = rabbit_ra_queue:force_delete(Servers),
                    delete_queue_data(QName, ActingUser),
                    {ok, ReadyMsgs}
            end
    end.
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
    {Name, _} = amqqueue:get_pid(Q),
    whereis(Name);
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
    {Name, _} = amqqueue:get_pid(Q),
    try
        {memory, M} = process_info(whereis(Name), memory),
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
    {Name, _} = amqqueue:get_pid(Q),
    try
        rabbit_misc:get_gc_info(whereis(Name))
    catch
        error:badarg ->
            []
    end;
i(members, Q) when ?is_amqqueue(Q) ->
    get_nodes(Q);
i(online, Q) ->
    get_nodes(Q);
i(leader, Q) ->
    {_Name, Leader} = amqqueue:get_pid(Q),
    Leader;
i(open_files, Q) when ?is_amqqueue(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    Nodes = get_nodes(Q),
    {Data, _} = rpc:multicall(Nodes, ?MODULE, open_files, [Name]),
    lists:flatten(Data);
i(single_active_consumer_pid, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    {ok, {_, SacResult}, _} = ra:local_query(QPid,
                                             fun rabbit_fifo:query_single_active_consumer/1),
    case SacResult of
        {value, {_ConsumerTag, ChPid}} ->
            ChPid;
        _ ->
            ''
    end;
i(single_active_consumer_ctag, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    {ok, {_, SacResult}, _} = ra:local_query(QPid,
                                             fun rabbit_fifo:query_single_active_consumer/1),
    case SacResult of
        {value, {ConsumerTag, _ChPid}} ->
            ConsumerTag;
        _ ->
            ''
    end;
i(type, _) -> quorum;
i(messages_ram, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    {ok, {_, {Length, _}}, _} = ra:local_query(QPid,
                                          fun rabbit_fifo:query_in_memory_usage/1),
    Length;
i(message_bytes_ram, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    {ok, {_, {_, Bytes}}, _} = ra:local_query(QPid,
                                         fun rabbit_fifo:query_in_memory_usage/1),
    Bytes;
i(_K, _Q) -> ''.

get_nodes(Q) when ?is_amqqueue(Q) ->
    [node(amqqueue:get_pid(Q)) |
     [node(P) || P <- amqqueue:get_slave_pids(Q)]].

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

stat(_Q) ->
    {ok, 0, 0}.
