-module(rabbit_data).

-export([dir/0,
         mnesia_dir/0,
         metadata_dir/0,
         message_store_dir/0,
         vhosts_store_dir/0,
         quorum_queues_dir/0]).
-export([metadata_file/1]).
-export([plugins_expand_dir/0, plugins_dir/0, enabled_plugins_file/0]).

dir() ->
    mnesia_dir().

mnesia_dir() ->
    mnesia:system_info(directory).

metadata_dir() ->
    mnesia_dir().

message_store_dir() ->
    filename:join(mnesia_dir(), "msg_stores").

vhosts_store_dir() ->
    filename:join(message_store_dir(), "vhosts").

quorum_queues_dir() ->
    filename:join(mnesia_dir(), "quorum").

metadata_file(FileName) when is_list(FileName); is_binary(FileName) ->
    filename:join(metadata_dir(), FileName).

upper_level_file(FileName) ->
    filename:join(dir(), FileName).

-spec plugins_expand_dir() -> file:filename().
plugins_expand_dir() ->
    case application:get_env(rabbit, plugins_expand_dir) of
        {ok, ExpandDir} ->
            ExpandDir;
        _ ->
            upper_level_file("plugins_expand_dir")
    end.

-spec plugins_dir() -> file:filename().
plugins_dir() ->
    case application:get_env(rabbit, plugins_dir) of
        {ok, PluginsDistDir} ->
            PluginsDistDir;
        _ ->
            upper_level_file("plugins_dir_stub")
    end.

-spec enabled_plugins_file() -> file:filename().
enabled_plugins_file() ->
     case application:get_env(rabbit, enabled_plugins_file) of
        {ok, Val} ->
            Val;
        _ ->
            upper_level_file("enabled_plugins")
    end.
