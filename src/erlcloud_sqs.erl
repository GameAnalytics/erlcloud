%% Amazon Simple Queue Service (SQS)

-module(erlcloud_sqs).

-export([configure/2, configure/3, new/2, new/3]).

-export([
         add_permission/3, add_permission/4,
         change_message_visibility/3, change_message_visibility/4,
         create_queue/1, create_queue/2, create_queue/3,
         delete_message/2, delete_message/3,
         delete_queue/1, delete_queue/2,
         get_queue_attributes/1, get_queue_attributes/2, get_queue_attributes/3,
         list_queues/0, list_queues/1, list_queues/2,
         receive_message/1, receive_message/2, receive_message/3, receive_message/4,
         receive_message/5, receive_message/6,
         remove_permission/2, remove_permission/3,
         send_message/2, send_message/3, send_message/4,
         set_queue_attributes/2, set_queue_attributes/3
        ]).

-include_lib("erlcloud/include/erlcloud.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-define(API_VERSION, "2012-11-05").

-type(sqs_permission() :: string()).
-type(sqs_acl() :: [{string(), sqs_permission()}]).
-type(sqs_msg_attribute_name() :: string()).
-type(sqs_queue_attribute_name() :: string()).



-spec(new/2 :: (string(), string()) -> aws_config()).
new(AccessKeyID, SecretAccessKey) ->
    #aws_config{access_key_id=AccessKeyID,
                secret_access_key=SecretAccessKey}.

-spec(new/3 :: (string(), string(), string()) -> aws_config()).
new(AccessKeyID, SecretAccessKey, Host) ->
    #aws_config{access_key_id=AccessKeyID,
                secret_access_key=SecretAccessKey,
                sqs_host=Host}.

-spec(configure/2 :: (string(), string()) -> ok).
configure(AccessKeyID, SecretAccessKey) ->
    put(aws_config, new(AccessKeyID, SecretAccessKey)),
    ok.

-spec(configure/3 :: (string(), string(), string()) -> ok).
configure(AccessKeyID, SecretAccessKey, Host) ->
    put(aws_config, new(AccessKeyID, SecretAccessKey, Host)),
    ok.


-spec add_permission/3 :: (string(), string(), sqs_acl()) -> ok.
add_permission(QueueName, Label, Permissions) ->
    add_permission(QueueName, Label, Permissions, default_config()).

-spec add_permission/4 :: (string(), string(), sqs_acl(), aws_config()) -> ok.
add_permission(QueueName, Label, Permissions, Config)
  when is_list(QueueName),
       is_list(Label), length(Label) =< 80,
       is_list(Permissions) ->
    sqs_simple_request(Config, QueueName, "AddPermission",
                       [{"Label", Label} | erlcloud_aws:param_list(
                                             Permissions, {"AWSAccountId", "ActionName"})]).

-spec change_message_visibility/3 :: (string(), string(), 0..43200) -> ok.
change_message_visibility(QueueName, ReceiptHandle, VisibilityTimeout) ->
    change_message_visibility(QueueName, ReceiptHandle, VisibilityTimeout,
                              default_config()).

-spec change_message_visibility/4 :: (string(), string(), 0..43200, aws_config()) -> ok.
change_message_visibility(QueueName, ReceiptHandle, VisibilityTimeout, Config) ->
    sqs_simple_request(Config, QueueName, "ChangeMessageVisibility",
                       [{"ReceiptHandle", ReceiptHandle}, {"VisibilityTimeout", VisibilityTimeout}]).

-spec create_queue/1 :: (string()) -> proplist().
create_queue(QueueName) ->
    create_queue(QueueName, default_config()).

-spec create_queue/2 :: (string(), [{sqs_queue_attribute_name(), string() | integer()}] | aws_config()) -> proplist().
create_queue(QueueName, Config)
  when is_record(Config, aws_config) ->
    create_queue(QueueName, [], Config);
create_queue(QueueName, Attributes) ->
    create_queue(QueueName, Attributes, default_config()).

-spec create_queue/3 :: (string(), [{sqs_queue_attribute_name(), string() | integer()}], aws_config()) -> proplist().
create_queue(QueueName, Attributes, Config)
  when is_list(QueueName), is_list(Attributes) ->
    Params = erlcloud_aws:param_map(Attributes, "Attribute"),
    Doc = sqs_xml_request(Config, "/", "CreateQueue",
                          [{"QueueName", QueueName} | Params]),
    erlcloud_xml:decode(
      [
       {queue_url, "CreateQueueResult/QueueUrl", text}
      ],
      Doc
     ).

-spec delete_message/2 :: (string(), string()) -> ok.
delete_message(QueueName, ReceiptHandle) ->
    delete_message(QueueName, ReceiptHandle, default_config()).

-spec delete_message/3 :: (string(), string(), aws_config()) -> ok.
delete_message(QueueName, ReceiptHandle, Config)
  when is_list(QueueName), is_list(ReceiptHandle) ->
    sqs_simple_request(Config, QueueName, "DeleteMessage",
                       [{"ReceiptHandle", ReceiptHandle}]).

-spec delete_queue/1 :: (string()) -> ok.
delete_queue(QueueName) ->
    delete_queue(QueueName, default_config()).

-spec delete_queue/2 :: (string(), aws_config()) -> ok.
delete_queue(QueueName, Config)
  when is_list(QueueName) ->
    sqs_simple_request(Config, QueueName, "DeleteQueue", []).

-spec get_queue_attributes/1 :: (string()) -> proplist().
get_queue_attributes(QueueName) ->
    get_queue_attributes(QueueName, ["All"]).

-spec get_queue_attributes/2 :: (string(), [sqs_queue_attribute_name()] | aws_config()) -> proplist().
get_queue_attributes(QueueName, Config)
  when is_record(Config, aws_config) ->
    get_queue_attributes(QueueName, ["All"], default_config());
get_queue_attributes(QueueName, AttributeNames) ->
    get_queue_attributes(QueueName, AttributeNames, default_config()).

-spec get_queue_attributes/3 :: (string(), [sqs_queue_attribute_name()], aws_config()) -> proplist().
get_queue_attributes(QueueName, AttributeNames, Config)
  when is_list(QueueName), is_list(AttributeNames) ->
    Doc = sqs_xml_request(Config, QueueName, "GetQueueAttributes",
                          erlcloud_aws:param_list(AttributeNames, "AttributeName")),
    Attrs = decode_attributes(xmerl_xpath:string("GetQueueAttributesResult/Attribute", Doc)),
    [{Name, case Name of
                "Policy"   -> Value;
                "QueueArn" -> Value;
                _          -> list_to_integer(Value)
            end}
     || {Name, Value} <- Attrs].

-spec list_queues/0 :: () -> [string()].
list_queues() ->
    list_queues("").

-spec list_queues/1 :: (string() | aws_config()) -> [string()].
list_queues(Config)
  when is_record(Config, aws_config) ->
    list_queues("", Config);
list_queues(QueueNamePrefix) ->
    list_queues(QueueNamePrefix, default_config()).

-spec list_queues/2 :: (string(), aws_config()) -> [string()].
list_queues(QueueNamePrefix, Config)
  when is_list(QueueNamePrefix) ->
    Doc = case QueueNamePrefix of
              "" -> sqs_xml_request(Config, "/", "ListQueues", []);
              _  -> sqs_xml_request(Config, "/", "ListQueues",
                                    [{"QueueNamePrefix", QueueNamePrefix}])
          end,
    erlcloud_xml:get_list("ListQueuesResult/QueueUrl", Doc).

-spec receive_message/1 :: (string()) -> proplist().
receive_message(QueueName) ->
    receive_message(QueueName, default_config()).

-spec receive_message/2 :: (string(), [sqs_msg_attribute_name()] | aws_config()) -> proplist().
receive_message(QueueName, Config)
  when is_record(Config, aws_config) ->
    receive_message(QueueName, [], Config);
receive_message(QueueName, AttributeNames) ->
    receive_message(QueueName, AttributeNames, default_config()).

-spec receive_message/3 :: (string(), [sqs_msg_attribute_name()], 1..10 | aws_config()) -> proplist().
receive_message(QueueName, AttributeNames, Config)
  when is_record(Config, aws_config) ->
    receive_message(QueueName, AttributeNames, 1, Config);
receive_message(QueueName, AttributeNames, MaxNumberOfMessages) ->
    receive_message(QueueName, AttributeNames, MaxNumberOfMessages, default_config()).

-spec receive_message/4 :: (string(), [sqs_msg_attribute_name()], 1..10, 0..43200 | none | aws_config()) -> proplist().
receive_message(QueueName, AttributeNames, MaxNumberOfMessages, Config)
  when is_record(Config, aws_config) ->
    receive_message(QueueName, AttributeNames, MaxNumberOfMessages, none, Config);
receive_message(QueueName, AttributeNames, MaxNumberOfMessages, VisibilityTimeout) ->
    receive_message(QueueName, AttributeNames, MaxNumberOfMessages,
                    VisibilityTimeout, default_config()).

-spec receive_message/5 :: (string(), [sqs_msg_attribute_name()], 1..10,
                            0..43200 | none, 0..20 | none | aws_config()) -> proplist().
receive_message(QueueName, AttributeNames, MaxNumberOfMessages,
                VisibilityTimeout, Config)
  when is_record(Config, aws_config) ->
    receive_message(QueueName, AttributeNames, MaxNumberOfMessages,
                    VisibilityTimeout, none, Config);
receive_message(QueueName, AttributeNames, MaxNumberOfMessages,
                VisibilityTimeout, WaitTimeSeconds) ->
    receive_message(QueueName, AttributeNames, MaxNumberOfMessages,
                    VisibilityTimeout, WaitTimeSeconds, default_config()).

-spec receive_message/6 :: (string(), [sqs_msg_attribute_name()], 1..10,
                            0..43200 | none, 0..20 | none, aws_config()) -> proplist().
receive_message(QueueName, AttributeNames, MaxNumberOfMessages,
                VisibilityTimeout, WaitTimeSeconds, Config)
  when is_list(AttributeNames),
       MaxNumberOfMessages >= 1, MaxNumberOfMessages =< 10,
       (VisibilityTimeout >= 0 andalso VisibilityTimeout =< 43200) orelse
       VisibilityTimeout =:= none,
       (WaitTimeSeconds >= 0 andalso WaitTimeSeconds =< 20) orelse
       WaitTimeSeconds =:= none ->
    TotalTimeout = if (WaitTimeSeconds =/= none andalso WaitTimeSeconds >= 0) ->
                          Config#aws_config.timeout + (WaitTimeSeconds * 1000) ;
                      true ->
                          Config#aws_config.timeout
                   end,
    Params = [{K, V} || {K, V} <- [{"MaxNumberOfMessages", MaxNumberOfMessages},
                                   {"VisibilityTimeout", VisibilityTimeout},
                                   {"WaitTimeSeconds", WaitTimeSeconds}|
                                   erlcloud_aws:param_list(AttributeNames, "AttributeName")],
                        V =/= none],
    Doc = sqs_xml_request(Config#aws_config{timeout=TotalTimeout}, QueueName,
                          "ReceiveMessage", Params),
    erlcloud_xml:decode(
      [
       {messages, "ReceiveMessageResult/Message", fun decode_messages/1}
      ],
      Doc
     ).


decode_messages(Messages) ->
    [decode_message(Message) || Message <- Messages].

decode_message(Message) ->
    erlcloud_xml:decode(
      [
       {body, "Body", text},
       {md5_of_body, "MD5OfBody", text},
       {message_id, "MessageId", text},
       {receipt_handle, "ReceiptHandle", text},
       {attributes, "Attribute", fun decode_msg_attributes/1}
      ],
      Message
     ).

decode_msg_attributes(Attrs)  ->
    [{Name, case Name of
                "SenderId" -> Value;
                _ -> list_to_integer(Value)
            end}
     || {Name, Value} <- decode_attributes(Attrs)].

decode_attributes(Attrs) ->
    [{erlcloud_xml:get_text("Name", Attr), erlcloud_xml:get_text("Value", Attr)}
     || Attr <- Attrs].

-spec remove_permission/2 :: (string(), string()) -> ok.
remove_permission(QueueName, Label) ->
    remove_permission(QueueName, Label, default_config()).

-spec remove_permission/3 :: (string(), string(), aws_config()) -> ok.
remove_permission(QueueName, Label, Config)
  when is_list(QueueName), is_list(Label) ->
    sqs_simple_request(Config, QueueName, "RemovePermission",
                       [{"Label", Label}]).

-spec send_message/2 :: (string(), string()) -> proplist().
send_message(QueueName, MessageBody) ->
    send_message(QueueName, MessageBody, default_config()).

-spec send_message/3 :: (string(), string(), 0..900 | none | aws_config()) -> proplist().
send_message(QueueName, MessageBody, Config)
  when is_record(Config, aws_config) ->
    send_message(QueueName, MessageBody, none, Config);
send_message(QueueName, MessageBody, DelaySeconds) ->
    send_message(QueueName, MessageBody, DelaySeconds, default_config()).

-spec send_message/4 :: (string(), string(), 0..900 | none, aws_config()) -> proplist().
send_message(QueueName, MessageBody, DelaySeconds, Config)
  when is_list(QueueName), (is_list(MessageBody) orelse is_binary(MessageBody)),
       ((DelaySeconds >= 0 andalso DelaySeconds =< 900) orelse
        DelaySeconds =:= none) ->
    Params = [{K, V} || {K, V} <- [{"MessageBody", MessageBody},
                                   {"DelaySeconds", DelaySeconds}],
                        V =/= none],
    Doc = sqs_xml_request(Config, QueueName, "SendMessage", Params),
    erlcloud_xml:decode(
      [
       {message_id, "SendMessageResult/MessageId", text},
       {md5_of_message_body, "SendMessageResult/MD5OfMessageBody", text}
      ],
      Doc
     ).

-spec set_queue_attributes/2 :: (string(), [{sqs_queue_attribute_name(), string() | integer()}]) -> ok.
set_queue_attributes(QueueName, Attributes) ->
    set_queue_attributes(QueueName, Attributes, default_config()).

-spec set_queue_attributes/3 :: (string(), [{sqs_queue_attribute_name(), string() | integer()}], aws_config()) -> ok.
set_queue_attributes(QueueName, Attributes, Config)
  when is_list(QueueName), is_list(Attributes) ->
    Params = erlcloud_aws:param_map(Attributes, "Attribute"),
    sqs_simple_request(Config, QueueName, "SetQueueAttributes", Params).

default_config() -> erlcloud_aws:default_config().

sqs_simple_request(Config, QueueName, Action, Params) ->
    sqs_request(Config, QueueName, Action, Params),
    ok.

sqs_xml_request(Config, QueueName, Action, Params) ->
    erlcloud_aws:aws_request_xml(post, Config#aws_config.sqs_protocol,
                                 Config#aws_config.sqs_host, Config#aws_config.sqs_port,
                                 queue_path(QueueName), [{"Action", Action}, {"Version", ?API_VERSION}|Params], Config).

sqs_request(Config, QueueName, Action, Params) ->
    erlcloud_aws:aws_request(post, Config#aws_config.sqs_protocol,
                             Config#aws_config.sqs_host, Config#aws_config.sqs_port,
                             queue_path(QueueName), [{"Action", Action}, {"Version", ?API_VERSION}|Params], Config).

queue_path([$/|_] = QueueName) -> QueueName;
queue_path([$h,$t,$t,$p|_] = URL) ->
    re:replace(URL, "^https?://[^/]*", "", [{return, list}]);
queue_path(QueueName) -> [$/|QueueName].
