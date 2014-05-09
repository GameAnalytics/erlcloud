-module(erlcloud_sqs_tests).
-include_lib("eunit/include/eunit.hrl").

erlcloud_sqs_test_() ->
    {foreach,
     fun setup/0, fun teardown/1,
     [?_test(add_permission()),
      ?_test(change_message_visibility()),
      ?_test(create_queue()),
      ?_test(delete_message()),
      ?_test(get_queue_attributes_all()),
      ?_test(get_queue_attributes()),
      ?_test(list_queues()),
      ?_test(receive_message()),
      ?_test(remove_permission()),
      ?_test(send_message()),
      ?_test(set_queue_attributes())
     ]
    }.

setup() ->
    meck:new(erlcloud_aws, [passthrough]),
    erlcloud_sqs:configure(string:copies("A", 20), string:copies("a", 40)),
    ok.

teardown(_) ->
    ?assert(meck:validate(erlcloud_aws)),
    meck:unload(erlcloud_aws).


add_permission() ->
    ExampleResponse = <<"<AddPermissionResponse>
                             <ResponseMetadata>
                                 <RequestId>
                                     9a285199-c8d6-47c2-bdb2-314cb47d599d
                                 </RequestId>
                             </ResponseMetadata>
                         </AddPermissionResponse>">>,

    QueueName = "testQueue",
    Label = "testLabel",
    Permissions = [{"125074342641", "SendMessage"},
                   {"125074342642", "ReceiveMessage"},
                   {"1234567890", "*"}],

    meck:expect(erlcloud_aws, aws_request, 5, ExampleResponse),

    ?assertEqual(ok,
                 erlcloud_sqs:add_permission(QueueName, Label, Permissions)),

    ?assert(meck:called(erlcloud_aws, aws_request,
                        [post, "queue.amazonaws.com",
                         "/" ++ QueueName,
                         [{"Action","AddPermission"},
                          {"Version","2012-11-05"},
                          {"Label","testLabel"},
                          {"AWSAccountId.1","125074342641"},
                          {"ActionName.1","SendMessage"},
                          {"AWSAccountId.2","125074342642"},
                          {"ActionName.2","ReceiveMessage"},
                          {"AWSAccountId.3","1234567890"},
                          {"ActionName.3","*"}],
                         '_'])),
    ok.

change_message_visibility() ->
    ExampleResponse = <<"<ChangeMessageVisibilityResponse>
                               <ResponseMetadata>
                                   <RequestId>
                                       6a7a282a-d013-4a59-aba9-335b0fa48bed
                                   </RequestId>
                               </ResponseMetadata>
                           </ChangeMessageVisibilityResponse>">>,

    QueueName = "testQueue",
    ReceiptHandle = "MbZj6wDWli%2BJvwwJaBV%2B3dcjk2YW2vA3%2BSTFFljTM8tJJg6HRG6PYSasuWXPJB%2BCwLj1FjgXUv1uSj1gUPAWV66FU/WeR4mq2OKpEGYWbnLmpRCJVAyeMjeU5ZBdtcQ%2BQEauMZc8ZRv37sIW2iJKq3M9MFx1YvV11A2x/KSbkJ0=",
    VisibilityTimeout = 60,

    meck:expect(erlcloud_aws, aws_request, 5, ExampleResponse),

    ?assertEqual(ok,
                 erlcloud_sqs:change_message_visibility(QueueName,
                                                        ReceiptHandle,
                                                        VisibilityTimeout)),

    ?assert(meck:called(erlcloud_aws, aws_request,
                        [post, "queue.amazonaws.com",
                         "/" ++ QueueName,
                         [{"Action","ChangeMessageVisibility"},
                          {"Version","2012-11-05"},
                          {"ReceiptHandle",ReceiptHandle},
                          {"VisibilityTimeout",VisibilityTimeout}],
                         '_'])),
    ok.

create_queue() ->
    ExampleResponse = "<CreateQueueResponse>
                            <CreateQueueResult>
                                <QueueUrl>http://sqs.us-east-1.amazonaws.com/123456789012/testQueue</QueueUrl>
                            </CreateQueueResult>
                            <ResponseMetadata>
                                <RequestId>
                                    7a62c49f-347e-4fc4-9331-6e8e7a96aa73
                                </RequestId>
                            </ResponseMetadata>
                        </CreateQueueResponse>",

    QueueName = "testQueue",
    Attributes = [{"VisibilityTimeout", VisibilityTimeout=40}],

    meck:expect(erlcloud_aws, aws_request_xml, 5,
                element(1, xmerl_scan:string(ExampleResponse))),

    Response = erlcloud_sqs:create_queue(QueueName, Attributes),
    ?assertEqual("http://sqs.us-east-1.amazonaws.com/123456789012/testQueue",
                 proplists:get_value(queue_url, Response)),

    ?assert(meck:called(erlcloud_aws, aws_request_xml,
                        [post, "queue.amazonaws.com",
                         "/",
                         [{"Action","CreateQueue"},
                          {"Version","2012-11-05"},
                          {"QueueName",QueueName},
                          {"Attribute.1.Name","VisibilityTimeout"},
                          {"Attribute.1.Value",VisibilityTimeout}],
                         '_'])),
    ok.

delete_message() ->
    ExampleResponse = "<DeleteMessageResponse>
                            <ResponseMetadata>
                                <RequestId>
                                    b5293cb5-d306-4a17-9048-b263635abe42
                                </RequestId>
                            </ResponseMetadata>
                        </DeleteMessageResponse>",

    QueueName = "testQueue",
    ReceiptHandle = "MbZj6wDWli%2BJvwwJaBV%2B3dcjk2YW2vA3%2BSTFFljTM8tJJg6HRG6PYSasuWXPJB%2BCwLj1FjgXUv1uSj1gUPAWV66FU/WeR4mq2OKpEGYWbnLmpRCJVAyeMjeU5ZBdtcQ%2BQEauMZc8ZRv37sIW2iJKq3M9MFx1YvV11A2x/KSbkJ0=",

    meck:expect(erlcloud_aws, aws_request, 5, ExampleResponse),

    ?assertEqual(ok, erlcloud_sqs:delete_message(QueueName, ReceiptHandle)),

    ?assert(meck:called(erlcloud_aws, aws_request,
                        [post, "queue.amazonaws.com",
                         "/" ++ QueueName,
                         [{"Action","DeleteMessage"},
                          {"Version","2012-11-05"},
                          {"ReceiptHandle",ReceiptHandle}],
                         '_'])),
    ok.

get_queue_attributes_all() ->
    ExampleResponse = "<GetQueueAttributesResponse>
                          <GetQueueAttributesResult>
                            <Attribute>
                              <Name>ReceiveMessageWaitTimeSeconds</Name>
                              <Value>2</Value>
                            </Attribute>
                            <Attribute>
                              <Name>VisibilityTimeout</Name>
                              <Value>30</Value>
                            </Attribute>
                            <Attribute>
                              <Name>ApproximateNumberOfMessages</Name>
                              <Value>0</Value>
                            </Attribute>
                            <Attribute>
                              <Name>ApproximateNumberOfMessagesNotVisible</Name>
                              <Value>0</Value>
                            </Attribute>
                            <Attribute>
                              <Name>CreatedTimestamp</Name>
                              <Value>1286771522</Value>
                            </Attribute>
                            <Attribute>
                              <Name>LastModifiedTimestamp</Name>
                              <Value>1286771522</Value>
                            </Attribute>
                            <Attribute>
                              <Name>QueueArn</Name>
                              <Value>arn:aws:sqs:us-east-1:123456789012:qfoo</Value>
                            </Attribute>
                            <Attribute>
                              <Name>MaximumMessageSize</Name>
                              <Value>8192</Value>
                            </Attribute>
                            <Attribute>
                              <Name>MessageRetentionPeriod</Name>
                              <Value>345600</Value>
                            </Attribute>
                          </GetQueueAttributesResult>
                          <ResponseMetadata>
                            <RequestId>1ea71be5-b5a2-4f9d-b85a-945d8d08cd0b</RequestId>
                          </ResponseMetadata>
                        </GetQueueAttributesResponse>",

    QueueName = "testQueue",

    meck:expect(erlcloud_aws, aws_request_xml, 5,
                element(1, xmerl_scan:string(ExampleResponse))),

    Response = erlcloud_sqs:get_queue_attributes(QueueName),
    ?assertEqual([{"ReceiveMessageWaitTimeSeconds", 2},
                  {"VisibilityTimeout", 30},
                  {"ApproximateNumberOfMessages", 0},
                  {"ApproximateNumberOfMessagesNotVisible", 0},
                  {"CreatedTimestamp", 1286771522},
                  {"LastModifiedTimestamp", 1286771522},
                  {"QueueArn", "arn:aws:sqs:us-east-1:123456789012:qfoo"},
                  {"MaximumMessageSize", 8192},
                  {"MessageRetentionPeriod", 345600}],
                 Response),

    ?assert(meck:called(erlcloud_aws, aws_request_xml,
                        [post, "queue.amazonaws.com",
                         "/" ++ QueueName,
                         [{"Action","GetQueueAttributes"},
                          {"Version","2012-11-05"},
                          {"AttributeName.1","All"}],
                         '_'])),
    ok.

get_queue_attributes() ->
    ExampleResponse = "<GetQueueAttributesResponse>
                          <GetQueueAttributesResult>
                            <Attribute>
                              <Name>VisibilityTimeout</Name>
                              <Value>30</Value>
                            </Attribute>
                            <Attribute>
                              <Name>DelaySeconds</Name>
                              <Value>0</Value>
                            </Attribute>
                            <Attribute>
                              <Name>ReceiveMessageWaitTimeSeconds</Name>
                              <Value>2</Value>
                            </Attribute>
                          </GetQueueAttributesResult>
                          <ResponseMetadata>
                            <RequestId>1ea71be5-b5a2-4f9d-b85a-945d8d08cd0b</RequestId>
                          </ResponseMetadata>
                        </GetQueueAttributesResponse>",

    QueueName = "testQueue",
    Attributes = ["VisibilityTimeout", "DelaySeconds", "ReceiveMessageWaitTimeSeconds"],

    meck:expect(erlcloud_aws, aws_request_xml, 5,
                element(1, xmerl_scan:string(ExampleResponse))),

    Response = erlcloud_sqs:get_queue_attributes(QueueName, Attributes),
    ?assertEqual([{"VisibilityTimeout", 30},
                  {"DelaySeconds", 0},
                  {"ReceiveMessageWaitTimeSeconds", 2}],
                 Response),

    ?assert(meck:called(erlcloud_aws, aws_request_xml,
                        [post, "queue.amazonaws.com",
                         "/" ++ QueueName,
                         [{"Action","GetQueueAttributes"},
                          {"Version","2012-11-05"},
                          {"AttributeName.1","VisibilityTimeout"},
                          {"AttributeName.2","DelaySeconds"},
                          {"AttributeName.3","ReceiveMessageWaitTimeSeconds"}],
                         '_'])),
    ok.

list_queues() ->
    ExampleResponse = "<ListQueuesResponse>
                            <ListQueuesResult>
                                <QueueUrl>http://sqs.us-east-1.amazonaws.com/123456789012/testQueue</QueueUrl>
                            </ListQueuesResult>
                            <ResponseMetadata>
                                <RequestId>
                                    725275ae-0b9b-4762-b238-436d7c65a1ac
                                </RequestId>
                            </ResponseMetadata>
                        </ListQueuesResponse>",

    Prefix = "t",

    meck:expect(erlcloud_aws, aws_request_xml, 5,
                element(1, xmerl_scan:string(ExampleResponse))),

    Response = erlcloud_sqs:list_queues(Prefix),
    ?assertEqual(["http://sqs.us-east-1.amazonaws.com/123456789012/testQueue"],
                 Response),

    ?assert(meck:called(erlcloud_aws, aws_request_xml,
                        [post, "queue.amazonaws.com",
                         "/",
                         [{"Action","ListQueues"},
                          {"Version","2012-11-05"},
                          {"QueueNamePrefix",Prefix}],
                         '_'])),


    Response = erlcloud_sqs:list_queues(),
    ?assertEqual(["http://sqs.us-east-1.amazonaws.com/123456789012/testQueue"],
                 Response),

    ?assert(meck:called(erlcloud_aws, aws_request_xml,
                        [post, "queue.amazonaws.com",
                         "/",
                         [{"Action","ListQueues"},
                          {"Version","2012-11-05"}],
                         '_'])),

    ok.

receive_message() ->
    ExampleResponse = "<ReceiveMessageResponse>
                          <ReceiveMessageResult>
                            <Message>
                              <MessageId>5fea7756-0ea4-451a-a703-a558b933e274</MessageId>
                              <ReceiptHandle>MbZj6wDWli+JvwwJaBV+3dcjk2YW2vA3+STFFljTM8tJJg6HRG6PYSasuWXPJB+CwLj1FjgXUv1uSj1gUPAWV66FU/WeR4mq2OKpEGYWbnLmpRCJVAyeMjeU5ZBdtcQ+QEauMZc8ZRv37sIW2iJKq3M9MFx1YvV11A2x/KSbkJ0=</ReceiptHandle>
                              <MD5OfBody>fafb00f5732ab283681e124bf8747ed1</MD5OfBody>
                              <Body>This is a test message</Body>
                              <Attribute>
                                <Name>SenderId</Name>
                                <Value>195004372649</Value>
                              </Attribute>
                              <Attribute>
                                <Name>SentTimestamp</Name>
                                <Value>1238099229000</Value>
                              </Attribute>
                              <Attribute>
                                <Name>ApproximateReceiveCount</Name>
                                <Value>5</Value>
                              </Attribute>
                              <Attribute>
                                <Name>ApproximateFirstReceiveTimestamp</Name>
                                <Value>1250700979248</Value>
                              </Attribute>
                            </Message>
                          </ReceiveMessageResult>
                          <ResponseMetadata>
                            <RequestId>
                              b6633655-283d-45b4-aee4-4e84e0ae6afa
                            </RequestId>
                          </ResponseMetadata>
                        </ReceiveMessageResponse>",

    QueueName = "testQueue",
    MaxNumberOfMessages = 5,
    VisibilityTimeout = 15,
    AttributeNames = ["All"],

    meck:expect(erlcloud_aws, aws_request_xml, 5,
                element(1, xmerl_scan:string(ExampleResponse))),

    Response = erlcloud_sqs:receive_message(
                 QueueName, AttributeNames, MaxNumberOfMessages, VisibilityTimeout),
    ?assertEqual([{messages, [[{body, "This is a test message"},
                              {md5_of_body, "fafb00f5732ab283681e124bf8747ed1"}, 
                              {message_id, "5fea7756-0ea4-451a-a703-a558b933e274"}, 
                              {receipt_handle, "MbZj6wDWli+JvwwJaBV+3dcjk2YW2vA3+STFFljTM8tJJg6HRG6PYSasuWXPJB+CwLj1FjgXUv1uSj1gUPAWV66FU/WeR4mq2OKpEGYWbnLmpRCJVAyeMjeU5ZBdtcQ+QEauMZc8ZRv37sIW2iJKq3M9MFx1YvV11A2x/KSbkJ0="}, 
                              {attributes, [{"SenderId", "195004372649"},
                                           {"SentTimestamp", 1238099229000},
                                           {"ApproximateReceiveCount", 5},
                                           {"ApproximateFirstReceiveTimestamp", 
                                            1250700979248}]}]]}],
                 Response),

    ?assert(meck:called(erlcloud_aws, aws_request_xml,
                        [post, "queue.amazonaws.com",
                         "/" ++ QueueName,
                         [{"Action","ReceiveMessage"},
                          {"Version","2012-11-05"},
                          {"MaxNumberOfMessages",MaxNumberOfMessages},
                          {"VisibilityTimeout",VisibilityTimeout},
                          {"AttributeName.1","All"}],
                         '_'])),
    ok.

remove_permission() ->
    ExampleResponse = "<RemovePermissionResponse>
                            <ResponseMetadata>
                                <RequestId>
                                    f8bdb362-6616-42c0-977a-ce9a8bcce3bb
                                </RequestId>
                            </ResponseMetadata>
                        </RemovePermissionResponse>",

    QueueName = "testQueue",
    Label = "testLabel",

    meck:expect(erlcloud_aws, aws_request, 5, ExampleResponse),

    ?assertEqual(ok,
                 erlcloud_sqs:remove_permission(QueueName, Label)),

    ?assert(meck:called(erlcloud_aws, aws_request,
                        [post, "queue.amazonaws.com",
                         "/" ++ QueueName,
                         [{"Action","RemovePermission"},
                          {"Version","2012-11-05"},
                          {"Label","testLabel"}],
                         '_'])),
    ok.

send_message() ->
    ExampleResponse = "<SendMessageResponse>
                        <SendMessageResult>
                            <MD5OfMessageBody>fafb00f5732ab283681e124bf8747ed1</MD5OfMessageBody>
                            <MD5OfMessageAttributes>3ae8f24a165a8cedc005670c81a27295</MD5OfMessageAttributes>
                            <MessageId>5fea7756-0ea4-451a-a703-a558b933e274</MessageId>
                        </SendMessageResult>
                        <ResponseMetadata>
                            <RequestId>
                                27daac76-34dd-47df-bd01-1f6e873584a0
                            </RequestId>
                        </ResponseMetadata>
                    </SendMessageResponse>",

    QueueName = "testQueue",
    MessageBody = "This is a test message",

    meck:expect(erlcloud_aws, aws_request_xml, 5,
                element(1, xmerl_scan:string(ExampleResponse))),

    ?assertEqual([{message_id, "5fea7756-0ea4-451a-a703-a558b933e274"},
                  {md5_of_message_body, "fafb00f5732ab283681e124bf8747ed1"}],
                 erlcloud_sqs:send_message(QueueName, MessageBody)),

    ?assert(meck:called(erlcloud_aws, aws_request_xml,
                        [post, "queue.amazonaws.com",
                         "/" ++ QueueName,
                         [{"Action","SendMessage"},
                          {"Version","2012-11-05"},
                          {"MessageBody",MessageBody}],
                         '_'])),
    ok.

set_queue_attributes() ->
    ExampleResponse = "<SetQueueAttributesResponse>
                            <ResponseMetadata>
                                <RequestId>
                                    e5cca473-4fc0-4198-a451-8abb94d02c75
                                </RequestId>
                            </ResponseMetadata>
                        </SetQueueAttributesResponse>",

    QueueName = "testQueue",
    Attributes = [{"VisibilityTimeout", 35}],

    meck:expect(erlcloud_aws, aws_request, 5, ExampleResponse),

    ?assertEqual(ok, erlcloud_sqs:set_queue_attributes(QueueName, Attributes)),

    ?assert(meck:called(erlcloud_aws, aws_request,
                        [post, "queue.amazonaws.com",
                         "/" ++ QueueName,
                         [{"Action","SetQueueAttributes"},
                          {"Version","2012-11-05"},
                          {"Attribute.1.Name","VisibilityTimeout"},
                          {"Attribute.1.Value",35}],
                         '_'])),
    ok.
