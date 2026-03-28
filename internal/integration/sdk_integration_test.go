//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	smithy "github.com/aws/smithy-go"

	"emulator-aws-sns/internal/testsupport"
)

func TestSDKTopicLifecycleAndTags(t *testing.T) {
	ctx := context.Background()
	h := testsupport.NewHarness(t)
	client := h.SNSClient(ctx)

	created, err := client.CreateTopic(ctx, &snssdk.CreateTopicInput{Name: aws.String("sdk-topic")})
	if err != nil {
		t.Fatalf("CreateTopic(standard) error = %v", err)
	}
	fifo, err := client.CreateTopic(ctx, &snssdk.CreateTopicInput{
		Name: aws.String("sdk-orders.fifo"),
		Attributes: map[string]string{
			"FifoTopic":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic(fifo) error = %v", err)
	}
	attrs, err := client.GetTopicAttributes(ctx, &snssdk.GetTopicAttributesInput{TopicArn: created.TopicArn})
	if err != nil {
		t.Fatalf("GetTopicAttributes(standard) error = %v", err)
	}
	if attrs.Attributes["TopicArn"] != aws.ToString(created.TopicArn) {
		t.Fatalf("unexpected TopicArn attribute: %#v", attrs.Attributes)
	}
	if _, err := client.SetTopicAttributes(ctx, &snssdk.SetTopicAttributesInput{
		TopicArn:       fifo.TopicArn,
		AttributeName:  aws.String("SignatureVersion"),
		AttributeValue: aws.String("2"),
	}); err != nil {
		t.Fatalf("SetTopicAttributes() error = %v", err)
	}
	fifoAttrs, err := client.GetTopicAttributes(ctx, &snssdk.GetTopicAttributesInput{TopicArn: fifo.TopicArn})
	if err != nil {
		t.Fatalf("GetTopicAttributes(fifo) error = %v", err)
	}
	if got := fifoAttrs.Attributes["SignatureVersion"]; got != "2" {
		t.Fatalf("expected SignatureVersion=2, got %q", got)
	}
	if got := fifoAttrs.Attributes["FifoTopic"]; got != "true" {
		t.Fatalf("expected FifoTopic=true, got %q", got)
	}

	var listed []string
	paginator := snssdk.NewListTopicsPaginator(client, &snssdk.ListTopicsInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			t.Fatalf("ListTopics paginator error = %v", err)
		}
		for _, topic := range page.Topics {
			listed = append(listed, aws.ToString(topic.TopicArn))
		}
	}
	sort.Strings(listed)
	if !contains(listed, aws.ToString(created.TopicArn)) || !contains(listed, aws.ToString(fifo.TopicArn)) {
		t.Fatalf("expected created topics in ListTopics output: %#v", listed)
	}

	_, err = client.TagResource(ctx, &snssdk.TagResourceInput{
		ResourceArn: created.TopicArn,
		Tags: []snstypes.Tag{
			{Key: aws.String("team"), Value: aws.String("sdk")},
			{Key: aws.String("env"), Value: aws.String("test")},
		},
	})
	if err != nil {
		t.Fatalf("TagResource() error = %v", err)
	}
	tagged, err := client.ListTagsForResource(ctx, &snssdk.ListTagsForResourceInput{ResourceArn: created.TopicArn})
	if err != nil {
		t.Fatalf("ListTagsForResource() error = %v", err)
	}
	if len(tagged.Tags) != 2 {
		t.Fatalf("expected 2 tags, got %d", len(tagged.Tags))
	}
	_, err = client.UntagResource(ctx, &snssdk.UntagResourceInput{
		ResourceArn: created.TopicArn,
		TagKeys:     []string{"env"},
	})
	if err != nil {
		t.Fatalf("UntagResource() error = %v", err)
	}
	tagged, err = client.ListTagsForResource(ctx, &snssdk.ListTagsForResourceInput{ResourceArn: created.TopicArn})
	if err != nil {
		t.Fatalf("ListTagsForResource(after untag) error = %v", err)
	}
	if len(tagged.Tags) != 1 || aws.ToString(tagged.Tags[0].Key) != "team" {
		t.Fatalf("unexpected tags after untag: %#v", tagged.Tags)
	}

	_, err = client.CreateTopic(ctx, &snssdk.CreateTopicInput{Name: aws.String("bad topic name")})
	assertAPIErrorCode(t, err, "InvalidParameter")
	_, err = client.CreateTopic(ctx, &snssdk.CreateTopicInput{
		Name: aws.String("notfifo"),
		Attributes: map[string]string{
			"FifoTopic": "true",
		},
	})
	assertAPIErrorCode(t, err, "InvalidParameter")

	_, _ = client.DeleteTopic(ctx, &snssdk.DeleteTopicInput{TopicArn: created.TopicArn})
	_, _ = client.DeleteTopic(ctx, &snssdk.DeleteTopicInput{TopicArn: fifo.TopicArn})
}

func TestSDKHTTPAndHTTPSSubscriptions(t *testing.T) {
	ctx := context.Background()
	h := testsupport.NewHarness(t)
	client := h.SNSClient(ctx)

	topic, err := client.CreateTopic(ctx, &snssdk.CreateTopicInput{Name: aws.String("http-topic")})
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}

	httpHook := testsupport.NewWebhookCapture(t, false)
	sub, err := client.Subscribe(ctx, &snssdk.SubscribeInput{
		TopicArn:              topic.TopicArn,
		Protocol:              aws.String("http"),
		Endpoint:              aws.String(httpHook.URL()),
		ReturnSubscriptionArn: true,
	})
	if err != nil {
		t.Fatalf("Subscribe(http) error = %v", err)
	}
	if aws.ToString(sub.SubscriptionArn) == "" {
		t.Fatalf("expected http subscription ARN")
	}

	if _, err := client.Publish(ctx, &snssdk.PublishInput{
		TopicArn: topic.TopicArn,
		Message:  aws.String("before-confirm"),
	}); err != nil {
		t.Fatalf("Publish(before confirm) error = %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if len(httpHook.Events()) != 1 {
		t.Fatalf("expected only confirmation event before confirm, got %d", len(httpHook.Events()))
	}

	confirmEvent, err := httpHook.WaitForType(timeoutContext(t), "SubscriptionConfirmation")
	if err != nil {
		t.Fatalf("WaitForType(SubscriptionConfirmation) error = %v", err)
	}
	token := stringField(confirmEvent.JSON, "Token")
	if _, err := client.ConfirmSubscription(ctx, &snssdk.ConfirmSubscriptionInput{
		TopicArn: topic.TopicArn,
		Token:    aws.String(token),
	}); err != nil {
		t.Fatalf("ConfirmSubscription(http) error = %v", err)
	}

	subAttrs, err := client.GetSubscriptionAttributes(ctx, &snssdk.GetSubscriptionAttributesInput{
		SubscriptionArn: sub.SubscriptionArn,
	})
	if err != nil {
		t.Fatalf("GetSubscriptionAttributes() error = %v", err)
	}
	if subAttrs.Attributes["PendingConfirmation"] != "false" {
		t.Fatalf("expected confirmed subscription, got %#v", subAttrs.Attributes)
	}

	if _, err := client.SetSubscriptionAttributes(ctx, &snssdk.SetSubscriptionAttributesInput{
		SubscriptionArn: sub.SubscriptionArn,
		AttributeName:   aws.String("FilterPolicy"),
		AttributeValue:  aws.String(`{"event":["match"]}`),
	}); err != nil {
		t.Fatalf("SetSubscriptionAttributes(FilterPolicy attrs) error = %v", err)
	}
	httpHook.Clear()
	if _, err := client.Publish(ctx, &snssdk.PublishInput{
		TopicArn: topic.TopicArn,
		Message:  aws.String("attr-miss"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"event": {DataType: aws.String("String"), StringValue: aws.String("miss")},
		},
	}); err != nil {
		t.Fatalf("Publish(attr miss) error = %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if len(httpHook.Events()) != 0 {
		t.Fatalf("expected no delivery for attribute miss")
	}
	if _, err := client.Publish(ctx, &snssdk.PublishInput{
		TopicArn: topic.TopicArn,
		Message:  aws.String("attr-hit"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"event": {DataType: aws.String("String"), StringValue: aws.String("match")},
		},
	}); err != nil {
		t.Fatalf("Publish(attr hit) error = %v", err)
	}
	if _, err := httpHook.WaitForType(timeoutContext(t), "Notification"); err != nil {
		t.Fatalf("expected Notification for attribute match: %v", err)
	}

	if _, err := client.SetSubscriptionAttributes(ctx, &snssdk.SetSubscriptionAttributesInput{
		SubscriptionArn: sub.SubscriptionArn,
		AttributeName:   aws.String("FilterPolicy"),
		AttributeValue:  aws.String(`{"kind":["receipt"]}`),
	}); err != nil {
		t.Fatalf("SetSubscriptionAttributes(FilterPolicy body) error = %v", err)
	}
	if _, err := client.SetSubscriptionAttributes(ctx, &snssdk.SetSubscriptionAttributesInput{
		SubscriptionArn: sub.SubscriptionArn,
		AttributeName:   aws.String("FilterPolicyScope"),
		AttributeValue:  aws.String("MessageBody"),
	}); err != nil {
		t.Fatalf("SetSubscriptionAttributes(FilterPolicyScope) error = %v", err)
	}
	httpHook.Clear()
	_, _ = client.Publish(ctx, &snssdk.PublishInput{TopicArn: topic.TopicArn, Message: aws.String(`{"kind":"invoice"}`)})
	time.Sleep(200 * time.Millisecond)
	if len(httpHook.Events()) != 0 {
		t.Fatalf("expected no delivery for body filter miss")
	}
	_, _ = client.Publish(ctx, &snssdk.PublishInput{TopicArn: topic.TopicArn, Message: aws.String(`{"kind":"receipt"}`)})
	bodyEvent, err := httpHook.WaitForType(timeoutContext(t), "Notification")
	if err != nil {
		t.Fatalf("expected body-scope notification: %v", err)
	}
	if stringField(bodyEvent.JSON, "Message") != `{"kind":"receipt"}` {
		t.Fatalf("unexpected body-scope message payload: %s", bodyEvent.Body)
	}

	if _, err := client.SetSubscriptionAttributes(ctx, &snssdk.SetSubscriptionAttributesInput{
		SubscriptionArn: sub.SubscriptionArn,
		AttributeName:   aws.String("FilterPolicy"),
		AttributeValue:  aws.String(""),
	}); err != nil {
		t.Fatalf("clear filter policy error = %v", err)
	}
	if _, err := client.SetSubscriptionAttributes(ctx, &snssdk.SetSubscriptionAttributesInput{
		SubscriptionArn: sub.SubscriptionArn,
		AttributeName:   aws.String("FilterPolicyScope"),
		AttributeValue:  aws.String("MessageAttributes"),
	}); err != nil {
		t.Fatalf("reset filter scope error = %v", err)
	}
	if _, err := client.SetSubscriptionAttributes(ctx, &snssdk.SetSubscriptionAttributesInput{
		SubscriptionArn: sub.SubscriptionArn,
		AttributeName:   aws.String("RawMessageDelivery"),
		AttributeValue:  aws.String("true"),
	}); err != nil {
		t.Fatalf("SetSubscriptionAttributes(RawMessageDelivery) error = %v", err)
	}
	httpHook.Clear()
	_, _ = client.Publish(ctx, &snssdk.PublishInput{TopicArn: topic.TopicArn, Message: aws.String("raw-http")})
	rawEvent, err := httpHook.WaitForType(timeoutContext(t), "Notification")
	if err != nil {
		t.Fatalf("expected raw notification event: %v", err)
	}
	if string(rawEvent.Body) != "raw-http" {
		t.Fatalf("expected raw body, got %q", rawEvent.Body)
	}
	if got := rawEvent.Headers.Get("x-amz-sns-rawdelivery"); got != "true" {
		t.Fatalf("expected x-amz-sns-rawdelivery=true, got %q", got)
	}

	_, err = client.SetSubscriptionAttributes(ctx, &snssdk.SetSubscriptionAttributesInput{
		SubscriptionArn: sub.SubscriptionArn,
		AttributeName:   aws.String("FilterPolicy"),
		AttributeValue:  aws.String(`{"$or":["bad"]}`),
	})
	assertAPIErrorCode(t, err, "InvalidParameter")

	if _, err := client.Unsubscribe(ctx, &snssdk.UnsubscribeInput{SubscriptionArn: sub.SubscriptionArn}); err != nil {
		t.Fatalf("Unsubscribe(http) error = %v", err)
	}
	if _, err := httpHook.WaitForType(timeoutContext(t), "UnsubscribeConfirmation"); err != nil {
		t.Fatalf("expected UnsubscribeConfirmation: %v", err)
	}

	httpsHook := testsupport.NewWebhookCapture(t, true)
	httpsSub, err := client.Subscribe(ctx, &snssdk.SubscribeInput{
		TopicArn:              topic.TopicArn,
		Protocol:              aws.String("https"),
		Endpoint:              aws.String(httpsHook.URL()),
		ReturnSubscriptionArn: true,
	})
	if err != nil {
		t.Fatalf("Subscribe(https) error = %v", err)
	}
	httpsConfirm, err := httpsHook.WaitForType(timeoutContext(t), "SubscriptionConfirmation")
	if err != nil {
		t.Fatalf("expected https confirmation: %v", err)
	}
	if _, err := client.ConfirmSubscription(ctx, &snssdk.ConfirmSubscriptionInput{
		TopicArn: topic.TopicArn,
		Token:    aws.String(stringField(httpsConfirm.JSON, "Token")),
	}); err != nil {
		t.Fatalf("ConfirmSubscription(https) error = %v", err)
	}
	_, _ = client.Publish(ctx, &snssdk.PublishInput{TopicArn: topic.TopicArn, Message: aws.String("https-delivery")})
	if _, err := httpsHook.WaitForType(timeoutContext(t), "Notification"); err != nil {
		t.Fatalf("expected https notification: %v", err)
	}
	if _, err := client.Unsubscribe(ctx, &snssdk.UnsubscribeInput{SubscriptionArn: httpsSub.SubscriptionArn}); err != nil {
		t.Fatalf("Unsubscribe(https) error = %v", err)
	}
}

func TestSDKSQLitePersistenceAcrossRestart(t *testing.T) {
	ctx := context.Background()
	h := testsupport.NewSQLiteHarness(t)
	client := h.SNSClient(ctx)
	queueARN := h.SQS.AddQueue("persist-orders.fifo", true)

	topic, err := client.CreateTopic(ctx, &snssdk.CreateTopicInput{
		Name: aws.String("persist-orders.fifo"),
		Attributes: map[string]string{
			"FifoTopic":                 "true",
			"ContentBasedDeduplication": "false",
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	sub, err := client.Subscribe(ctx, &snssdk.SubscribeInput{
		TopicArn:              topic.TopicArn,
		Protocol:              aws.String("sqs"),
		Endpoint:              aws.String(queueARN),
		ReturnSubscriptionArn: true,
		Attributes: map[string]string{
			"RawMessageDelivery": "true",
		},
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if _, err := client.Publish(ctx, &snssdk.PublishInput{
		TopicArn:               topic.TopicArn,
		Message:                aws.String("persist-1"),
		MessageGroupId:         aws.String("group-a"),
		MessageDeduplicationId: aws.String("dedup-a"),
	}); err != nil {
		t.Fatalf("Publish(before restart) error = %v", err)
	}

	h.Restart()
	client = h.SNSClient(ctx)
	if len(h.SQS.Messages(queueARN)) != 1 {
		t.Fatalf("expected one SQS message before post-restart publish, got %d", len(h.SQS.Messages(queueARN)))
	}
	attrs, err := client.GetTopicAttributes(ctx, &snssdk.GetTopicAttributesInput{TopicArn: topic.TopicArn})
	if err != nil {
		t.Fatalf("GetTopicAttributes(after restart) error = %v", err)
	}
	if attrs.Attributes["FifoTopic"] != "true" {
		t.Fatalf("expected persisted topic attributes after restart, got %#v", attrs.Attributes)
	}
	listed, err := client.ListSubscriptionsByTopic(ctx, &snssdk.ListSubscriptionsByTopicInput{TopicArn: topic.TopicArn})
	if err != nil {
		t.Fatalf("ListSubscriptionsByTopic(after restart) error = %v", err)
	}
	if len(listed.Subscriptions) != 1 || aws.ToString(listed.Subscriptions[0].SubscriptionArn) != aws.ToString(sub.SubscriptionArn) {
		t.Fatalf("expected persisted subscription after restart, got %#v", listed.Subscriptions)
	}
	if _, err := client.Publish(ctx, &snssdk.PublishInput{
		TopicArn:               topic.TopicArn,
		Message:                aws.String("persist-1"),
		MessageGroupId:         aws.String("group-a"),
		MessageDeduplicationId: aws.String("dedup-a"),
	}); err != nil {
		t.Fatalf("Publish(after restart) error = %v", err)
	}
	if len(h.SQS.Messages(queueARN)) != 1 {
		t.Fatalf("expected dedup state to survive restart, got %d messages", len(h.SQS.Messages(queueARN)))
	}
}

func TestSDKSQSAndFIFO(t *testing.T) {
	ctx := context.Background()
	h := testsupport.NewHarness(t)
	client := h.SNSClient(ctx)

	queue1 := h.SQS.AddQueue("orders", false)
	queue2 := h.SQS.AddQueue("audit", false)

	topic, err := client.CreateTopic(ctx, &snssdk.CreateTopicInput{Name: aws.String("sqs-topic")})
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	_, err = client.Subscribe(ctx, &snssdk.SubscribeInput{
		TopicArn:              topic.TopicArn,
		Protocol:              aws.String("sqs"),
		Endpoint:              aws.String(queue1),
		ReturnSubscriptionArn: true,
	})
	if err != nil {
		t.Fatalf("Subscribe(queue1) error = %v", err)
	}
	sub2, err := client.Subscribe(ctx, &snssdk.SubscribeInput{
		TopicArn:              topic.TopicArn,
		Protocol:              aws.String("sqs"),
		Endpoint:              aws.String(queue2),
		ReturnSubscriptionArn: true,
	})
	if err != nil {
		t.Fatalf("Subscribe(queue2) error = %v", err)
	}

	if _, err := client.ListSubscriptions(ctx, &snssdk.ListSubscriptionsInput{}); err != nil {
		t.Fatalf("ListSubscriptions() error = %v", err)
	}
	byTopic, err := client.ListSubscriptionsByTopic(ctx, &snssdk.ListSubscriptionsByTopicInput{TopicArn: topic.TopicArn})
	if err != nil {
		t.Fatalf("ListSubscriptionsByTopic() error = %v", err)
	}
	if len(byTopic.Subscriptions) != 2 {
		t.Fatalf("expected 2 subscriptions by topic, got %d", len(byTopic.Subscriptions))
	}

	if _, err := client.SetSubscriptionAttributes(ctx, &snssdk.SetSubscriptionAttributesInput{
		SubscriptionArn: sub2.SubscriptionArn,
		AttributeName:   aws.String("RawMessageDelivery"),
		AttributeValue:  aws.String("true"),
	}); err != nil {
		t.Fatalf("SetSubscriptionAttributes(raw sqs) error = %v", err)
	}
	if _, err := client.Publish(ctx, &snssdk.PublishInput{
		TopicArn: topic.TopicArn,
		Message:  aws.String("fanout-message"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"kind": {DataType: aws.String("String"), StringValue: aws.String("fanout")},
		},
	}); err != nil {
		t.Fatalf("Publish(fanout) error = %v", err)
	}
	messages1 := h.SQS.Messages(queue1)
	messages2 := h.SQS.Messages(queue2)
	if len(messages1) != 1 || len(messages2) != 1 {
		t.Fatalf("expected fanout to 2 queues, got q1=%d q2=%d", len(messages1), len(messages2))
	}
	if messages2[0].Body != "fanout-message" {
		t.Fatalf("expected raw SQS body, got %q", messages2[0].Body)
	}
	if messages2[0].Attributes["kind"] != "fanout" {
		t.Fatalf("expected raw SQS message attribute to be forwarded")
	}
	var envelope map[string]any
	if err := json.Unmarshal([]byte(messages1[0].Body), &envelope); err != nil {
		t.Fatalf("expected non-raw SQS envelope JSON, error = %v", err)
	}
	if envelope["Message"] != "fanout-message" {
		t.Fatalf("unexpected SQS envelope message: %#v", envelope)
	}

	fifoQueue := h.SQS.AddQueue("orders.fifo", true)
	fifoTopic, err := client.CreateTopic(ctx, &snssdk.CreateTopicInput{
		Name: aws.String("orders.fifo"),
		Attributes: map[string]string{
			"FifoTopic":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic(fifo) error = %v", err)
	}
	fifoSub, err := client.Subscribe(ctx, &snssdk.SubscribeInput{
		TopicArn:              fifoTopic.TopicArn,
		Protocol:              aws.String("sqs"),
		Endpoint:              aws.String(fifoQueue),
		ReturnSubscriptionArn: true,
	})
	if err != nil {
		t.Fatalf("Subscribe(fifo sqs) error = %v", err)
	}
	if _, err := client.SetSubscriptionAttributes(ctx, &snssdk.SetSubscriptionAttributesInput{
		SubscriptionArn: fifoSub.SubscriptionArn,
		AttributeName:   aws.String("RawMessageDelivery"),
		AttributeValue:  aws.String("true"),
	}); err != nil {
		t.Fatalf("SetSubscriptionAttributes(raw fifo sqs) error = %v", err)
	}

	_, err = client.Publish(ctx, &snssdk.PublishInput{
		TopicArn: fifoTopic.TopicArn,
		Message:  aws.String("missing-group"),
	})
	assertAPIErrorCode(t, err, "InvalidParameter")

	first, err := client.Publish(ctx, &snssdk.PublishInput{
		TopicArn:       fifoTopic.TopicArn,
		Message:        aws.String("event-1"),
		MessageGroupId: aws.String("group-a"),
	})
	if err != nil {
		t.Fatalf("Publish(fifo first) error = %v", err)
	}
	if aws.ToString(first.SequenceNumber) == "" {
		t.Fatalf("expected FIFO sequence number")
	}
	_, err = client.Publish(ctx, &snssdk.PublishInput{
		TopicArn:       fifoTopic.TopicArn,
		Message:        aws.String("event-1"),
		MessageGroupId: aws.String("group-a"),
	})
	if err != nil {
		t.Fatalf("Publish(fifo dedup) error = %v", err)
	}
	_, err = client.Publish(ctx, &snssdk.PublishInput{
		TopicArn:       fifoTopic.TopicArn,
		Message:        aws.String("event-2"),
		MessageGroupId: aws.String("group-a"),
	})
	if err != nil {
		t.Fatalf("Publish(fifo second) error = %v", err)
	}
	fifoMessages := h.SQS.Messages(fifoQueue)
	if len(fifoMessages) != 2 {
		t.Fatalf("expected deduplicated FIFO queue length 2, got %d", len(fifoMessages))
	}
	if fifoMessages[0].Body != "event-1" || fifoMessages[1].Body != "event-2" {
		t.Fatalf("unexpected FIFO order: %#v", fifoMessages)
	}
	if fifoMessages[0].GroupID != "group-a" || fifoMessages[1].GroupID != "group-a" {
		t.Fatalf("expected group id to be forwarded")
	}

	h.SQS.Clear(fifoQueue)
	var batchEntries []snstypes.PublishBatchRequestEntry
	for i := 0; i < 10; i++ {
		batchEntries = append(batchEntries, snstypes.PublishBatchRequestEntry{
			Id:                     aws.String(fmt.Sprintf("id-%d", i)),
			Message:                aws.String(fmt.Sprintf("batch-%d", i)),
			MessageGroupId:         aws.String("group-a"),
			MessageDeduplicationId: aws.String(fmt.Sprintf("dedup-%d", i)),
		})
	}
	batchOut, err := client.PublishBatch(ctx, &snssdk.PublishBatchInput{
		TopicArn:                   fifoTopic.TopicArn,
		PublishBatchRequestEntries: batchEntries,
	})
	if err != nil {
		t.Fatalf("PublishBatch(10 entries) error = %v", err)
	}
	if len(batchOut.Successful) != 10 {
		t.Fatalf("expected 10 successful batch entries, got %d", len(batchOut.Successful))
	}
	fifoMessages = h.SQS.Messages(fifoQueue)
	if len(fifoMessages) != 10 {
		t.Fatalf("expected 10 queued FIFO batch messages, got %d", len(fifoMessages))
	}
	for i, msg := range fifoMessages {
		want := fmt.Sprintf("batch-%d", i)
		if msg.Body != want {
			t.Fatalf("expected batch ordering %q at index %d, got %q", want, i, msg.Body)
		}
	}

	var over []snstypes.PublishBatchRequestEntry
	for i := 0; i < 11; i++ {
		over = append(over, snstypes.PublishBatchRequestEntry{
			Id:                     aws.String(fmt.Sprintf("id-%d", i)),
			Message:                aws.String("x"),
			MessageGroupId:         aws.String("group-a"),
			MessageDeduplicationId: aws.String(fmt.Sprintf("d-%d", i)),
		})
	}
	_, err = client.PublishBatch(ctx, &snssdk.PublishBatchInput{
		TopicArn:                   fifoTopic.TopicArn,
		PublishBatchRequestEntries: over,
	})
	assertAPIErrorCode(t, err, "TooManyEntriesInBatchRequest")

	_, err = client.Subscribe(ctx, &snssdk.SubscribeInput{
		TopicArn: fifoTopic.TopicArn,
		Protocol: aws.String("http"),
		Endpoint: aws.String("http://localhost.invalid"),
	})
	assertAPIErrorCode(t, err, "InvalidParameter")
}

func timeoutContext(t *testing.T) context.Context {
	t.Helper()
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	return ctx
}

func stringField(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	value, _ := values[key].(string)
	return value
}

func contains(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func assertAPIErrorCode(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected API error %q, got nil", want)
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected smithy API error %q, got %T: %v", want, err, err)
	}
	if apiErr.ErrorCode() != want {
		t.Fatalf("expected API error code %q, got %q", want, apiErr.ErrorCode())
	}
}
