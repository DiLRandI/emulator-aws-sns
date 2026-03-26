package service

import (
	"context"
	"testing"

	"emulator-aws-sns/internal/domain"
	httpproto "emulator-aws-sns/internal/protocol/http"
	sqsproto "emulator-aws-sns/internal/protocol/sqs"
	"emulator-aws-sns/internal/signing"
	"emulator-aws-sns/internal/store/memory"
	"emulator-aws-sns/internal/util"
)

type fakeSQS struct {
	queues   map[string]sqsproto.QueueAttributes
	allowed  bool
	messages []sqsproto.SendRequest
}

func (f *fakeSQS) ResolveQueue(_ context.Context, queueARN string) (sqsproto.QueueAttributes, error) {
	return f.queues[queueARN], nil
}

func (f *fakeSQS) SendMessage(_ context.Context, req sqsproto.SendRequest) error {
	f.messages = append(f.messages, req)
	return nil
}

func (f *fakeSQS) QueueAllowsTopic(_ context.Context, _, _, _ string) (bool, error) {
	return f.allowed, nil
}

func TestFIFOPublishDeduplicatesForSQS(t *testing.T) {
	signer, err := signing.NewProvider("/__sns/certs/current.pem")
	if err != nil {
		t.Fatalf("signing.NewProvider() error = %v", err)
	}
	queueARN := "arn:aws:sqs:us-east-1:123456789012:orders.fifo"
	fake := &fakeSQS{
		queues: map[string]sqsproto.QueueAttributes{
			queueARN: {ARN: queueARN, FIFO: true, Policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"sns.amazonaws.com"},"Action":"SQS:SendMessage","Resource":"arn:aws:sqs:us-east-1:123456789012:orders.fifo","Condition":{"ArnEquals":{"aws:SourceArn":"arn:aws:sns:us-east-1:123456789012:orders.fifo"}}}]}`},
		},
		allowed: true,
	}
	svc := New(Config{
		Region:    "us-east-1",
		AccountID: "123456789012",
		BaseURL:   "http://localhost:4100",
		PageSize:  100,
	}, memory.NewStore(), util.RealClock{}, signer, httpproto.NewAdapter(nil), fake)

	topic, _, err := svc.CreateTopic(context.Background(), testIdentity(), "orders.fifo", map[string]string{
		"FifoTopic":                 "true",
		"ContentBasedDeduplication": "false",
	})
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	subArn, err := svc.Subscribe(context.Background(), testIdentity(), topic.ARN, "sqs", queueARN, map[string]string{
		"RawMessageDelivery": "true",
	}, true)
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if subArn == "" {
		t.Fatalf("expected subscription ARN")
	}

	first, err := svc.Publish(context.Background(), testIdentity(), testPublish(topic.ARN, "msg-1", "group-1", "dedup-1"))
	if err != nil {
		t.Fatalf("Publish(first) error = %v", err)
	}
	second, err := svc.Publish(context.Background(), testIdentity(), testPublish(topic.ARN, "msg-1", "group-1", "dedup-1"))
	if err != nil {
		t.Fatalf("Publish(second) error = %v", err)
	}

	if first.SequenceNumber == "" {
		t.Fatalf("expected FIFO sequence number")
	}
	if !second.Deduplicated {
		t.Fatalf("expected second publish to be deduplicated")
	}
	if len(fake.messages) != 1 {
		t.Fatalf("expected one SQS send, got %d", len(fake.messages))
	}
	if fake.messages[0].MessageGroupID != "group-1" {
		t.Fatalf("expected message group id to be forwarded")
	}
}

func TestRejectStandardTopicToFIFOQueue(t *testing.T) {
	signer, err := signing.NewProvider("/__sns/certs/current.pem")
	if err != nil {
		t.Fatalf("signing.NewProvider() error = %v", err)
	}
	queueARN := "arn:aws:sqs:us-east-1:123456789012:orders.fifo"
	fake := &fakeSQS{
		queues:  map[string]sqsproto.QueueAttributes{queueARN: {ARN: queueARN, FIFO: true}},
		allowed: true,
	}
	svc := New(Config{Region: "us-east-1", AccountID: "123456789012", BaseURL: "http://localhost:4100"}, memory.NewStore(), util.RealClock{}, signer, httpproto.NewAdapter(nil), fake)
	topic, _, err := svc.CreateTopic(context.Background(), testIdentity(), "orders", nil)
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if _, err := svc.Subscribe(context.Background(), testIdentity(), topic.ARN, "sqs", queueARN, nil, true); err == nil {
		t.Fatalf("expected standard topic -> FIFO queue subscription to fail")
	}
}

func testIdentity() domain.Identity {
	return domain.Identity{AccountID: "123456789012", Principal: "123456789012"}
}

func testPublish(topicARN, message, groupID, dedupID string) domain.PublishInput {
	return domain.PublishInput{
		TopicARN:               topicARN,
		Message:                message,
		MessageGroupID:         groupID,
		MessageDeduplicationID: dedupID,
	}
}
