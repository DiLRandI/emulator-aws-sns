package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"emulator-aws-sns/internal/domain"
)

func TestStorePersistsCoreStateAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sns.sqlite")
	store1, err := Open(path)
	if err != nil {
		t.Fatalf("Open(store1) error = %v", err)
	}
	now := time.Now().UTC().Round(time.Second)
	topic := &domain.Topic{
		ARN:       "arn:aws:sns:us-east-1:123456789012:orders.fifo",
		Name:      "orders.fifo",
		Owner:     "123456789012",
		Region:    "us-east-1",
		CreatedAt: now,
		UpdatedAt: now,
		Attributes: domain.TopicAttributes{
			FifoTopic:                 true,
			ContentBasedDeduplication: true,
		},
		Tags:           map[string]string{"team": "store"},
		Subscriptions:  map[string]struct{}{},
		GroupSequences: map[string]domain.SequenceState{"group-a": {Prefix: "0001", Counter: 7}},
		DedupRecords: map[string]domain.DedupRecord{
			"scope:dedup-1": {MessageID: "m-1", SequenceNumber: "42", ExpiresAt: now.Add(5 * time.Minute)},
		},
	}
	sub := &domain.Subscription{
		ARN:                 topic.ARN + ":sub-1",
		TopicARN:            topic.ARN,
		Owner:               "123456789012",
		Protocol:            "http",
		Endpoint:            "http://127.0.0.1/hook",
		CreatedAt:           now,
		UpdatedAt:           now,
		PendingConfirmation: true,
		Attributes: domain.SubscriptionAttributes{
			FilterPolicyScope:  domain.FilterScopeMessageAttributes,
			RawMessageDelivery: true,
		},
	}
	token := domain.ConfirmationToken{
		Token:           "token-1",
		SubscriptionARN: sub.ARN,
		TopicARN:        topic.ARN,
		Endpoint:        sub.Endpoint,
		Protocol:        sub.Protocol,
		ExpiresAt:       now.Add(48 * time.Hour),
		Kind:            "subscribe",
	}
	job := &domain.DeliveryJob{
		ID:                    "job-1",
		Kind:                  "notification",
		Payload:               domain.DeliveryJobPayload{TopicARN: topic.ARN, SubscriptionARN: sub.ARN, Protocol: "http", Endpoint: sub.Endpoint, MessageID: "mid-1", ProtocolMessage: "hello", Timestamp: now},
		EffectiveDeliveryJSON: `{"healthyRetryPolicy":{"numRetries":3}}`,
		RawMessageDelivery:    false,
		NextAttemptAt:         now,
		CreatedAt:             now,
		UpdatedAt:             now,
	}
	material := domain.SigningMaterial{PrivateKeyPEM: []byte("key"), CertPEM: []byte("cert"), CreatedAt: now}

	if err := store1.CreateTopic(topic); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store1.CreateSubscription(sub); err != nil {
		t.Fatalf("CreateSubscription() error = %v", err)
	}
	store1.PutConfirmationToken(token)
	if err := store1.EnqueueDeliveryJob(job); err != nil {
		t.Fatalf("EnqueueDeliveryJob() error = %v", err)
	}
	if err := store1.SaveSigningMaterial(material); err != nil {
		t.Fatalf("SaveSigningMaterial() error = %v", err)
	}
	if err := store1.Close(); err != nil {
		t.Fatalf("Close(store1) error = %v", err)
	}

	store2, err := Open(path)
	if err != nil {
		t.Fatalf("Open(store2) error = %v", err)
	}
	defer func() { _ = store2.Close() }()

	gotTopic, err := store2.GetTopic(topic.ARN)
	if err != nil {
		t.Fatalf("GetTopic() error = %v", err)
	}
	if !gotTopic.Attributes.FifoTopic || gotTopic.GroupSequences["group-a"].Counter != 7 {
		t.Fatalf("topic state did not persist: %#v", gotTopic)
	}
	gotSub, err := store2.GetSubscription(sub.ARN)
	if err != nil {
		t.Fatalf("GetSubscription() error = %v", err)
	}
	if !gotSub.Attributes.RawMessageDelivery || !gotSub.PendingConfirmation {
		t.Fatalf("subscription state did not persist: %#v", gotSub)
	}
	gotToken, err := store2.GetConfirmationToken(token.Token)
	if err != nil {
		t.Fatalf("GetConfirmationToken() error = %v", err)
	}
	if gotToken.SubscriptionARN != sub.ARN {
		t.Fatalf("confirmation token did not persist: %#v", gotToken)
	}
	jobs, err := store2.ListReadyDeliveryJobs(now.Add(time.Second), 10)
	if err != nil {
		t.Fatalf("ListReadyDeliveryJobs() error = %v", err)
	}
	if len(jobs) != 1 || jobs[0].Payload.MessageID != "mid-1" {
		t.Fatalf("delivery job did not persist: %#v", jobs)
	}
	gotMaterial, err := store2.LoadSigningMaterial()
	if err != nil {
		t.Fatalf("LoadSigningMaterial() error = %v", err)
	}
	if string(gotMaterial.CertPEM) != "cert" || string(gotMaterial.PrivateKeyPEM) != "key" {
		t.Fatalf("signing material did not persist: %#v", gotMaterial)
	}
	if err := store2.Health(context.Background()); err != nil {
		t.Fatalf("Health() error = %v", err)
	}
}
