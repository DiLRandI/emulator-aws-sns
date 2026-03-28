package service

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"time"

	"emulator-aws-sns/internal/delivery"
	"emulator-aws-sns/internal/domain"
	sqsproto "emulator-aws-sns/internal/protocol/sqs"
	"emulator-aws-sns/internal/util"
)

func (s *Service) Close() error {
	close(s.stopCh)
	<-s.doneCh
	if s.store != nil {
		return s.store.Close()
	}
	return nil
}

func (s *Service) Health(ctx context.Context) error {
	if s.store == nil {
		return nil
	}
	return s.store.Health(ctx)
}

func (s *Service) runDeliveryWorker() {
	defer close(s.doneCh)
	for {
		s.processReadyDeliveryJobs(context.Background(), 32)
		select {
		case <-s.stopCh:
			return
		case <-s.clock.After(s.deliveryPollInterval()):
		}
	}
}

func (s *Service) deliveryPollInterval() time.Duration {
	if s.cfg.DeliveryPollInterval > 0 {
		return s.cfg.DeliveryPollInterval
	}
	return 200 * time.Millisecond
}

func (s *Service) processReadyDeliveryJobs(ctx context.Context, limit int) {
	jobs, err := s.store.ListReadyDeliveryJobs(s.clock.Now(), limit)
	if err != nil {
		return
	}
	for _, job := range jobs {
		s.processDeliveryJob(ctx, job)
	}
}

func (s *Service) processDeliveryJob(ctx context.Context, job *domain.DeliveryJob) {
	payload := domain.CopyDeliveryJobPayload(job.Payload)
	adapter, ok := s.protocols[payload.Protocol]
	if !ok {
		s.rescheduleDeliveryJob(job, 5*time.Second, "protocol adapter not configured")
		return
	}
	policyValue, err := delivery.ParseSubscriptionPolicy(job.EffectiveDeliveryJSON)
	if err != nil {
		policyValue = delivery.DefaultPolicy()
	}
	attempt := attemptFromPayload(payload)
	result := adapter.Deliver(ctx, attempt, policyValue, job.RawMessageDelivery)
	if result.Success {
		_ = s.store.DeleteDeliveryJob(job.ID)
		return
	}

	delays, err := delivery.RetryDelays(policyValue)
	if err != nil {
		delays = nil
	}
	if !result.Retryable || job.AttemptCount >= len(delays) {
		_ = s.store.DeleteDeliveryJob(job.ID)
		if job.Kind == "notification" {
			s.redrivePayload(ctx, payload, job.RedrivePolicy)
		}
		return
	}
	s.rescheduleDeliveryJob(job, delays[job.AttemptCount], result.ErrorMessage)
}

func (s *Service) rescheduleDeliveryJob(job *domain.DeliveryJob, delay time.Duration, lastError string) {
	_, _ = s.store.UpdateDeliveryJob(job.ID, func(current *domain.DeliveryJob) error {
		current.AttemptCount++
		current.LastError = lastError
		current.NextAttemptAt = s.clock.Now().Add(delay)
		current.UpdatedAt = s.clock.Now()
		return nil
	})
}

func (s *Service) enqueueDeliveryJob(kind string, attempt *domain.DeliveryAttempt, effectivePolicyJSON string, raw bool, redrivePolicy string) error {
	job, err := s.prepareDeliveryJob(kind, attempt, effectivePolicyJSON, raw, redrivePolicy)
	if err != nil {
		return err
	}
	return s.store.EnqueueDeliveryJob(job)
}

func (s *Service) prepareDeliveryJob(kind string, attempt *domain.DeliveryAttempt, effectivePolicyJSON string, raw bool, redrivePolicy string) (*domain.DeliveryJob, error) {
	signature, err := s.signer.Sign(attempt)
	if err == nil {
		attempt.Signature = signature
	}
	now := s.clock.Now()
	return &domain.DeliveryJob{
		ID:                    util.UUID(),
		Kind:                  kind,
		Payload:               payloadFromAttempt(attempt),
		EffectiveDeliveryJSON: effectivePolicyJSON,
		RawMessageDelivery:    raw,
		RedrivePolicy:         redrivePolicy,
		NextAttemptAt:         now,
		CreatedAt:             now,
		UpdatedAt:             now,
	}, nil
}

func (s *Service) redrivePayload(ctx context.Context, payload domain.DeliveryJobPayload, redrivePolicy string) {
	if s.sqsClient == nil || redrivePolicy == "" {
		return
	}
	rp, err := domain.ParseRedrivePolicy(redrivePolicy)
	if err != nil || rp.DeadLetterTargetArn == "" {
		return
	}
	envelope := map[string]any{
		"Type":            "Notification",
		"MessageId":       payload.MessageID,
		"TopicArn":        payload.TopicARN,
		"Message":         payload.ProtocolMessage,
		"Timestamp":       domain.DefaultTimestamp(payload.Timestamp),
		"SubscriptionArn": payload.SubscriptionARN,
	}
	body, _ := json.Marshal(envelope)
	_ = s.sqsClient.SendMessage(ctx, sqsproto.SendRequest{
		QueueARN:               rp.DeadLetterTargetArn,
		Body:                   string(body),
		MessageGroupID:         payload.GroupID,
		MessageDeduplicationID: payload.DeduplicationID,
	})
}

func payloadFromAttempt(attempt *domain.DeliveryAttempt) domain.DeliveryJobPayload {
	return domain.DeliveryJobPayload{
		Type:              attempt.Type,
		TopicARN:          attempt.Topic.ARN,
		TopicOwner:        attempt.Topic.Owner,
		SubscriptionARN:   attempt.Subscription.ARN,
		Protocol:          attempt.Subscription.Protocol,
		Endpoint:          attempt.Subscription.Endpoint,
		MessageID:         attempt.MessageID,
		Subject:           attempt.Subject,
		Message:           attempt.Message,
		ProtocolMessage:   attempt.ProtocolMessage,
		Timestamp:         attempt.Timestamp,
		Token:             attempt.Token,
		SubscribeURL:      attempt.SubscribeURL,
		UnsubscribeURL:    attempt.UnsubscribeURL,
		SignatureVersion:  attempt.SignatureVersion,
		Signature:         attempt.Signature,
		SigningCertURL:    attempt.SigningCertURL,
		MessageAttributes: cloneMessageAttributes(attempt.MessageAttributes),
		GroupID:           attempt.GroupID,
		DeduplicationID:   attempt.DeduplicationID,
		SequenceNumber:    attempt.SequenceNumber,
		Headers:           cloneStringMap(attempt.Headers),
	}
}

func attemptFromPayload(payload domain.DeliveryJobPayload) *domain.DeliveryAttempt {
	return &domain.DeliveryAttempt{
		Subscription: &domain.Subscription{
			ARN:      payload.SubscriptionARN,
			Protocol: payload.Protocol,
			Endpoint: payload.Endpoint,
		},
		Topic: &domain.Topic{
			ARN:   payload.TopicARN,
			Owner: payload.TopicOwner,
		},
		MessageID:         payload.MessageID,
		Subject:           payload.Subject,
		Message:           payload.Message,
		ProtocolMessage:   payload.ProtocolMessage,
		Timestamp:         payload.Timestamp,
		Type:              payload.Type,
		Token:             payload.Token,
		SubscribeURL:      payload.SubscribeURL,
		UnsubscribeURL:    payload.UnsubscribeURL,
		SignatureVersion:  payload.SignatureVersion,
		Signature:         payload.Signature,
		SigningCertURL:    payload.SigningCertURL,
		MessageAttributes: cloneMessageAttributes(payload.MessageAttributes),
		GroupID:           payload.GroupID,
		DeduplicationID:   payload.DeduplicationID,
		SequenceNumber:    payload.SequenceNumber,
		Headers:           cloneStringMap(payload.Headers),
	}
}

func cloneStringMap(src map[string]string) map[string]string {
	if src == nil {
		return map[string]string{}
	}
	dst := make(map[string]string, len(src))
	maps.Copy(dst, src)
	return dst
}

func cloneMessageAttributes(src map[string]domain.MessageAttributeValue) map[string]domain.MessageAttributeValue {
	if src == nil {
		return map[string]domain.MessageAttributeValue{}
	}
	dst := make(map[string]domain.MessageAttributeValue, len(src))
	for k, v := range src {
		dst[k] = v.DeepCopy()
	}
	return dst
}

func isNotFound(err error) bool {
	return errors.Is(err, domain.ErrNotFound)
}
