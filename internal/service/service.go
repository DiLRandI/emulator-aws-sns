package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"emulator-aws-sns/internal/delivery"
	"emulator-aws-sns/internal/domain"
	"emulator-aws-sns/internal/filter"
	"emulator-aws-sns/internal/policy"
	httpproto "emulator-aws-sns/internal/protocol/http"
	sqsproto "emulator-aws-sns/internal/protocol/sqs"
	"emulator-aws-sns/internal/signing"
	storepkg "emulator-aws-sns/internal/store"
	"emulator-aws-sns/internal/util"
)

type Config struct {
	Region               string
	AccountID            string
	BaseURL              string
	PageSize             int
	DeliveryPollInterval time.Duration
}

type ProtocolAdapter interface {
	Protocols() []string
	ValidateEndpoint(endpoint string) error
	Deliver(ctx context.Context, attempt *domain.DeliveryAttempt, policy delivery.SubscriptionPolicy, raw bool) domain.DeliveryResult
}

type Service struct {
	cfg       Config
	store     storepkg.Store
	clock     util.Clock
	signer    *signing.Provider
	protocols map[string]ProtocolAdapter
	sqsClient sqsproto.Client
	stopCh    chan struct{}
	doneCh    chan struct{}
}

func New(cfg Config, store storepkg.Store, clock util.Clock, signer *signing.Provider, httpAdapter *httpproto.Adapter, sqsClient sqsproto.Client) *Service {
	svc := &Service{
		cfg:       cfg,
		store:     store,
		clock:     clock,
		signer:    signer,
		protocols: map[string]ProtocolAdapter{},
		sqsClient: sqsClient,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	if httpAdapter != nil {
		for _, protocol := range httpAdapter.Protocols() {
			svc.protocols[protocol] = httpAdapter
		}
	}
	go svc.runDeliveryWorker()
	return svc
}

func (s *Service) CreateTopic(ctx context.Context, identity domain.Identity, name string, attrs, tags map[string]string) (*domain.Topic, bool, error) {
	fifo := strings.EqualFold(attrs["FifoTopic"], "true")
	if err := util.ValidateTopicName(name, fifo); err != nil {
		return nil, false, domain.NewInvalidParameter("%s", err.Error())
	}
	if existing, err := s.store.FindTopicByName(name); err == nil {
		return existing, true, nil
	}
	now := s.clock.Now()
	topic := &domain.Topic{
		ARN:            util.FormatARN("sns", s.cfg.Region, s.cfg.AccountID, name),
		Name:           name,
		Owner:          s.cfg.AccountID,
		Region:         s.cfg.Region,
		CreatedAt:      now,
		UpdatedAt:      now,
		Tags:           cloneStringMap(tags),
		Subscriptions:  map[string]struct{}{},
		GroupSequences: map[string]domain.SequenceState{},
		DedupRecords:   map[string]domain.DedupRecord{},
		Attributes: domain.TopicAttributes{
			Policy:              policy.DefaultTopicPolicy(util.FormatARN("sns", s.cfg.Region, s.cfg.AccountID, name), s.cfg.AccountID),
			SignatureVersion:    "1",
			TracingConfig:       "PassThrough",
			FifoTopic:           fifo,
			FifoThroughputScope: domain.FifoThroughputTopic,
			Unsupported:         map[string]string{},
		},
	}
	if err := s.applyTopicAttributes(topic, attrs, true); err != nil {
		return nil, false, err
	}
	if len(topic.Tags) > 50 {
		return nil, false, &domain.APIError{Code: "TagLimitExceeded", Message: "Can't add more than 50 tags to a topic.", HTTPStatus: 400, Sender: true}
	}
	if err := s.store.CreateTopic(topic); err != nil {
		return nil, false, err
	}
	return topic, false, nil
}

func (s *Service) DeleteTopic(ctx context.Context, identity domain.Identity, topicARN string) error {
	if _, err := s.store.DeleteTopic(topicARN); err != nil {
		return nil
	}
	return nil
}

func (s *Service) GetTopic(ctx context.Context, identity domain.Identity, topicARN string) (*domain.Topic, error) {
	topic, err := s.store.GetTopic(topicARN)
	if err != nil {
		return nil, domain.NewNotFound("Topic does not exist")
	}
	if err := s.authorizeTopic(identity, topic, "SNS:GetTopicAttributes"); err != nil {
		return nil, err
	}
	return topic, nil
}

func (s *Service) SetTopicAttribute(ctx context.Context, identity domain.Identity, topicARN, name, value string) (*domain.Topic, error) {
	topic, err := s.store.GetTopic(topicARN)
	if err != nil {
		return nil, domain.NewNotFound("Topic does not exist")
	}
	if err := s.authorizeTopic(identity, topic, "SNS:SetTopicAttributes"); err != nil {
		return nil, err
	}
	updated, err := s.store.UpdateTopic(topicARN, func(topic *domain.Topic) error {
		return s.applyTopicAttributes(topic, map[string]string{name: value}, false)
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func (s *Service) ListTopics(ctx context.Context, identity domain.Identity, nextToken string) ([]*domain.Topic, string, error) {
	items, token := s.store.ListTopics(nextToken, s.pageSize())
	return items, token, nil
}

func (s *Service) TagTopic(ctx context.Context, identity domain.Identity, topicARN string, tags map[string]string) error {
	topic, err := s.store.GetTopic(topicARN)
	if err != nil {
		return domain.NewNotFound("Topic does not exist")
	}
	if err := s.authorizeTopic(identity, topic, "SNS:TagResource"); err != nil {
		return err
	}
	if len(topic.Tags)+len(tags) > 50 {
		return &domain.APIError{Code: "TagLimitExceeded", Message: "Can't add more than 50 tags to a topic.", HTTPStatus: 400, Sender: true}
	}
	_, err = s.store.UpdateTopic(topicARN, func(topic *domain.Topic) error {
		if topic.Tags == nil {
			topic.Tags = map[string]string{}
		}
		maps.Copy(topic.Tags, tags)
		return nil
	})
	return err
}

func (s *Service) UntagTopic(ctx context.Context, identity domain.Identity, topicARN string, tagKeys []string) error {
	topic, err := s.store.GetTopic(topicARN)
	if err != nil {
		return domain.NewNotFound("Topic does not exist")
	}
	if err := s.authorizeTopic(identity, topic, "SNS:UntagResource"); err != nil {
		return err
	}
	_, err = s.store.UpdateTopic(topicARN, func(topic *domain.Topic) error {
		for _, key := range tagKeys {
			delete(topic.Tags, key)
		}
		return nil
	})
	return err
}

func (s *Service) AddPermission(ctx context.Context, identity domain.Identity, topicARN, label string, accountIDs, actionNames []string) error {
	topic, err := s.store.GetTopic(topicARN)
	if err != nil {
		return domain.NewNotFound("Topic does not exist")
	}
	if err := s.authorizeTopic(identity, topic, "SNS:AddPermission"); err != nil {
		return err
	}
	newPolicy, err := policy.AddPermission(topic.Attributes.Policy, label, accountIDs, actionNames, topicARN)
	if err != nil {
		return domain.NewInvalidParameter("%s", err.Error())
	}
	_, err = s.store.UpdateTopic(topicARN, func(topic *domain.Topic) error {
		topic.Attributes.Policy = newPolicy
		return nil
	})
	return err
}

func (s *Service) RemovePermission(ctx context.Context, identity domain.Identity, topicARN, label string) error {
	topic, err := s.store.GetTopic(topicARN)
	if err != nil {
		return domain.NewNotFound("Topic does not exist")
	}
	if err := s.authorizeTopic(identity, topic, "SNS:RemovePermission"); err != nil {
		return err
	}
	newPolicy, err := policy.RemovePermission(topic.Attributes.Policy, label)
	if err != nil {
		return domain.NewNotFound("Statement not found")
	}
	_, err = s.store.UpdateTopic(topicARN, func(topic *domain.Topic) error {
		topic.Attributes.Policy = newPolicy
		return nil
	})
	return err
}

func (s *Service) Subscribe(ctx context.Context, identity domain.Identity, topicARN, protocolName, endpoint string, attrs map[string]string, returnSubscriptionARN bool) (string, error) {
	topic, err := s.store.GetTopic(topicARN)
	if err != nil {
		return "", domain.NewNotFound("Topic does not exist")
	}
	if err := s.authorizeTopic(identity, topic, "SNS:Subscribe"); err != nil {
		return "", err
	}
	protocolName = strings.ToLower(protocolName)
	if protocolName != domain.ProtocolSQS && protocolName != domain.ProtocolHTTP && protocolName != domain.ProtocolHTTPS {
		return "", domain.NewInvalidParameter("Invalid parameter: Protocol")
	}
	if topic.Attributes.FifoTopic && protocolName != domain.ProtocolSQS {
		return "", domain.NewInvalidParameter("Invalid parameter: protocol")
	}
	if existing := s.findExistingSubscription(topicARN, protocolName, endpoint); existing != nil {
		if existing.PendingConfirmation && !returnSubscriptionARN {
			return "pending confirmation", nil
		}
		return existing.ARN, nil
	}
	now := s.clock.Now()
	sub := &domain.Subscription{
		ARN:                 fmt.Sprintf("%s:%s", topicARN, util.UUID()),
		TopicARN:            topicARN,
		Owner:               s.cfg.AccountID,
		Protocol:            protocolName,
		Endpoint:            endpoint,
		CreatedAt:           now,
		UpdatedAt:           now,
		Confirmed:           protocolName == domain.ProtocolSQS,
		PendingConfirmation: protocolName != domain.ProtocolSQS,
		Attributes: domain.SubscriptionAttributes{
			FilterPolicyScope: domain.FilterScopeMessageAttributes,
		},
	}
	if err := s.applySubscriptionAttributes(sub, topic, attrs); err != nil {
		return "", err
	}
	if protocolName == domain.ProtocolSQS {
		if err := s.validateSQSSubscription(ctx, topic, sub); err != nil {
			return "", err
		}
	} else if adapter, ok := s.protocols[protocolName]; ok {
		if err := adapter.ValidateEndpoint(endpoint); err != nil {
			return "", err
		}
	}
	if _, effective, err := delivery.EffectivePolicy(topic.Attributes.DeliveryPolicy, sub.Attributes.DeliveryPolicy); err == nil {
		sub.Attributes.EffectiveDeliveryPolicy = effective
	}
	var confirmationJob *domain.DeliveryJob
	if protocolName != domain.ProtocolSQS {
		token := domain.ConfirmationToken{
			Token:                     util.RandomHex(160),
			SubscriptionARN:           sub.ARN,
			TopicARN:                  sub.TopicARN,
			Endpoint:                  sub.Endpoint,
			Protocol:                  sub.Protocol,
			ExpiresAt:                 now.Add(48 * time.Hour),
			AuthenticateOnUnsubscribe: false,
			Kind:                      "subscribe",
		}
		attempt, effectiveJSON, err := s.buildConfirmationAttempt(topic, sub, token, "SubscriptionConfirmation")
		if err != nil {
			return "", err
		}
		confirmationJob, err = s.prepareDeliveryJob("subscription_confirmation", attempt, effectiveJSON, false, "")
		if err != nil {
			return "", err
		}
		err = s.store.Tx(ctx, func(tx storepkg.Tx) error {
			if err := tx.CreateSubscription(sub); err != nil {
				return err
			}
			tx.PutConfirmationToken(token)
			return tx.EnqueueDeliveryJob(confirmationJob)
		})
		if err != nil {
			return "", err
		}
	} else {
		if err := s.store.CreateSubscription(sub); err != nil {
			return "", err
		}
	}
	if sub.PendingConfirmation && !returnSubscriptionARN {
		return "pending confirmation", nil
	}
	return sub.ARN, nil
}

func (s *Service) ConfirmSubscription(ctx context.Context, identity domain.Identity, topicARN, token string, authOnUnsubscribe bool) (string, error) {
	record, err := s.store.GetConfirmationToken(token)
	if err != nil {
		return "", domain.NewNotFound("Token does not exist")
	}
	if record.TopicARN != topicARN {
		return "", domain.NewInvalidParameter("Invalid parameter: Token")
	}
	if s.clock.Now().After(record.ExpiresAt) {
		return "", domain.NewInvalidParameter("Invalid parameter: Token")
	}
	switch record.Kind {
	case "subscribe":
		sub, err := s.store.UpdateSubscription(record.SubscriptionARN, func(sub *domain.Subscription) error {
			sub.PendingConfirmation = false
			sub.Confirmed = true
			sub.AuthenticateOnUnsubscribe = authOnUnsubscribe
			sub.ConfirmationWasAuthenticated = authOnUnsubscribe
			return nil
		})
		if err != nil {
			return "", domain.NewNotFound("Subscription does not exist")
		}
		s.store.DeleteConfirmationToken(token)
		return sub.ARN, nil
	case "unsubscribe":
		restore := domain.CopySubscription(record.Restore)
		restore.PendingConfirmation = false
		restore.Confirmed = true
		restore.AuthenticateOnUnsubscribe = record.AuthenticateOnUnsubscribe
		if err := s.store.CreateSubscription(restore); err != nil {
			return "", err
		}
		s.store.DeleteConfirmationToken(token)
		return restore.ARN, nil
	default:
		return "", domain.NewInvalidParameter("Invalid parameter: Token")
	}
}

func (s *Service) Unsubscribe(ctx context.Context, identity domain.Identity, subscriptionARN string) error {
	sub, err := s.store.GetSubscription(subscriptionARN)
	if err != nil {
		return domain.NewNotFound("Subscription does not exist")
	}
	if sub.AuthenticateOnUnsubscribe && identity.AccountID != s.cfg.AccountID {
		return domain.NewAuthorization("Authorization required")
	}
	topic, err := s.store.GetTopic(sub.TopicARN)
	if err != nil {
		return domain.NewNotFound("Topic does not exist")
	}
	if sub.Protocol == domain.ProtocolHTTP || sub.Protocol == domain.ProtocolHTTPS {
		token := domain.ConfirmationToken{
			Token:                     util.RandomHex(160),
			SubscriptionARN:           sub.ARN,
			TopicARN:                  sub.TopicARN,
			Endpoint:                  sub.Endpoint,
			Protocol:                  sub.Protocol,
			ExpiresAt:                 s.clock.Now().Add(48 * time.Hour),
			AuthenticateOnUnsubscribe: sub.AuthenticateOnUnsubscribe,
			Kind:                      "unsubscribe",
			Restore:                   sub,
		}
		attempt, effectiveJSON, err := s.buildConfirmationAttempt(topic, sub, token, "UnsubscribeConfirmation")
		if err != nil {
			return err
		}
		job, err := s.prepareDeliveryJob("unsubscribe_confirmation", attempt, effectiveJSON, false, "")
		if err != nil {
			return err
		}
		return s.store.Tx(ctx, func(tx storepkg.Tx) error {
			if _, err := tx.DeleteSubscription(subscriptionARN); err != nil {
				return domain.NewNotFound("Subscription does not exist")
			}
			tx.PutConfirmationToken(token)
			return tx.EnqueueDeliveryJob(job)
		})
	}
	_, err = s.store.DeleteSubscription(subscriptionARN)
	if err != nil {
		return domain.NewNotFound("Subscription does not exist")
	}
	return nil
}

func (s *Service) GetSubscription(ctx context.Context, identity domain.Identity, subscriptionARN string) (*domain.Subscription, *domain.Topic, error) {
	sub, err := s.store.GetSubscription(subscriptionARN)
	if err != nil {
		return nil, nil, domain.NewNotFound("Subscription does not exist")
	}
	topic, err := s.store.GetTopic(sub.TopicARN)
	if err != nil {
		return nil, nil, domain.NewNotFound("Topic does not exist")
	}
	return sub, topic, nil
}

func (s *Service) SetSubscriptionAttribute(ctx context.Context, identity domain.Identity, subscriptionARN, name, value string) (*domain.Subscription, error) {
	sub, err := s.store.GetSubscription(subscriptionARN)
	if err != nil {
		return nil, domain.NewNotFound("Subscription does not exist")
	}
	topic, err := s.store.GetTopic(sub.TopicARN)
	if err != nil {
		return nil, domain.NewNotFound("Topic does not exist")
	}
	updated, err := s.store.UpdateSubscription(subscriptionARN, func(sub *domain.Subscription) error {
		return s.applySubscriptionAttributes(sub, topic, map[string]string{name: value})
	})
	if err != nil {
		return nil, err
	}
	if _, effective, err := delivery.EffectivePolicy(topic.Attributes.DeliveryPolicy, updated.Attributes.DeliveryPolicy); err == nil {
		updated.Attributes.EffectiveDeliveryPolicy = effective
		updated, _ = s.store.UpdateSubscription(subscriptionARN, func(sub *domain.Subscription) error {
			sub.Attributes.EffectiveDeliveryPolicy = effective
			return nil
		})
	}
	return updated, nil
}

func (s *Service) ListSubscriptions(ctx context.Context, identity domain.Identity, nextToken string) ([]*domain.Subscription, string, error) {
	items, token := s.store.ListSubscriptions(nextToken, s.pageSize())
	return items, token, nil
}

func (s *Service) ListSubscriptionsByTopic(ctx context.Context, identity domain.Identity, topicARN, nextToken string) ([]*domain.Subscription, string, error) {
	items, token := s.store.ListSubscriptionsByTopic(topicARN, nextToken, s.pageSize())
	return items, token, nil
}

func (s *Service) Publish(ctx context.Context, identity domain.Identity, input domain.PublishInput) (*domain.PublishOutput, error) {
	topic, err := s.store.GetTopic(input.TopicARN)
	if err != nil {
		return nil, domain.NewNotFound("Topic does not exist")
	}
	if err := s.authorizeTopic(identity, topic, "SNS:Publish"); err != nil {
		return nil, err
	}
	if err := validatePublishInput(topic, input); err != nil {
		return nil, err
	}
	messageID := util.UUID()
	dedupID := input.MessageDeduplicationID
	if topic.Attributes.FifoTopic && dedupID == "" && topic.Attributes.ContentBasedDeduplication {
		sum := sha256.Sum256([]byte(input.Message))
		dedupID = fmt.Sprintf("%x", sum[:])
	}
	sequenceNumber := ""
	deduplicated := false
	if topic.Attributes.FifoTopic {
		updated, err := s.store.UpdateTopic(topic.ARN, func(topic *domain.Topic) error {
			scope := topic.ARN
			if topic.Attributes.FifoThroughputScope == domain.FifoThroughputMessageGroup {
				scope = topic.ARN + ":" + input.MessageGroupID
			}
			record, ok := topic.DedupRecords[scope+":"+dedupID]
			if ok && record.ExpiresAt.After(s.clock.Now()) {
				messageID = record.MessageID
				sequenceNumber = record.SequenceNumber
				deduplicated = true
				return nil
			}
			state := topic.GroupSequences[input.MessageGroupID]
			next, seq := domain.NextSequence(state)
			topic.GroupSequences[input.MessageGroupID] = next
			sequenceNumber = seq
			topic.DedupRecords[scope+":"+dedupID] = domain.DedupRecord{
				MessageID:      messageID,
				SequenceNumber: sequenceNumber,
				ExpiresAt:      s.clock.Now().Add(5 * time.Minute),
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		topic = updated
	}
	if !deduplicated {
		if err := s.dispatchPublish(ctx, topic, input, messageID, dedupID, sequenceNumber); err != nil {
			return nil, err
		}
	}
	return &domain.PublishOutput{MessageID: messageID, SequenceNumber: sequenceNumber, Deduplicated: deduplicated}, nil
}

func (s *Service) PublishBatch(ctx context.Context, identity domain.Identity, topicARN string, entries []domain.PublishBatchEntry) ([]domain.BatchDeliveryResult, []domain.PublishBatchFailure, error) {
	topic, err := s.store.GetTopic(topicARN)
	if err != nil {
		return nil, nil, domain.NewNotFound("Topic does not exist")
	}
	if err := s.authorizeTopic(identity, topic, "SNS:Publish"); err != nil {
		return nil, nil, err
	}
	if len(entries) == 0 {
		return nil, nil, &domain.APIError{Code: "EmptyBatchRequest", Message: "The batch request doesn't contain any entries.", HTTPStatus: 400, Sender: true}
	}
	if len(entries) > 10 {
		return nil, nil, &domain.APIError{Code: "TooManyEntriesInBatchRequest", Message: "The batch request contains more entries than permissible (more than 10).", HTTPStatus: 400, Sender: true}
	}
	totalBytes := 0
	seen := map[string]struct{}{}
	var success []domain.BatchDeliveryResult
	var failed []domain.PublishBatchFailure
	for _, entry := range entries {
		if _, ok := seen[entry.ID]; ok {
			return nil, nil, &domain.APIError{Code: "BatchEntryIdsNotDistinct", Message: "Two or more batch entries in the request have the same Id.", HTTPStatus: 400, Sender: true}
		}
		seen[entry.ID] = struct{}{}
		totalBytes += len(entry.Message)
	}
	if totalBytes > 262144 {
		return nil, nil, &domain.APIError{Code: "BatchRequestTooLong", Message: "The length of all the batch messages put together is more than the limit.", HTTPStatus: 400, Sender: true}
	}
	for _, entry := range entries {
		out, err := s.Publish(ctx, identity, domain.PublishInput{
			TopicARN:               topicARN,
			Message:                entry.Message,
			Subject:                entry.Subject,
			MessageStructure:       entry.MessageStructure,
			MessageAttributes:      entry.MessageAttributes,
			MessageGroupID:         entry.MessageGroupID,
			MessageDeduplicationID: entry.MessageDeduplicationID,
		})
		if err != nil {
			var apiErr *domain.APIError
			if errors.As(err, &apiErr) {
				failed = append(failed, domain.PublishBatchFailure{ID: entry.ID, Code: apiErr.Code, Message: apiErr.Message, SenderFault: apiErr.Sender})
			} else {
				failed = append(failed, domain.PublishBatchFailure{ID: entry.ID, Code: "InternalError", Message: err.Error(), SenderFault: false})
			}
			continue
		}
		success = append(success, domain.BatchDeliveryResult{ID: entry.ID, MessageID: out.MessageID, SequenceNumber: out.SequenceNumber})
	}
	return success, failed, nil
}

func (s *Service) dispatchPublish(ctx context.Context, topic *domain.Topic, input domain.PublishInput, messageID, dedupID, sequenceNumber string) error {
	selectedMessages, err := resolveProtocolMessages(input.Message, input.MessageStructure)
	if err != nil {
		return domain.NewInvalidParameter("%s", err.Error())
	}
	type sqsDelivery struct {
		topic           *domain.Topic
		sub             *domain.Subscription
		protocolMessage string
	}
	var sqsDeliveries []sqsDelivery
	err = s.store.Tx(ctx, func(tx storepkg.Tx) error {
		subs, _ := tx.ListSubscriptionsByTopic(topic.ARN, "", 10_000)
		for _, sub := range subs {
			if !sub.Confirmed || sub.PendingConfirmation {
				continue
			}
			matched, err := filter.Matches(sub.Attributes.FilterPolicy, sub.Attributes.FilterPolicyScope, selectedMessages["default"], input.MessageAttributes)
			if err != nil || !matched {
				continue
			}
			protocolMessage := selectedMessages[sub.Protocol]
			if protocolMessage == "" {
				protocolMessage = selectedMessages["default"]
			}
			if sub.Protocol == domain.ProtocolSQS {
				sqsDeliveries = append(sqsDeliveries, sqsDelivery{
					topic:           domain.CopyTopic(topic),
					sub:             domain.CopySubscription(sub),
					protocolMessage: protocolMessage,
				})
				continue
			}
			attempt, effectiveJSON, err := s.buildNotificationAttempt(topic, sub, protocolMessage, input, messageID, dedupID, sequenceNumber)
			if err != nil {
				return err
			}
			job, err := s.prepareDeliveryJob("notification", attempt, effectiveJSON, sub.Attributes.RawMessageDelivery, sub.Attributes.RedrivePolicy)
			if err != nil {
				return err
			}
			if err := tx.EnqueueDeliveryJob(job); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, item := range sqsDeliveries {
		if err := s.deliverToSQS(ctx, item.topic, item.sub, item.protocolMessage, input, messageID, dedupID, sequenceNumber); err != nil {
			payload := domain.DeliveryJobPayload{
				TopicARN:        item.topic.ARN,
				SubscriptionARN: item.sub.ARN,
				ProtocolMessage: item.protocolMessage,
				Timestamp:       s.clock.Now(),
				MessageID:       messageID,
				GroupID:         input.MessageGroupID,
				DeduplicationID: dedupID,
			}
			s.redrivePayload(ctx, payload, item.sub.Attributes.RedrivePolicy)
		}
	}
	return nil
}

func (s *Service) deliverToSQS(ctx context.Context, topic *domain.Topic, sub *domain.Subscription, protocolMessage string, input domain.PublishInput, messageID, dedupID, sequenceNumber string) error {
	if s.sqsClient == nil {
		return errors.New("sqs adapter not configured")
	}
	allowed, err := s.sqsClient.QueueAllowsTopic(ctx, sub.Endpoint, topic.ARN, topic.Owner)
	if err != nil {
		return err
	}
	if !allowed {
		return domain.NewAuthorization("Queue policy does not allow SNS delivery")
	}
	body := protocolMessage
	attrs := map[string]sqsproto.MessageAttribute{}
	if !sub.Attributes.RawMessageDelivery {
		signature := ""
		attempt, _, err := s.buildNotificationAttempt(topic, sub, protocolMessage, input, messageID, dedupID, sequenceNumber)
		if err == nil && attempt != nil {
			if sig, signErr := s.signer.Sign(attempt); signErr == nil {
				signature = sig
			}
		}
		envelope := map[string]any{
			"Type":             "Notification",
			"MessageId":        messageID,
			"TopicArn":         topic.ARN,
			"Message":          protocolMessage,
			"Timestamp":        domain.DefaultTimestamp(s.clock.Now()),
			"SignatureVersion": topic.Attributes.SignatureVersion,
			"Signature":        signature,
			"SigningCertURL":   s.signer.SigningCertURL(s.cfg.BaseURL),
			"UnsubscribeURL":   fmt.Sprintf("%s/?Action=Unsubscribe&SubscriptionArn=%s", strings.TrimRight(s.cfg.BaseURL, "/"), sub.ARN),
			"SubscriptionArn":  sub.ARN,
		}
		if input.Subject != "" {
			envelope["Subject"] = input.Subject
		}
		if len(input.MessageAttributes) > 0 {
			envelope["MessageAttributes"] = input.MessageAttributes
		}
		raw, _ := json.Marshal(envelope)
		body = string(raw)
	} else {
		if len(input.MessageAttributes) > 10 {
			return domain.NewInvalidParameter("Raw SQS delivery supports at most 10 message attributes")
		}
		for key, value := range input.MessageAttributes {
			attrs[key] = sqsproto.MessageAttribute{DataType: value.DataType, StringValue: value.StringValue, BinaryValue: value.BinaryValue}
		}
	}
	return s.sqsClient.SendMessage(ctx, sqsproto.SendRequest{
		QueueARN:               sub.Endpoint,
		Body:                   body,
		MessageAttributes:      attrs,
		MessageGroupID:         input.MessageGroupID,
		MessageDeduplicationID: dedupID,
	})
}

func (s *Service) buildNotificationAttempt(topic *domain.Topic, sub *domain.Subscription, protocolMessage string, input domain.PublishInput, messageID, dedupID, sequenceNumber string) (*domain.DeliveryAttempt, string, error) {
	_, effectiveJSON, err := delivery.EffectivePolicy(topic.Attributes.DeliveryPolicy, sub.Attributes.DeliveryPolicy)
	if err != nil {
		return nil, "", err
	}
	attempt := &domain.DeliveryAttempt{
		Subscription:      sub,
		Topic:             topic,
		MessageID:         messageID,
		Subject:           input.Subject,
		Message:           input.Message,
		ProtocolMessage:   protocolMessage,
		Timestamp:         s.clock.Now(),
		Type:              "Notification",
		UnsubscribeURL:    fmt.Sprintf("%s/?Action=Unsubscribe&SubscriptionArn=%s", strings.TrimRight(s.cfg.BaseURL, "/"), sub.ARN),
		SignatureVersion:  topic.Attributes.SignatureVersion,
		SigningCertURL:    s.signer.SigningCertURL(s.cfg.BaseURL),
		MessageAttributes: input.MessageAttributes,
		GroupID:           input.MessageGroupID,
		DeduplicationID:   dedupID,
		SequenceNumber:    sequenceNumber,
	}
	return attempt, effectiveJSON, nil
}

func (s *Service) buildConfirmationAttempt(topic *domain.Topic, sub *domain.Subscription, token domain.ConfirmationToken, messageType string) (*domain.DeliveryAttempt, string, error) {
	_, effectiveJSON, err := delivery.EffectivePolicy(topic.Attributes.DeliveryPolicy, sub.Attributes.DeliveryPolicy)
	if err != nil {
		return nil, "", err
	}
	message := "You have chosen to subscribe to the topic. To confirm the subscription, visit the SubscribeURL included in this message."
	if messageType == "UnsubscribeConfirmation" {
		message = fmt.Sprintf("You have chosen to deactivate subscription %s.\nTo cancel this operation and restore the subscription, visit the SubscribeURL included in this message.", sub.ARN)
	}
	attempt := &domain.DeliveryAttempt{
		Subscription:     sub,
		Topic:            topic,
		MessageID:        util.UUID(),
		ProtocolMessage:  message,
		Timestamp:        s.clock.Now(),
		Type:             messageType,
		Token:            token.Token,
		SubscribeURL:     fmt.Sprintf("%s/?Action=ConfirmSubscription&TopicArn=%s&Token=%s", strings.TrimRight(s.cfg.BaseURL, "/"), topic.ARN, token.Token),
		SignatureVersion: topic.Attributes.SignatureVersion,
		SigningCertURL:   s.signer.SigningCertURL(s.cfg.BaseURL),
	}
	return attempt, effectiveJSON, nil
}

func (s *Service) authorizeTopic(identity domain.Identity, topic *domain.Topic, action string) error {
	allowed, err := policy.Evaluate(topic.Attributes.Policy, policy.Request{
		Principal: identity.AccountID,
		Action:    action,
		Resource:  topic.ARN,
		Context: map[string]string{
			"AWS:SourceOwner": identity.AccountID,
		},
	}, topic.Owner)
	if err != nil {
		return domain.NewInvalidParameter("Invalid topic policy")
	}
	if !allowed {
		return domain.NewAuthorization("Access denied")
	}
	return nil
}

func (s *Service) applyTopicAttributes(topic *domain.Topic, attrs map[string]string, creating bool) error {
	for name, value := range attrs {
		switch name {
		case "Policy":
			if _, err := policy.Parse(value); err != nil {
				return domain.NewInvalidParameter("Invalid parameter: Policy")
			}
			topic.Attributes.Policy = value
		case "DeliveryPolicy":
			if err := delivery.ValidateTopicPolicy(value); err != nil {
				return domain.NewInvalidParameter("Invalid parameter: DeliveryPolicy")
			}
			topic.Attributes.DeliveryPolicy = value
		case "FifoTopic":
			if !creating {
				return domain.NewInvalidParameter("FifoTopic can only be specified at topic creation time")
			}
			topic.Attributes.FifoTopic = strings.EqualFold(value, "true")
		case "ContentBasedDeduplication":
			if !topic.Attributes.FifoTopic {
				return domain.NewInvalidParameter("ContentBasedDeduplication is only valid for FIFO topics")
			}
			topic.Attributes.ContentBasedDeduplication = strings.EqualFold(value, "true")
		case "FifoThroughputScope":
			if !topic.Attributes.FifoTopic {
				return domain.NewInvalidParameter("FifoThroughputScope is only valid for FIFO topics")
			}
			if value != domain.FifoThroughputTopic && value != domain.FifoThroughputMessageGroup {
				return domain.NewInvalidParameter("Invalid parameter: FifoThroughputScope")
			}
			if topic.Attributes.FifoThroughputScope == domain.FifoThroughputMessageGroup && value == domain.FifoThroughputTopic {
				return domain.NewInvalidParameter("FifoThroughputScope cannot be reverted from MessageGroup to Topic")
			}
			topic.Attributes.FifoThroughputScope = value
		case "ArchivePolicy":
			if strings.TrimSpace(value) != "" && !json.Valid([]byte(value)) {
				return domain.NewInvalidParameter("Invalid parameter: ArchivePolicy")
			}
			topic.Attributes.ArchivePolicy = value
		case "SignatureVersion":
			if value != "1" && value != "2" {
				return domain.NewInvalidParameter("Invalid parameter: SignatureVersion")
			}
			topic.Attributes.SignatureVersion = value
		case "TracingConfig":
			if value != "PassThrough" && value != "Active" {
				return domain.NewInvalidParameter("Invalid parameter: TracingConfig")
			}
			topic.Attributes.TracingConfig = value
		case "DisplayName":
			topic.Attributes.DisplayName = value
		case "KmsMasterKeyId",
			"HTTPSuccessFeedbackRoleArn", "HTTPSuccessFeedbackSampleRate", "HTTPFailureFeedbackRoleArn",
			"SQSSuccessFeedbackRoleArn", "SQSSuccessFeedbackSampleRate", "SQSFailureFeedbackRoleArn",
			"LambdaSuccessFeedbackRoleArn", "LambdaSuccessFeedbackSampleRate", "LambdaFailureFeedbackRoleArn",
			"FirehoseSuccessFeedbackRoleArn", "FirehoseSuccessFeedbackSampleRate", "FirehoseFailureFeedbackRoleArn",
			"ApplicationSuccessFeedbackRoleArn", "ApplicationSuccessFeedbackSampleRate", "ApplicationFailureFeedbackRoleArn":
			if topic.Attributes.Unsupported == nil {
				topic.Attributes.Unsupported = map[string]string{}
			}
			topic.Attributes.Unsupported[name] = value
		case "DataProtectionPolicy":
			return domain.NewInvalidParameter("Invalid parameter: DataProtectionPolicy")
		default:
			return domain.NewInvalidParameter("Invalid parameter: %s", name)
		}
	}
	topic.UpdatedAt = s.clock.Now()
	return nil
}

func (s *Service) applySubscriptionAttributes(sub *domain.Subscription, topic *domain.Topic, attrs map[string]string) error {
	for name, value := range attrs {
		switch name {
		case "DeliveryPolicy":
			if sub.Protocol != domain.ProtocolHTTP && sub.Protocol != domain.ProtocolHTTPS {
				return domain.NewInvalidParameter("Invalid parameter: DeliveryPolicy")
			}
			if err := delivery.ValidateSubscriptionPolicy(value); err != nil {
				return domain.NewInvalidParameter("Invalid parameter: DeliveryPolicy")
			}
			sub.Attributes.DeliveryPolicy = value
		case "FilterPolicy":
			if strings.TrimSpace(value) != "" && !json.Valid([]byte(value)) {
				return domain.NewInvalidParameter("Invalid parameter: FilterPolicy")
			}
			if err := filter.Validate(value); err != nil {
				return domain.NewInvalidParameter("Invalid parameter: FilterPolicy")
			}
			sub.Attributes.FilterPolicy = value
		case "FilterPolicyScope":
			if value != domain.FilterScopeMessageAttributes && value != domain.FilterScopeMessageBody {
				return domain.NewInvalidParameter("Invalid parameter: FilterPolicyScope")
			}
			sub.Attributes.FilterPolicyScope = value
		case "RawMessageDelivery":
			sub.Attributes.RawMessageDelivery = strings.EqualFold(value, "true")
		case "RedrivePolicy":
			if _, err := domain.ParseRedrivePolicy(value); err != nil {
				return domain.NewInvalidParameter("Invalid parameter: RedrivePolicy")
			}
			sub.Attributes.RedrivePolicy = value
		case "ReplayPolicy", "ReplayStatus":
			return domain.NewInvalidParameter("Invalid parameter: %s", name)
		default:
			return domain.NewInvalidParameter("Invalid parameter: %s", name)
		}
	}
	if _, effective, err := delivery.EffectivePolicy(topic.Attributes.DeliveryPolicy, sub.Attributes.DeliveryPolicy); err == nil {
		sub.Attributes.EffectiveDeliveryPolicy = effective
	}
	return nil
}

func (s *Service) validateSQSSubscription(ctx context.Context, topic *domain.Topic, sub *domain.Subscription) error {
	if s.sqsClient == nil {
		return domain.NewInvalidParameter("SQS integration is not configured")
	}
	queue, err := s.sqsClient.ResolveQueue(ctx, sub.Endpoint)
	if err != nil {
		return domain.NewInvalidParameter("Invalid parameter: Endpoint")
	}
	if queue.Region != topic.Region {
		return domain.NewInvalidParameter("Invalid parameter: Endpoint")
	}
	if topic.Attributes.FifoTopic {
		return nil
	}
	if queue.FIFO {
		return domain.NewInvalidParameter("Standard SNS topics cannot subscribe FIFO SQS queues")
	}
	return nil
}

func (s *Service) findExistingSubscription(topicARN, protocolName, endpoint string) *domain.Subscription {
	items, _ := s.store.ListSubscriptionsByTopic(topicARN, "", 10_000)
	for _, item := range items {
		if item.Protocol == protocolName && item.Endpoint == endpoint {
			return item
		}
	}
	return nil
}

func validatePublishInput(topic *domain.Topic, input domain.PublishInput) error {
	if len(input.Message) == 0 || len(input.Message) > 262144 {
		return domain.NewInvalidParameter("Invalid parameter: Message")
	}
	if input.MessageStructure != "" && input.MessageStructure != "json" {
		return domain.NewInvalidParameter("Invalid parameter: MessageStructure")
	}
	if input.MessageStructure == "json" && len(input.MessageAttributes) > 0 {
		return domain.NewInvalidParameter("Message attributes are only supported when MessageStructure is String")
	}
	if topic.Attributes.FifoTopic {
		if err := util.ValidateFIFOIdentifier(input.MessageGroupID, "MessageGroupId"); err != nil {
			return domain.NewInvalidParameter("%s", err.Error())
		}
		if !topic.Attributes.ContentBasedDeduplication {
			if err := util.ValidateFIFOIdentifier(input.MessageDeduplicationID, "MessageDeduplicationId"); err != nil {
				return domain.NewInvalidParameter("%s", err.Error())
			}
		}
	} else if input.MessageGroupID != "" {
		if err := util.ValidateFIFOIdentifier(input.MessageGroupID, "MessageGroupId"); err != nil {
			return domain.NewInvalidParameter("%s", err.Error())
		}
	}
	if input.Subject != "" {
		if len(input.Subject) >= 100 || strings.ContainsAny(input.Subject, "\r\n") {
			return domain.NewInvalidParameter("Invalid parameter: Subject")
		}
	}
	if err := validateMessageAttributes(input.MessageAttributes); err != nil {
		return err
	}
	return nil
}

func validateMessageAttributes(attrs map[string]domain.MessageAttributeValue) error {
	for key, value := range attrs {
		if strings.HasPrefix(strings.ToLower(key), "aws.") || strings.HasPrefix(strings.ToLower(key), "amazon.") {
			return domain.NewInvalidParameter("Message attribute names cannot start with AWS. or Amazon.")
		}
		switch value.DataType {
		case "String", "String.Array", "Number", "Binary":
		default:
			return domain.NewInvalidParameter("Invalid message attribute type")
		}
	}
	return nil
}

func resolveProtocolMessages(message, structure string) (map[string]string, error) {
	if structure == "" {
		return map[string]string{"default": message}, nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(message), &payload); err != nil {
		return nil, fmt.Errorf("Invalid parameter: Message")
	}
	result := map[string]string{}
	for key, value := range payload {
		if str, ok := value.(string); ok {
			result[key] = str
		}
	}
	if result["default"] == "" {
		return nil, fmt.Errorf("Invalid parameter: Message")
	}
	return result, nil
}

func (s *Service) pageSize() int {
	if s.cfg.PageSize <= 0 {
		return 100
	}
	return s.cfg.PageSize
}

func (s *Service) SetBaseURL(baseURL string) {
	s.cfg.BaseURL = strings.TrimRight(baseURL, "/")
}

func SortedTags(tags map[string]string) []struct{ Key, Value string } {
	items := make([]struct{ Key, Value string }, 0, len(tags))
	for k, v := range tags {
		items = append(items, struct{ Key, Value string }{Key: k, Value: v})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	return items
}
