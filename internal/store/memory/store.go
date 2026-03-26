package memory

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"emulator-aws-sns/internal/domain"
)

type Store struct {
	mu            sync.RWMutex
	topics        map[string]*domain.Topic
	topicsByName  map[string]string
	subscriptions map[string]*domain.Subscription
	subsByTopic   map[string]map[string]struct{}
	confirmation  map[string]domain.ConfirmationToken
}

func NewStore() *Store {
	return &Store{
		topics:        map[string]*domain.Topic{},
		topicsByName:  map[string]string{},
		subscriptions: map[string]*domain.Subscription{},
		subsByTopic:   map[string]map[string]struct{}{},
		confirmation:  map[string]domain.ConfirmationToken{},
	}
}

func (s *Store) CreateTopic(topic *domain.Topic) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if arn, ok := s.topicsByName[topic.Name]; ok {
		existing := s.topics[arn]
		topic.ARN = existing.ARN
		return domain.ErrConflict
	}
	s.topics[topic.ARN] = domain.CopyTopic(topic)
	s.topicsByName[topic.Name] = topic.ARN
	return nil
}

func (s *Store) GetTopic(arn string) (*domain.Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	topic, ok := s.topics[arn]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return domain.CopyTopic(topic), nil
}

func (s *Store) FindTopicByName(name string) (*domain.Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	arn, ok := s.topicsByName[name]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return domain.CopyTopic(s.topics[arn]), nil
}

func (s *Store) UpdateTopic(arn string, fn func(*domain.Topic) error) (*domain.Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.topics[arn]
	if !ok {
		return nil, domain.ErrNotFound
	}
	copy := domain.CopyTopic(current)
	if err := fn(copy); err != nil {
		return nil, err
	}
	s.topics[arn] = domain.CopyTopic(copy)
	return domain.CopyTopic(copy), nil
}

func (s *Store) DeleteTopic(arn string) ([]*domain.Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	topic, ok := s.topics[arn]
	if !ok {
		return nil, nil
	}
	delete(s.topics, arn)
	delete(s.topicsByName, topic.Name)
	var removed []*domain.Subscription
	for subARN := range s.subsByTopic[arn] {
		if sub, ok := s.subscriptions[subARN]; ok {
			removed = append(removed, domain.CopySubscription(sub))
			delete(s.subscriptions, subARN)
		}
	}
	delete(s.subsByTopic, arn)
	return removed, nil
}

func (s *Store) ListTopics(nextToken string, pageSize int) ([]*domain.Topic, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.topics))
	for arn := range s.topics {
		keys = append(keys, arn)
	}
	sort.Strings(keys)
	start := decodePageToken(nextToken)
	if start >= len(keys) {
		return nil, ""
	}
	end := start + pageSize
	if pageSize <= 0 || end > len(keys) {
		end = len(keys)
	}
	out := make([]*domain.Topic, 0, end-start)
	for _, arn := range keys[start:end] {
		out = append(out, domain.CopyTopic(s.topics[arn]))
	}
	token := ""
	if end < len(keys) {
		token = encodePageToken(end)
	}
	return out, token
}

func (s *Store) CreateSubscription(sub *domain.Subscription) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions[sub.ARN] = domain.CopySubscription(sub)
	if _, ok := s.subsByTopic[sub.TopicARN]; !ok {
		s.subsByTopic[sub.TopicARN] = map[string]struct{}{}
	}
	s.subsByTopic[sub.TopicARN][sub.ARN] = struct{}{}
	return nil
}

func (s *Store) GetSubscription(arn string) (*domain.Subscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sub, ok := s.subscriptions[arn]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return domain.CopySubscription(sub), nil
}

func (s *Store) UpdateSubscription(arn string, fn func(*domain.Subscription) error) (*domain.Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.subscriptions[arn]
	if !ok {
		return nil, domain.ErrNotFound
	}
	copy := domain.CopySubscription(current)
	if err := fn(copy); err != nil {
		return nil, err
	}
	s.subscriptions[arn] = domain.CopySubscription(copy)
	return domain.CopySubscription(copy), nil
}

func (s *Store) DeleteSubscription(arn string) (*domain.Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subscriptions[arn]
	if !ok {
		return nil, domain.ErrNotFound
	}
	delete(s.subscriptions, arn)
	if topicSubs, ok := s.subsByTopic[sub.TopicARN]; ok {
		delete(topicSubs, arn)
	}
	return domain.CopySubscription(sub), nil
}

func (s *Store) ListSubscriptions(nextToken string, pageSize int) ([]*domain.Subscription, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.subscriptions))
	for arn := range s.subscriptions {
		keys = append(keys, arn)
	}
	sort.Strings(keys)
	start := decodePageToken(nextToken)
	if start >= len(keys) {
		return nil, ""
	}
	end := start + pageSize
	if pageSize <= 0 || end > len(keys) {
		end = len(keys)
	}
	out := make([]*domain.Subscription, 0, end-start)
	for _, arn := range keys[start:end] {
		out = append(out, domain.CopySubscription(s.subscriptions[arn]))
	}
	token := ""
	if end < len(keys) {
		token = encodePageToken(end)
	}
	return out, token
}

func (s *Store) ListSubscriptionsByTopic(topicARN, nextToken string, pageSize int) ([]*domain.Subscription, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	subSet := s.subsByTopic[topicARN]
	keys := make([]string, 0, len(subSet))
	for arn := range subSet {
		keys = append(keys, arn)
	}
	sort.Strings(keys)
	start := decodePageToken(nextToken)
	if start >= len(keys) {
		return nil, ""
	}
	end := start + pageSize
	if pageSize <= 0 || end > len(keys) {
		end = len(keys)
	}
	out := make([]*domain.Subscription, 0, end-start)
	for _, arn := range keys[start:end] {
		out = append(out, domain.CopySubscription(s.subscriptions[arn]))
	}
	token := ""
	if end < len(keys) {
		token = encodePageToken(end)
	}
	return out, token
}

func (s *Store) PutConfirmationToken(token domain.ConfirmationToken) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.confirmation[token.Token] = token
}

func (s *Store) GetConfirmationToken(token string) (domain.ConfirmationToken, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.confirmation[token]
	if !ok {
		return domain.ConfirmationToken{}, domain.ErrNotFound
	}
	return t, nil
}

func (s *Store) DeleteConfirmationToken(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.confirmation, token)
}

func encodePageToken(i int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(i)))
}

func decodePageToken(token string) int {
	if strings.TrimSpace(token) == "" {
		return 0
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0
	}
	n, err := strconv.Atoi(string(raw))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func (s *Store) DebugString() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("topics=%d subscriptions=%d confirmations=%d", len(s.topics), len(s.subscriptions), len(s.confirmation))
}
