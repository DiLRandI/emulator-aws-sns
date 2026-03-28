package memory

import (
	"context"
	"encoding/base64"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"emulator-aws-sns/internal/domain"
	"emulator-aws-sns/internal/store"
)

var _ store.Store = (*Store)(nil)

type Store struct {
	mu    sync.RWMutex
	state *state
}

type state struct {
	topics          map[string]*domain.Topic
	topicsByName    map[string]string
	subscriptions   map[string]*domain.Subscription
	subsByTopic     map[string]map[string]struct{}
	confirmation    map[string]domain.ConfirmationToken
	deliveryJobs    map[string]*domain.DeliveryJob
	signingMaterial domain.SigningMaterial
}

type txStore struct {
	state *state
}

func NewStore() *Store {
	return &Store{
		state: &state{
			topics:        map[string]*domain.Topic{},
			topicsByName:  map[string]string{},
			subscriptions: map[string]*domain.Subscription{},
			subsByTopic:   map[string]map[string]struct{}{},
			confirmation:  map[string]domain.ConfirmationToken{},
			deliveryJobs:  map[string]*domain.DeliveryJob{},
		},
	}
}

func (s *Store) Tx(_ context.Context, fn func(store.Tx) error) error {
	s.mu.RLock()
	working := cloneState(s.state)
	s.mu.RUnlock()

	tx := &txStore{state: working}
	if err := fn(tx); err != nil {
		return err
	}

	s.mu.Lock()
	s.state = working
	s.mu.Unlock()
	return nil
}

func (s *Store) CreateTopic(topic *domain.Topic) error {
	return s.Tx(context.Background(), func(tx store.Tx) error { return tx.CreateTopic(topic) })
}

func (s *Store) GetTopic(arn string) (*domain.Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return getTopic(s.state, arn)
}

func (s *Store) FindTopicByName(name string) (*domain.Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return findTopicByName(s.state, name)
}

func (s *Store) UpdateTopic(arn string, fn func(*domain.Topic) error) (*domain.Topic, error) {
	var updated *domain.Topic
	err := s.Tx(context.Background(), func(tx store.Tx) error {
		var err error
		updated, err = tx.UpdateTopic(arn, fn)
		return err
	})
	return updated, err
}

func (s *Store) DeleteTopic(arn string) ([]*domain.Subscription, error) {
	var removed []*domain.Subscription
	err := s.Tx(context.Background(), func(tx store.Tx) error {
		var err error
		removed, err = tx.DeleteTopic(arn)
		return err
	})
	return removed, err
}

func (s *Store) ListTopics(nextToken string, pageSize int) ([]*domain.Topic, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return listTopics(s.state, nextToken, pageSize)
}

func (s *Store) CreateSubscription(sub *domain.Subscription) error {
	return s.Tx(context.Background(), func(tx store.Tx) error { return tx.CreateSubscription(sub) })
}

func (s *Store) GetSubscription(arn string) (*domain.Subscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return getSubscription(s.state, arn)
}

func (s *Store) UpdateSubscription(arn string, fn func(*domain.Subscription) error) (*domain.Subscription, error) {
	var updated *domain.Subscription
	err := s.Tx(context.Background(), func(tx store.Tx) error {
		var err error
		updated, err = tx.UpdateSubscription(arn, fn)
		return err
	})
	return updated, err
}

func (s *Store) DeleteSubscription(arn string) (*domain.Subscription, error) {
	var removed *domain.Subscription
	err := s.Tx(context.Background(), func(tx store.Tx) error {
		var err error
		removed, err = tx.DeleteSubscription(arn)
		return err
	})
	return removed, err
}

func (s *Store) ListSubscriptions(nextToken string, pageSize int) ([]*domain.Subscription, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return listSubscriptions(s.state, nextToken, pageSize)
}

func (s *Store) ListSubscriptionsByTopic(topicARN, nextToken string, pageSize int) ([]*domain.Subscription, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return listSubscriptionsByTopic(s.state, topicARN, nextToken, pageSize)
}

func (s *Store) PutConfirmationToken(token domain.ConfirmationToken) {
	_ = s.Tx(context.Background(), func(tx store.Tx) error {
		tx.PutConfirmationToken(token)
		return nil
	})
}

func (s *Store) GetConfirmationToken(token string) (domain.ConfirmationToken, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return getConfirmationToken(s.state, token)
}

func (s *Store) DeleteConfirmationToken(token string) {
	_ = s.Tx(context.Background(), func(tx store.Tx) error {
		tx.DeleteConfirmationToken(token)
		return nil
	})
}

func (s *Store) EnqueueDeliveryJob(job *domain.DeliveryJob) error {
	return s.Tx(context.Background(), func(tx store.Tx) error { return tx.EnqueueDeliveryJob(job) })
}

func (s *Store) LoadSigningMaterial() (domain.SigningMaterial, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return copySigningMaterial(s.state.signingMaterial), nil
}

func (s *Store) SaveSigningMaterial(material domain.SigningMaterial) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.signingMaterial = copySigningMaterial(material)
	return nil
}

func (s *Store) ListReadyDeliveryJobs(now time.Time, limit int) ([]*domain.DeliveryJob, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.state.deliveryJobs))
	for id, job := range s.state.deliveryJobs {
		if !job.NextAttemptAt.After(now) {
			keys = append(keys, id)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		a := s.state.deliveryJobs[keys[i]]
		b := s.state.deliveryJobs[keys[j]]
		if a.NextAttemptAt.Equal(b.NextAttemptAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return a.NextAttemptAt.Before(b.NextAttemptAt)
	})
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]*domain.DeliveryJob, 0, len(keys))
	for _, id := range keys {
		out = append(out, domain.CopyDeliveryJob(s.state.deliveryJobs[id]))
	}
	return out, nil
}

func (s *Store) UpdateDeliveryJob(id string, fn func(*domain.DeliveryJob) error) (*domain.DeliveryJob, error) {
	var updated *domain.DeliveryJob
	err := s.Tx(context.Background(), func(tx store.Tx) error {
		txx := tx.(*txStore)
		current, ok := txx.state.deliveryJobs[id]
		if !ok {
			return domain.ErrNotFound
		}
		copyJob := domain.CopyDeliveryJob(current)
		if err := fn(copyJob); err != nil {
			return err
		}
		txx.state.deliveryJobs[id] = domain.CopyDeliveryJob(copyJob)
		updated = domain.CopyDeliveryJob(copyJob)
		return nil
	})
	return updated, err
}

func (s *Store) DeleteDeliveryJob(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.state.deliveryJobs, id)
	return nil
}

func (s *Store) Health(context.Context) error { return nil }

func (s *Store) Close() error { return nil }

func (s *Store) DebugString() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("topics=%d subscriptions=%d confirmations=%d jobs=%d", len(s.state.topics), len(s.state.subscriptions), len(s.state.confirmation), len(s.state.deliveryJobs))
}

func (tx *txStore) CreateTopic(topic *domain.Topic) error {
	if arn, ok := tx.state.topicsByName[topic.Name]; ok {
		existing := tx.state.topics[arn]
		topic.ARN = existing.ARN
		return domain.ErrConflict
	}
	tx.state.topics[topic.ARN] = domain.CopyTopic(topic)
	tx.state.topicsByName[topic.Name] = topic.ARN
	return nil
}

func (tx *txStore) GetTopic(arn string) (*domain.Topic, error) {
	return getTopic(tx.state, arn)
}

func (tx *txStore) FindTopicByName(name string) (*domain.Topic, error) {
	return findTopicByName(tx.state, name)
}

func (tx *txStore) UpdateTopic(arn string, fn func(*domain.Topic) error) (*domain.Topic, error) {
	current, ok := tx.state.topics[arn]
	if !ok {
		return nil, domain.ErrNotFound
	}
	copyTopic := domain.CopyTopic(current)
	if err := fn(copyTopic); err != nil {
		return nil, err
	}
	tx.state.topics[arn] = domain.CopyTopic(copyTopic)
	return domain.CopyTopic(copyTopic), nil
}

func (tx *txStore) DeleteTopic(arn string) ([]*domain.Subscription, error) {
	topic, ok := tx.state.topics[arn]
	if !ok {
		return nil, nil
	}
	delete(tx.state.topics, arn)
	delete(tx.state.topicsByName, topic.Name)
	var removed []*domain.Subscription
	for subARN := range tx.state.subsByTopic[arn] {
		if sub, ok := tx.state.subscriptions[subARN]; ok {
			removed = append(removed, domain.CopySubscription(sub))
			delete(tx.state.subscriptions, subARN)
		}
	}
	delete(tx.state.subsByTopic, arn)
	for token, record := range tx.state.confirmation {
		if record.TopicARN == arn {
			delete(tx.state.confirmation, token)
		}
	}
	for id, job := range tx.state.deliveryJobs {
		if job.Payload.TopicARN == arn {
			delete(tx.state.deliveryJobs, id)
		}
	}
	return removed, nil
}

func (tx *txStore) ListTopics(nextToken string, pageSize int) ([]*domain.Topic, string) {
	return listTopics(tx.state, nextToken, pageSize)
}

func (tx *txStore) CreateSubscription(sub *domain.Subscription) error {
	tx.state.subscriptions[sub.ARN] = domain.CopySubscription(sub)
	if _, ok := tx.state.subsByTopic[sub.TopicARN]; !ok {
		tx.state.subsByTopic[sub.TopicARN] = map[string]struct{}{}
	}
	tx.state.subsByTopic[sub.TopicARN][sub.ARN] = struct{}{}
	return nil
}

func (tx *txStore) GetSubscription(arn string) (*domain.Subscription, error) {
	return getSubscription(tx.state, arn)
}

func (tx *txStore) UpdateSubscription(arn string, fn func(*domain.Subscription) error) (*domain.Subscription, error) {
	current, ok := tx.state.subscriptions[arn]
	if !ok {
		return nil, domain.ErrNotFound
	}
	copySub := domain.CopySubscription(current)
	if err := fn(copySub); err != nil {
		return nil, err
	}
	tx.state.subscriptions[arn] = domain.CopySubscription(copySub)
	return domain.CopySubscription(copySub), nil
}

func (tx *txStore) DeleteSubscription(arn string) (*domain.Subscription, error) {
	sub, ok := tx.state.subscriptions[arn]
	if !ok {
		return nil, domain.ErrNotFound
	}
	delete(tx.state.subscriptions, arn)
	if topicSubs, ok := tx.state.subsByTopic[sub.TopicARN]; ok {
		delete(topicSubs, arn)
	}
	for token, record := range tx.state.confirmation {
		if record.SubscriptionARN == arn {
			delete(tx.state.confirmation, token)
		}
	}
	return domain.CopySubscription(sub), nil
}

func (tx *txStore) ListSubscriptions(nextToken string, pageSize int) ([]*domain.Subscription, string) {
	return listSubscriptions(tx.state, nextToken, pageSize)
}

func (tx *txStore) ListSubscriptionsByTopic(topicARN, nextToken string, pageSize int) ([]*domain.Subscription, string) {
	return listSubscriptionsByTopic(tx.state, topicARN, nextToken, pageSize)
}

func (tx *txStore) PutConfirmationToken(token domain.ConfirmationToken) {
	tx.state.confirmation[token.Token] = domain.CopyConfirmationToken(token)
}

func (tx *txStore) GetConfirmationToken(token string) (domain.ConfirmationToken, error) {
	return getConfirmationToken(tx.state, token)
}

func (tx *txStore) DeleteConfirmationToken(token string) {
	delete(tx.state.confirmation, token)
}

func (tx *txStore) EnqueueDeliveryJob(job *domain.DeliveryJob) error {
	tx.state.deliveryJobs[job.ID] = domain.CopyDeliveryJob(job)
	return nil
}

func getTopic(s *state, arn string) (*domain.Topic, error) {
	topic, ok := s.topics[arn]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return domain.CopyTopic(topic), nil
}

func findTopicByName(s *state, name string) (*domain.Topic, error) {
	arn, ok := s.topicsByName[name]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return domain.CopyTopic(s.topics[arn]), nil
}

func listTopics(s *state, nextToken string, pageSize int) ([]*domain.Topic, string) {
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

func getSubscription(s *state, arn string) (*domain.Subscription, error) {
	sub, ok := s.subscriptions[arn]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return domain.CopySubscription(sub), nil
}

func listSubscriptions(s *state, nextToken string, pageSize int) ([]*domain.Subscription, string) {
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

func listSubscriptionsByTopic(s *state, topicARN, nextToken string, pageSize int) ([]*domain.Subscription, string) {
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

func getConfirmationToken(s *state, token string) (domain.ConfirmationToken, error) {
	t, ok := s.confirmation[token]
	if !ok {
		return domain.ConfirmationToken{}, domain.ErrNotFound
	}
	return domain.CopyConfirmationToken(t), nil
}

func cloneState(src *state) *state {
	dst := &state{
		topics:          map[string]*domain.Topic{},
		topicsByName:    map[string]string{},
		subscriptions:   map[string]*domain.Subscription{},
		subsByTopic:     map[string]map[string]struct{}{},
		confirmation:    map[string]domain.ConfirmationToken{},
		deliveryJobs:    map[string]*domain.DeliveryJob{},
		signingMaterial: copySigningMaterial(src.signingMaterial),
	}
	for k, v := range src.topics {
		dst.topics[k] = domain.CopyTopic(v)
	}
	maps.Copy(dst.topicsByName, src.topicsByName)
	for k, v := range src.subscriptions {
		dst.subscriptions[k] = domain.CopySubscription(v)
	}
	for topicARN, subSet := range src.subsByTopic {
		dst.subsByTopic[topicARN] = map[string]struct{}{}
		for subARN := range subSet {
			dst.subsByTopic[topicARN][subARN] = struct{}{}
		}
	}
	for k, v := range src.confirmation {
		dst.confirmation[k] = domain.CopyConfirmationToken(v)
	}
	for k, v := range src.deliveryJobs {
		dst.deliveryJobs[k] = domain.CopyDeliveryJob(v)
	}
	return dst
}

func copySigningMaterial(src domain.SigningMaterial) domain.SigningMaterial {
	return domain.SigningMaterial{
		PrivateKeyPEM: append([]byte(nil), src.PrivateKeyPEM...),
		CertPEM:       append([]byte(nil), src.CertPEM...),
		CreatedAt:     src.CreatedAt,
	}
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
