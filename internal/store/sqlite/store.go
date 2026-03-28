package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"emulator-aws-sns/internal/domain"
	"emulator-aws-sns/internal/store"

	_ "modernc.org/sqlite"
)

const schemaVersion = 1

var _ store.Store = (*Store)(nil)

type Store struct {
	db *sql.DB
}

type txStore struct {
	tx *sql.Tx
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("sqlite path is required")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	st := &Store{db: db}
	if err := st.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return st, nil
}

func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{
		`PRAGMA journal_mode = WAL`,
		`PRAGMA foreign_keys = ON`,
		`PRAGMA busy_timeout = 5000`,
		`CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			applied_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS topics (
			arn TEXT PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			owner_account TEXT NOT NULL,
			region TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			data_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS subscriptions (
			arn TEXT PRIMARY KEY,
			topic_arn TEXT NOT NULL,
			owner_account TEXT NOT NULL,
			protocol TEXT NOT NULL,
			endpoint TEXT NOT NULL,
			confirmed INTEGER NOT NULL,
			pending_confirmation INTEGER NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			data_json TEXT NOT NULL,
			FOREIGN KEY(topic_arn) REFERENCES topics(arn) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_subscriptions_topic ON subscriptions(topic_arn, arn)`,
		`CREATE TABLE IF NOT EXISTS confirmation_tokens (
			token TEXT PRIMARY KEY,
			topic_arn TEXT NOT NULL,
			subscription_arn TEXT,
			endpoint TEXT NOT NULL,
			protocol TEXT NOT NULL,
			expires_at TEXT NOT NULL,
			kind TEXT NOT NULL,
			data_json TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_confirmation_tokens_topic ON confirmation_tokens(topic_arn)`,
		`CREATE TABLE IF NOT EXISTS signing_material (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			private_key_pem BLOB NOT NULL,
			cert_pem BLOB NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS delivery_jobs (
			id TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			next_attempt_at TEXT NOT NULL,
			attempt_count INTEGER NOT NULL,
			last_error TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			effective_delivery_json TEXT NOT NULL,
			raw_message_delivery INTEGER NOT NULL,
			redrive_policy TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_delivery_jobs_next_attempt ON delivery_jobs(next_attempt_at, created_at)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)`, schemaVersion, formatTime(time.Now().UTC()))
	return err
}

func (s *Store) Tx(ctx context.Context, fn func(store.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	wrapped := &txStore{tx: tx}
	if err := fn(wrapped); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *Store) CreateTopic(topic *domain.Topic) error {
	return s.Tx(context.Background(), func(tx store.Tx) error { return tx.CreateTopic(topic) })
}

func (s *Store) GetTopic(arn string) (*domain.Topic, error) {
	return getTopic(context.Background(), s.db, arn)
}

func (s *Store) FindTopicByName(name string) (*domain.Topic, error) {
	return findTopicByName(context.Background(), s.db, name)
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
	return listTopics(context.Background(), s.db, nextToken, pageSize)
}

func (s *Store) CreateSubscription(sub *domain.Subscription) error {
	return s.Tx(context.Background(), func(tx store.Tx) error { return tx.CreateSubscription(sub) })
}

func (s *Store) GetSubscription(arn string) (*domain.Subscription, error) {
	return getSubscription(context.Background(), s.db, arn)
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
	return listSubscriptions(context.Background(), s.db, "", nextToken, pageSize)
}

func (s *Store) ListSubscriptionsByTopic(topicARN, nextToken string, pageSize int) ([]*domain.Subscription, string) {
	return listSubscriptions(context.Background(), s.db, topicARN, nextToken, pageSize)
}

func (s *Store) PutConfirmationToken(token domain.ConfirmationToken) {
	_ = s.Tx(context.Background(), func(tx store.Tx) error {
		tx.PutConfirmationToken(token)
		return nil
	})
}

func (s *Store) GetConfirmationToken(token string) (domain.ConfirmationToken, error) {
	return getConfirmationToken(context.Background(), s.db, token)
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
	var keyPEM, certPEM []byte
	var createdAt string
	err := s.db.QueryRowContext(context.Background(), `SELECT private_key_pem, cert_pem, created_at FROM signing_material WHERE id = 1`).Scan(&keyPEM, &certPEM, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.SigningMaterial{}, nil
		}
		return domain.SigningMaterial{}, err
	}
	parsed, err := parseTime(createdAt)
	if err != nil {
		return domain.SigningMaterial{}, err
	}
	return domain.SigningMaterial{
		PrivateKeyPEM: append([]byte(nil), keyPEM...),
		CertPEM:       append([]byte(nil), certPEM...),
		CreatedAt:     parsed,
	}, nil
}

func (s *Store) SaveSigningMaterial(material domain.SigningMaterial) error {
	_, err := s.db.ExecContext(context.Background(), `
		INSERT INTO signing_material(id, private_key_pem, cert_pem, created_at)
		VALUES (1, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET private_key_pem=excluded.private_key_pem, cert_pem=excluded.cert_pem, created_at=excluded.created_at
	`, material.PrivateKeyPEM, material.CertPEM, formatTime(material.CreatedAt))
	return err
}

func (s *Store) ListReadyDeliveryJobs(now time.Time, limit int) ([]*domain.DeliveryJob, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(context.Background(), `
		SELECT id, kind, next_attempt_at, attempt_count, last_error, created_at, updated_at, payload_json, effective_delivery_json, raw_message_delivery, redrive_policy
		FROM delivery_jobs
		WHERE next_attempt_at <= ?
		ORDER BY next_attempt_at ASC, created_at ASC
		LIMIT ?
	`, formatTime(now), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.DeliveryJob
	for rows.Next() {
		job, err := scanDeliveryJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, job)
	}
	return out, rows.Err()
}

func (s *Store) UpdateDeliveryJob(id string, fn func(*domain.DeliveryJob) error) (*domain.DeliveryJob, error) {
	var updated *domain.DeliveryJob
	err := s.Tx(context.Background(), func(tx store.Tx) error {
		txx := tx.(*txStore)
		job, err := getDeliveryJob(context.Background(), txx.tx, id)
		if err != nil {
			return err
		}
		if err := fn(job); err != nil {
			return err
		}
		if err := saveDeliveryJob(context.Background(), txx.tx, job); err != nil {
			return err
		}
		updated = domain.CopyDeliveryJob(job)
		return nil
	})
	return updated, err
}

func (s *Store) DeleteDeliveryJob(id string) error {
	_, err := s.db.ExecContext(context.Background(), `DELETE FROM delivery_jobs WHERE id = ?`, id)
	return err
}

func (s *Store) Health(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (tx *txStore) CreateTopic(topic *domain.Topic) error {
	_, err := tx.tx.ExecContext(context.Background(), `
		INSERT INTO topics(arn, name, owner_account, region, created_at, updated_at, data_json)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, topic.ARN, topic.Name, topic.Owner, topic.Region, formatTime(topic.CreatedAt), formatTime(topic.UpdatedAt), mustJSON(topic))
	if err != nil {
		if isUniqueErr(err) {
			if existing, lookupErr := findTopicByName(context.Background(), tx.tx, topic.Name); lookupErr == nil {
				topic.ARN = existing.ARN
				return domain.ErrConflict
			}
		}
		return err
	}
	return nil
}

func (tx *txStore) GetTopic(arn string) (*domain.Topic, error) {
	return getTopic(context.Background(), tx.tx, arn)
}

func (tx *txStore) FindTopicByName(name string) (*domain.Topic, error) {
	return findTopicByName(context.Background(), tx.tx, name)
}

func (tx *txStore) UpdateTopic(arn string, fn func(*domain.Topic) error) (*domain.Topic, error) {
	topic, err := getTopic(context.Background(), tx.tx, arn)
	if err != nil {
		return nil, err
	}
	if err := fn(topic); err != nil {
		return nil, err
	}
	_, err = tx.tx.ExecContext(context.Background(), `
		UPDATE topics
		SET name = ?, owner_account = ?, region = ?, created_at = ?, updated_at = ?, data_json = ?
		WHERE arn = ?
	`, topic.Name, topic.Owner, topic.Region, formatTime(topic.CreatedAt), formatTime(topic.UpdatedAt), mustJSON(topic), arn)
	if err != nil {
		return nil, err
	}
	return topic, nil
}

func (tx *txStore) DeleteTopic(arn string) ([]*domain.Subscription, error) {
	subs, _ := listSubscriptions(context.Background(), tx.tx, arn, "", 100000)
	if _, err := tx.tx.ExecContext(context.Background(), `DELETE FROM topics WHERE arn = ?`, arn); err != nil {
		return nil, err
	}
	if _, err := tx.tx.ExecContext(context.Background(), `DELETE FROM confirmation_tokens WHERE topic_arn = ?`, arn); err != nil {
		return nil, err
	}
	if _, err := tx.tx.ExecContext(context.Background(), `DELETE FROM delivery_jobs WHERE json_extract(payload_json, '$.TopicARN') = ?`, arn); err != nil {
		return nil, err
	}
	return subs, nil
}

func (tx *txStore) ListTopics(nextToken string, pageSize int) ([]*domain.Topic, string) {
	return listTopics(context.Background(), tx.tx, nextToken, pageSize)
}

func (tx *txStore) CreateSubscription(sub *domain.Subscription) error {
	_, err := tx.tx.ExecContext(context.Background(), `
		INSERT INTO subscriptions(arn, topic_arn, owner_account, protocol, endpoint, confirmed, pending_confirmation, created_at, updated_at, data_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, sub.ARN, sub.TopicARN, sub.Owner, sub.Protocol, sub.Endpoint, boolInt(sub.Confirmed), boolInt(sub.PendingConfirmation), formatTime(sub.CreatedAt), formatTime(sub.UpdatedAt), mustJSON(sub))
	return err
}

func (tx *txStore) GetSubscription(arn string) (*domain.Subscription, error) {
	return getSubscription(context.Background(), tx.tx, arn)
}

func (tx *txStore) UpdateSubscription(arn string, fn func(*domain.Subscription) error) (*domain.Subscription, error) {
	sub, err := getSubscription(context.Background(), tx.tx, arn)
	if err != nil {
		return nil, err
	}
	if err := fn(sub); err != nil {
		return nil, err
	}
	_, err = tx.tx.ExecContext(context.Background(), `
		UPDATE subscriptions
		SET topic_arn = ?, owner_account = ?, protocol = ?, endpoint = ?, confirmed = ?, pending_confirmation = ?, created_at = ?, updated_at = ?, data_json = ?
		WHERE arn = ?
	`, sub.TopicARN, sub.Owner, sub.Protocol, sub.Endpoint, boolInt(sub.Confirmed), boolInt(sub.PendingConfirmation), formatTime(sub.CreatedAt), formatTime(sub.UpdatedAt), mustJSON(sub), arn)
	if err != nil {
		return nil, err
	}
	return sub, nil
}

func (tx *txStore) DeleteSubscription(arn string) (*domain.Subscription, error) {
	sub, err := getSubscription(context.Background(), tx.tx, arn)
	if err != nil {
		return nil, err
	}
	if _, err := tx.tx.ExecContext(context.Background(), `DELETE FROM subscriptions WHERE arn = ?`, arn); err != nil {
		return nil, err
	}
	if _, err := tx.tx.ExecContext(context.Background(), `DELETE FROM confirmation_tokens WHERE subscription_arn = ?`, arn); err != nil {
		return nil, err
	}
	return sub, nil
}

func (tx *txStore) ListSubscriptions(nextToken string, pageSize int) ([]*domain.Subscription, string) {
	return listSubscriptions(context.Background(), tx.tx, "", nextToken, pageSize)
}

func (tx *txStore) ListSubscriptionsByTopic(topicARN, nextToken string, pageSize int) ([]*domain.Subscription, string) {
	return listSubscriptions(context.Background(), tx.tx, topicARN, nextToken, pageSize)
}

func (tx *txStore) PutConfirmationToken(token domain.ConfirmationToken) {
	_, _ = tx.tx.ExecContext(context.Background(), `
		INSERT INTO confirmation_tokens(token, topic_arn, subscription_arn, endpoint, protocol, expires_at, kind, data_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(token) DO UPDATE SET topic_arn=excluded.topic_arn, subscription_arn=excluded.subscription_arn, endpoint=excluded.endpoint, protocol=excluded.protocol, expires_at=excluded.expires_at, kind=excluded.kind, data_json=excluded.data_json
	`, token.Token, token.TopicARN, nullIfEmpty(token.SubscriptionARN), token.Endpoint, token.Protocol, formatTime(token.ExpiresAt), token.Kind, mustJSON(token))
}

func (tx *txStore) GetConfirmationToken(token string) (domain.ConfirmationToken, error) {
	return getConfirmationToken(context.Background(), tx.tx, token)
}

func (tx *txStore) DeleteConfirmationToken(token string) {
	_, _ = tx.tx.ExecContext(context.Background(), `DELETE FROM confirmation_tokens WHERE token = ?`, token)
}

func (tx *txStore) EnqueueDeliveryJob(job *domain.DeliveryJob) error {
	return saveDeliveryJob(context.Background(), tx.tx, job)
}

func getTopic(ctx context.Context, q queryer, arn string) (*domain.Topic, error) {
	var raw string
	err := q.QueryRowContext(ctx, `SELECT data_json FROM topics WHERE arn = ?`, arn).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	var topic domain.Topic
	if err := json.Unmarshal([]byte(raw), &topic); err != nil {
		return nil, err
	}
	return domain.CopyTopic(&topic), nil
}

func findTopicByName(ctx context.Context, q queryer, name string) (*domain.Topic, error) {
	var raw string
	err := q.QueryRowContext(ctx, `SELECT data_json FROM topics WHERE name = ?`, name).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	var topic domain.Topic
	if err := json.Unmarshal([]byte(raw), &topic); err != nil {
		return nil, err
	}
	return domain.CopyTopic(&topic), nil
}

func listTopics(ctx context.Context, q queryer, nextToken string, pageSize int) ([]*domain.Topic, string) {
	if pageSize <= 0 {
		pageSize = 100
	}
	offset := decodePageToken(nextToken)
	rows, err := q.QueryContext(ctx, `SELECT data_json FROM topics ORDER BY arn ASC LIMIT ? OFFSET ?`, pageSize+1, offset)
	if err != nil {
		return nil, ""
	}
	defer rows.Close()
	var out []*domain.Topic
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, ""
		}
		var topic domain.Topic
		if err := json.Unmarshal([]byte(raw), &topic); err != nil {
			return nil, ""
		}
		out = append(out, domain.CopyTopic(&topic))
	}
	token := ""
	if len(out) > pageSize {
		out = out[:pageSize]
		token = encodePageToken(offset + pageSize)
	}
	return out, token
}

func getSubscription(ctx context.Context, q queryer, arn string) (*domain.Subscription, error) {
	var raw string
	err := q.QueryRowContext(ctx, `SELECT data_json FROM subscriptions WHERE arn = ?`, arn).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	var sub domain.Subscription
	if err := json.Unmarshal([]byte(raw), &sub); err != nil {
		return nil, err
	}
	return domain.CopySubscription(&sub), nil
}

func listSubscriptions(ctx context.Context, q queryer, topicARN, nextToken string, pageSize int) ([]*domain.Subscription, string) {
	if pageSize <= 0 {
		pageSize = 100
	}
	offset := decodePageToken(nextToken)
	base := `SELECT data_json FROM subscriptions`
	var rows *sql.Rows
	var err error
	if strings.TrimSpace(topicARN) == "" {
		rows, err = q.QueryContext(ctx, base+` ORDER BY arn ASC LIMIT ? OFFSET ?`, pageSize+1, offset)
	} else {
		rows, err = q.QueryContext(ctx, base+` WHERE topic_arn = ? ORDER BY arn ASC LIMIT ? OFFSET ?`, topicARN, pageSize+1, offset)
	}
	if err != nil {
		return nil, ""
	}
	defer rows.Close()
	var out []*domain.Subscription
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, ""
		}
		var sub domain.Subscription
		if err := json.Unmarshal([]byte(raw), &sub); err != nil {
			return nil, ""
		}
		out = append(out, domain.CopySubscription(&sub))
	}
	token := ""
	if len(out) > pageSize {
		out = out[:pageSize]
		token = encodePageToken(offset + pageSize)
	}
	return out, token
}

func getConfirmationToken(ctx context.Context, q queryer, token string) (domain.ConfirmationToken, error) {
	var raw string
	err := q.QueryRowContext(ctx, `SELECT data_json FROM confirmation_tokens WHERE token = ?`, token).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ConfirmationToken{}, domain.ErrNotFound
		}
		return domain.ConfirmationToken{}, err
	}
	var record domain.ConfirmationToken
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return domain.ConfirmationToken{}, err
	}
	return domain.CopyConfirmationToken(record), nil
}

func getDeliveryJob(ctx context.Context, q queryer, id string) (*domain.DeliveryJob, error) {
	row := q.QueryRowContext(ctx, `
		SELECT id, kind, next_attempt_at, attempt_count, last_error, created_at, updated_at, payload_json, effective_delivery_json, raw_message_delivery, redrive_policy
		FROM delivery_jobs WHERE id = ?
	`, id)
	return scanDeliveryJob(row)
}

func saveDeliveryJob(ctx context.Context, q execer, job *domain.DeliveryJob) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO delivery_jobs(id, kind, next_attempt_at, attempt_count, last_error, created_at, updated_at, payload_json, effective_delivery_json, raw_message_delivery, redrive_policy)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			kind=excluded.kind,
			next_attempt_at=excluded.next_attempt_at,
			attempt_count=excluded.attempt_count,
			last_error=excluded.last_error,
			created_at=excluded.created_at,
			updated_at=excluded.updated_at,
			payload_json=excluded.payload_json,
			effective_delivery_json=excluded.effective_delivery_json,
			raw_message_delivery=excluded.raw_message_delivery,
			redrive_policy=excluded.redrive_policy
	`, job.ID, job.Kind, formatTime(job.NextAttemptAt), job.AttemptCount, job.LastError, formatTime(job.CreatedAt), formatTime(job.UpdatedAt), mustJSON(job.Payload), job.EffectiveDeliveryJSON, boolInt(job.RawMessageDelivery), job.RedrivePolicy)
	return err
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type scanner interface {
	Scan(dest ...any) error
}

func scanDeliveryJob(s scanner) (*domain.DeliveryJob, error) {
	var (
		id, kind, nextAttemptAt, lastError, createdAt, updatedAt string
		attemptCount                                             int
		payloadRaw, effectiveDeliveryJSON, redrivePolicy         string
		rawMessageDelivery                                       int
	)
	err := s.Scan(&id, &kind, &nextAttemptAt, &attemptCount, &lastError, &createdAt, &updatedAt, &payloadRaw, &effectiveDeliveryJSON, &rawMessageDelivery, &redrivePolicy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	var payload domain.DeliveryJobPayload
	if err := json.Unmarshal([]byte(payloadRaw), &payload); err != nil {
		return nil, err
	}
	next, err := parseTime(nextAttemptAt)
	if err != nil {
		return nil, err
	}
	created, err := parseTime(createdAt)
	if err != nil {
		return nil, err
	}
	updated, err := parseTime(updatedAt)
	if err != nil {
		return nil, err
	}
	return &domain.DeliveryJob{
		ID:                    id,
		Kind:                  kind,
		Payload:               domain.CopyDeliveryJobPayload(payload),
		EffectiveDeliveryJSON: effectiveDeliveryJSON,
		RawMessageDelivery:    rawMessageDelivery != 0,
		RedrivePolicy:         redrivePolicy,
		AttemptCount:          attemptCount,
		NextAttemptAt:         next,
		LastError:             lastError,
		CreatedAt:             created,
		UpdatedAt:             updated,
	}, nil
}

func mustJSON(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(v string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, v)
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func nullIfEmpty(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}

func encodePageToken(i int) string {
	return fmt.Sprintf("%d", i)
}

func decodePageToken(token string) int {
	if strings.TrimSpace(token) == "" {
		return 0
	}
	var i int
	if _, err := fmt.Sscanf(token, "%d", &i); err != nil || i < 0 {
		return 0
	}
	return i
}

func isUniqueErr(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "unique")
}
