package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"strings"
	"time"
)

const (
	ActionVersion = "2010-03-31"

	ProtocolSQS   = "sqs"
	ProtocolHTTP  = "http"
	ProtocolHTTPS = "https"

	FilterScopeMessageAttributes = "MessageAttributes"
	FilterScopeMessageBody       = "MessageBody"

	FifoThroughputTopic        = "Topic"
	FifoThroughputMessageGroup = "MessageGroup"
)

var (
	ErrNotFound        = errors.New("not found")
	ErrConflict        = errors.New("conflict")
	ErrInvalidArgument = errors.New("invalid argument")
)

type APIError struct {
	Code       string
	Message    string
	HTTPStatus int
	Sender     bool
}

func (e *APIError) Error() string { return e.Message }

func NewInvalidParameter(format string, args ...any) *APIError {
	return &APIError{Code: "InvalidParameter", Message: fmt.Sprintf(format, args...), HTTPStatus: 400, Sender: true}
}

func NewParameterValueInvalid(format string, args ...any) *APIError {
	return &APIError{Code: "ParameterValueInvalid", Message: fmt.Sprintf(format, args...), HTTPStatus: 400, Sender: true}
}

func NewNotFound(format string, args ...any) *APIError {
	return &APIError{Code: "NotFound", Message: fmt.Sprintf(format, args...), HTTPStatus: 404, Sender: true}
}

func NewAuthorization(format string, args ...any) *APIError {
	return &APIError{Code: "AuthorizationError", Message: fmt.Sprintf(format, args...), HTTPStatus: 403, Sender: true}
}

func NewValidation(format string, args ...any) *APIError {
	return &APIError{Code: "Validation", Message: fmt.Sprintf(format, args...), HTTPStatus: 400, Sender: true}
}

type Identity struct {
	AccountID string
	Principal string
}

type Topic struct {
	ARN            string
	Name           string
	Owner          string
	Region         string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Attributes     TopicAttributes
	Tags           map[string]string
	Subscriptions  map[string]struct{}
	GroupSequences map[string]SequenceState
	DedupRecords   map[string]DedupRecord
	ConfirmedCount int
	DeletedCount   int
	PendingCount   int
}

type SequenceState struct {
	Prefix  string
	Counter uint64
}

type DedupRecord struct {
	MessageID      string
	SequenceNumber string
	ExpiresAt      time.Time
}

type TopicAttributes struct {
	DeliveryPolicy            string
	Policy                    string
	DisplayName               string
	SignatureVersion          string
	TracingConfig             string
	ArchivePolicy             string
	FifoTopic                 bool
	ContentBasedDeduplication bool
	FifoThroughputScope       string
	Unsupported               map[string]string
}

func (a TopicAttributes) AttributeMap() map[string]string {
	attrs := map[string]string{
		"DisplayName":               a.DisplayName,
		"Policy":                    a.Policy,
		"DeliveryPolicy":            a.DeliveryPolicy,
		"SignatureVersion":          a.SignatureVersion,
		"TracingConfig":             a.TracingConfig,
		"ArchivePolicy":             a.ArchivePolicy,
		"FifoTopic":                 boolString(a.FifoTopic),
		"ContentBasedDeduplication": boolString(a.ContentBasedDeduplication),
		"FifoThroughputScope":       a.FifoThroughputScope,
	}
	maps.Copy(attrs, a.Unsupported)
	return attrs
}

type Subscription struct {
	ARN                          string
	TopicARN                     string
	Owner                        string
	Protocol                     string
	Endpoint                     string
	CreatedAt                    time.Time
	UpdatedAt                    time.Time
	Confirmed                    bool
	PendingConfirmation          bool
	ConfirmationWasAuthenticated bool
	AuthenticateOnUnsubscribe    bool
	Attributes                   SubscriptionAttributes
}

type SubscriptionAttributes struct {
	DeliveryPolicy          string
	FilterPolicy            string
	FilterPolicyScope       string
	RawMessageDelivery      bool
	RedrivePolicy           string
	EffectiveDeliveryPolicy string
}

func (a SubscriptionAttributes) AttributeMap(sub *Subscription) map[string]string {
	return map[string]string{
		"DeliveryPolicy":               a.DeliveryPolicy,
		"FilterPolicy":                 a.FilterPolicy,
		"FilterPolicyScope":            defaultString(a.FilterPolicyScope, FilterScopeMessageAttributes),
		"RawMessageDelivery":           boolString(a.RawMessageDelivery),
		"RedrivePolicy":                a.RedrivePolicy,
		"EffectiveDeliveryPolicy":      a.EffectiveDeliveryPolicy,
		"PendingConfirmation":          boolString(sub.PendingConfirmation),
		"ConfirmationWasAuthenticated": boolString(sub.ConfirmationWasAuthenticated),
		"Owner":                        sub.Owner,
		"TopicArn":                     sub.TopicARN,
		"Endpoint":                     sub.Endpoint,
		"Protocol":                     sub.Protocol,
		"SubscriptionArn":              sub.ARN,
	}
}

type ConfirmationToken struct {
	Token                     string
	SubscriptionARN           string
	TopicARN                  string
	Endpoint                  string
	Protocol                  string
	ExpiresAt                 time.Time
	AuthenticateOnUnsubscribe bool
	Kind                      string
	Restore                   *Subscription
}

type SigningMaterial struct {
	PrivateKeyPEM []byte
	CertPEM       []byte
	CreatedAt     time.Time
}

type MessageAttributeValue struct {
	DataType    string
	StringValue string
	BinaryValue []byte
}

func (m MessageAttributeValue) DeepCopy() MessageAttributeValue {
	cp := m
	if m.BinaryValue != nil {
		cp.BinaryValue = append([]byte(nil), m.BinaryValue...)
	}
	return cp
}

func (m MessageAttributeValue) String() string {
	switch {
	case m.StringValue != "":
		return m.StringValue
	case len(m.BinaryValue) > 0:
		return string(m.BinaryValue)
	default:
		return ""
	}
}

type PublishInput struct {
	TopicARN               string
	Message                string
	Subject                string
	MessageStructure       string
	MessageAttributes      map[string]MessageAttributeValue
	MessageGroupID         string
	MessageDeduplicationID string
}

type PublishOutput struct {
	MessageID      string
	SequenceNumber string
	Deduplicated   bool
}

type PublishBatchEntry struct {
	ID                     string
	Message                string
	Subject                string
	MessageStructure       string
	MessageAttributes      map[string]MessageAttributeValue
	MessageGroupID         string
	MessageDeduplicationID string
}

type PublishBatchFailure struct {
	ID          string
	Code        string
	Message     string
	SenderFault bool
}

type DeliveryAttempt struct {
	Subscription      *Subscription
	Topic             *Topic
	MessageID         string
	Subject           string
	Message           string
	ProtocolMessage   string
	Timestamp         time.Time
	Type              string
	Token             string
	SubscribeURL      string
	UnsubscribeURL    string
	SignatureVersion  string
	Signature         string
	SigningCertURL    string
	MessageAttributes map[string]MessageAttributeValue
	GroupID           string
	DeduplicationID   string
	SequenceNumber    string
	Headers           map[string]string
}

type DeliveryJob struct {
	ID                    string
	Kind                  string
	Payload               DeliveryJobPayload
	EffectiveDeliveryJSON string
	RawMessageDelivery    bool
	RedrivePolicy         string
	AttemptCount          int
	NextAttemptAt         time.Time
	LastError             string
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

type DeliveryJobPayload struct {
	Type              string
	TopicARN          string
	TopicOwner        string
	SubscriptionARN   string
	Protocol          string
	Endpoint          string
	MessageID         string
	Subject           string
	Message           string
	ProtocolMessage   string
	Timestamp         time.Time
	Token             string
	SubscribeURL      string
	UnsubscribeURL    string
	SignatureVersion  string
	Signature         string
	SigningCertURL    string
	MessageAttributes map[string]MessageAttributeValue
	GroupID           string
	DeduplicationID   string
	SequenceNumber    string
	Headers           map[string]string
}

type DeliveryResult struct {
	Success      bool
	Retryable    bool
	StatusCode   int
	ErrorMessage string
}

type BatchDeliveryResult struct {
	ID             string
	MessageID      string
	SequenceNumber string
}

type RedrivePolicy struct {
	DeadLetterTargetArn string `json:"deadLetterTargetArn"`
}

func ParseRedrivePolicy(raw string) (RedrivePolicy, error) {
	if strings.TrimSpace(raw) == "" {
		return RedrivePolicy{}, nil
	}
	var rp RedrivePolicy
	if err := json.Unmarshal([]byte(raw), &rp); err != nil {
		return RedrivePolicy{}, err
	}
	return rp, nil
}

func boolString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func defaultString(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

func CopyTopic(src *Topic) *Topic {
	if src == nil {
		return nil
	}
	cp := *src
	cp.Tags = cloneStringMap(src.Tags)
	cp.Subscriptions = map[string]struct{}{}
	for k := range src.Subscriptions {
		cp.Subscriptions[k] = struct{}{}
	}
	cp.GroupSequences = map[string]SequenceState{}
	maps.Copy(cp.GroupSequences, src.GroupSequences)
	cp.DedupRecords = map[string]DedupRecord{}
	maps.Copy(cp.DedupRecords, src.DedupRecords)
	cp.Attributes.Unsupported = cloneStringMap(src.Attributes.Unsupported)
	return &cp
}

func CopySubscription(src *Subscription) *Subscription {
	if src == nil {
		return nil
	}
	cp := *src
	return &cp
}

func CopyConfirmationToken(src ConfirmationToken) ConfirmationToken {
	cp := src
	if src.Restore != nil {
		cp.Restore = CopySubscription(src.Restore)
	}
	return cp
}

func CopyDeliveryJob(src *DeliveryJob) *DeliveryJob {
	if src == nil {
		return nil
	}
	cp := *src
	cp.Payload = CopyDeliveryJobPayload(src.Payload)
	return &cp
}

func CopyDeliveryJobPayload(src DeliveryJobPayload) DeliveryJobPayload {
	cp := src
	cp.MessageAttributes = cloneMessageAttributes(src.MessageAttributes)
	cp.Headers = cloneStringMap(src.Headers)
	return cp
}

func cloneStringMap(src map[string]string) map[string]string {
	if src == nil {
		return map[string]string{}
	}
	dst := make(map[string]string, len(src))
	maps.Copy(dst, src)
	return dst
}

func cloneMessageAttributes(src map[string]MessageAttributeValue) map[string]MessageAttributeValue {
	if src == nil {
		return map[string]MessageAttributeValue{}
	}
	dst := make(map[string]MessageAttributeValue, len(src))
	for k, v := range src {
		dst[k] = v.DeepCopy()
	}
	return dst
}

func NextSequence(state SequenceState) (SequenceState, string) {
	if state.Prefix == "" {
		state.Prefix = fmt.Sprintf("%016x", time.Now().UnixNano())
	}
	state.Counter++
	prefix := new(big.Int)
	prefix.SetString(state.Prefix, 16)
	prefix.Lsh(prefix, 64)
	counter := new(big.Int).SetUint64(state.Counter)
	return state, new(big.Int).Add(prefix, counter).String()
}

func DefaultTimestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}
