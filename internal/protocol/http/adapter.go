package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"emulator-aws-sns/internal/delivery"
	"emulator-aws-sns/internal/domain"
)

type Adapter struct {
	Client *http.Client
}

func NewAdapter(client *http.Client) *Adapter {
	if client == nil {
		client = http.DefaultClient
	}
	return &Adapter{Client: client}
}

func (a *Adapter) Protocols() []string {
	return []string{domain.ProtocolHTTP, domain.ProtocolHTTPS}
}

func (a *Adapter) ValidateEndpoint(endpoint string) error {
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return nil
	}
	return domain.NewInvalidParameter("Invalid parameter: Endpoint")
}

func (a *Adapter) Deliver(ctx context.Context, attempt *domain.DeliveryAttempt, policy delivery.SubscriptionPolicy, raw bool) domain.DeliveryResult {
	body, contentType, err := bodyForAttempt(attempt, policy, raw)
	if err != nil {
		return domain.DeliveryResult{Success: false, Retryable: false, ErrorMessage: err.Error()}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, attempt.Subscription.Endpoint, bytes.NewReader(body))
	if err != nil {
		return domain.DeliveryResult{Success: false, Retryable: true, ErrorMessage: err.Error()}
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", "Amazon Simple Notification Service Agent")
	req.Header.Set("x-amz-sns-message-type", attempt.Type)
	req.Header.Set("x-amz-sns-message-id", attempt.MessageID)
	req.Header.Set("x-amz-sns-topic-arn", attempt.Topic.ARN)
	req.Header.Set("x-amz-sns-subscription-arn", attempt.Subscription.ARN)
	if raw {
		req.Header.Set("x-amz-sns-rawdelivery", "true")
	}
	for key, value := range attempt.Headers {
		req.Header.Set(key, value)
	}
	resp, err := a.Client.Do(req)
	if err != nil {
		return domain.DeliveryResult{Success: false, Retryable: true, ErrorMessage: err.Error()}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return domain.DeliveryResult{Success: true, StatusCode: resp.StatusCode}
	}
	retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
	return domain.DeliveryResult{
		Success:      false,
		Retryable:    retryable,
		StatusCode:   resp.StatusCode,
		ErrorMessage: resp.Status,
	}
}

func bodyForAttempt(attempt *domain.DeliveryAttempt, policy delivery.SubscriptionPolicy, raw bool) ([]byte, string, error) {
	contentType := policy.RequestPolicy.HeaderContentType
	if contentType == "" {
		contentType = "text/plain; charset=UTF-8"
	}
	if raw {
		return []byte(attempt.ProtocolMessage), contentType, nil
	}
	payload := map[string]any{
		"Type":             attempt.Type,
		"MessageId":        attempt.MessageID,
		"TopicArn":         attempt.Topic.ARN,
		"Message":          attempt.ProtocolMessage,
		"Timestamp":        domain.DefaultTimestamp(attempt.Timestamp),
		"SignatureVersion": attempt.SignatureVersion,
		"Signature":        attempt.Signature,
		"SigningCertURL":   attempt.SigningCertURL,
	}
	if attempt.Subscription.ARN != "" {
		payload["SubscriptionArn"] = attempt.Subscription.ARN
	}
	if attempt.Subject != "" {
		payload["Subject"] = attempt.Subject
	}
	if attempt.Type == "Notification" {
		payload["UnsubscribeURL"] = attempt.UnsubscribeURL
		if len(attempt.MessageAttributes) > 0 {
			payload["MessageAttributes"] = marshalAttributes(attempt.MessageAttributes)
		}
	} else {
		payload["Token"] = attempt.Token
		payload["SubscribeURL"] = attempt.SubscribeURL
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, "", err
	}
	return body, contentType, nil
}

func marshalAttributes(attrs map[string]domain.MessageAttributeValue) map[string]map[string]string {
	out := map[string]map[string]string{}
	for key, value := range attrs {
		entry := map[string]string{"Type": value.DataType}
		if value.StringValue != "" {
			entry["Value"] = value.StringValue
		}
		if len(value.BinaryValue) > 0 {
			entry["Value"] = string(value.BinaryValue)
		}
		out[key] = entry
	}
	return out
}

func ContentLength(body []byte) string {
	return strconv.Itoa(len(body))
}
