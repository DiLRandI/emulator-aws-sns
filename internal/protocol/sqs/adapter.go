package sqs

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"emulator-aws-sns/internal/policy"
	"emulator-aws-sns/internal/util"
)

type QueueAttributes struct {
	ARN     string
	URL     string
	FIFO    bool
	Policy  string
	Region  string
	Account string
	Name    string
}

type MessageAttribute struct {
	DataType    string
	StringValue string
	BinaryValue []byte
}

type SendRequest struct {
	QueueARN               string
	Body                   string
	MessageAttributes      map[string]MessageAttribute
	MessageGroupID         string
	MessageDeduplicationID string
}

type Client interface {
	ResolveQueue(ctx context.Context, queueARN string) (QueueAttributes, error)
	SendMessage(ctx context.Context, req SendRequest) error
	QueueAllowsTopic(ctx context.Context, queueARN string, topicARN string, topicOwner string) (bool, error)
}

type HTTPClient struct {
	Endpoint   string
	HTTPClient *http.Client
}

func NewHTTPClient(endpoint string, client *http.Client) *HTTPClient {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPClient{Endpoint: strings.TrimRight(endpoint, "/"), HTTPClient: client}
}

func (c *HTTPClient) ResolveQueue(ctx context.Context, queueARN string) (QueueAttributes, error) {
	arn, err := util.ParseARN(queueARN)
	if err != nil {
		return QueueAttributes{}, err
	}
	queueName := arn.Resource
	values := url.Values{
		"Action":                 {"GetQueueUrl"},
		"Version":                {"2012-11-05"},
		"QueueName":              {queueName},
		"QueueOwnerAWSAccountId": {arn.AccountID},
	}
	body, err := c.do(ctx, "", values)
	if err != nil {
		return QueueAttributes{}, err
	}
	queueURL := readTag(string(body), "QueueUrl")
	if queueURL == "" {
		return QueueAttributes{}, errors.New("queue URL not found")
	}
	values = url.Values{
		"Action":          {"GetQueueAttributes"},
		"Version":         {"2012-11-05"},
		"QueueUrl":        {queueURL},
		"AttributeName.1": {"QueueArn"},
		"AttributeName.2": {"FifoQueue"},
		"AttributeName.3": {"Policy"},
	}
	body, err = c.do(ctx, queueURL, values)
	if err != nil {
		return QueueAttributes{}, err
	}
	attrs := parseAttributeEntries(string(body))
	return QueueAttributes{
		ARN:     attrs["QueueArn"],
		URL:     queueURL,
		FIFO:    strings.EqualFold(attrs["FifoQueue"], "true"),
		Policy:  attrs["Policy"],
		Region:  arn.Region,
		Account: arn.AccountID,
		Name:    queueName,
	}, nil
}

func (c *HTTPClient) SendMessage(ctx context.Context, req SendRequest) error {
	queue, err := c.ResolveQueue(ctx, req.QueueARN)
	if err != nil {
		return err
	}
	values := url.Values{
		"Action":      {"SendMessage"},
		"Version":     {"2012-11-05"},
		"QueueUrl":    {queue.URL},
		"MessageBody": {req.Body},
	}
	if queue.FIFO {
		if req.MessageGroupID != "" {
			values.Set("MessageGroupId", req.MessageGroupID)
		}
		if req.MessageDeduplicationID != "" {
			values.Set("MessageDeduplicationId", req.MessageDeduplicationID)
		}
	}
	i := 1
	for key, value := range req.MessageAttributes {
		prefix := "MessageAttribute." + strconv.Itoa(i) + "."
		values.Set(prefix+"Name", key)
		values.Set(prefix+"Value.DataType", value.DataType)
		if value.StringValue != "" {
			values.Set(prefix+"Value.StringValue", value.StringValue)
		}
		if len(value.BinaryValue) > 0 {
			values.Set(prefix+"Value.BinaryValue", base64.StdEncoding.EncodeToString(value.BinaryValue))
		}
		i++
	}
	_, err = c.do(ctx, queue.URL, values)
	return err
}

func (c *HTTPClient) QueueAllowsTopic(ctx context.Context, queueARN string, topicARN string, topicOwner string) (bool, error) {
	queue, err := c.ResolveQueue(ctx, queueARN)
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(queue.Policy) == "" {
		return false, nil
	}
	return policy.Evaluate(queue.Policy, policy.Request{
		ServicePrincipal: "sns.amazonaws.com",
		Action:           "SQS:SendMessage",
		Resource:         queue.ARN,
		Context: map[string]string{
			"aws:SourceArn":     topicARN,
			"AWS:SourceOwner":   topicOwner,
			"aws:SourceAccount": topicOwner,
		},
	}, queue.Account)
}

func (c *HTTPClient) do(ctx context.Context, endpoint string, values url.Values) ([]byte, error) {
	target := endpoint
	if target == "" {
		target = c.Endpoint
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, strings.NewReader(values.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func readTag(body string, tag string) string {
	start := "<" + tag + ">"
	end := "</" + tag + ">"
	si := strings.Index(body, start)
	ei := strings.Index(body, end)
	if si < 0 || ei < 0 || ei <= si {
		return ""
	}
	return body[si+len(start) : ei]
}

func parseAttributeEntries(body string) map[string]string {
	out := map[string]string{}
	for {
		keyIdx := strings.Index(body, "<Name>")
		if keyIdx < 0 {
			break
		}
		body = body[keyIdx+len("<Name>"):]
		endKey := strings.Index(body, "</Name>")
		if endKey < 0 {
			break
		}
		name := body[:endKey]
		valueIdx := strings.Index(body[endKey:], "<Value>")
		if valueIdx < 0 {
			break
		}
		body = body[endKey+valueIdx+len("<Value>"):]
		endValue := strings.Index(body, "</Value>")
		if endValue < 0 {
			break
		}
		value := body[:endValue]
		body = body[endValue+len("</Value>"):]
		out[name] = value
	}
	return out
}
