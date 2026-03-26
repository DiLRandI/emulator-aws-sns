package testsupport

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"

	"emulator-aws-sns/internal/api"
	httpproto "emulator-aws-sns/internal/protocol/http"
	sqsproto "emulator-aws-sns/internal/protocol/sqs"
	"emulator-aws-sns/internal/service"
	"emulator-aws-sns/internal/signing"
	"emulator-aws-sns/internal/store/memory"
	"emulator-aws-sns/internal/util"
)

const (
	DefaultRegion    = "us-east-1"
	DefaultAccountID = "123456789012"
)

type Harness struct {
	T      testing.TB
	Server *httptest.Server
	SQS    *FakeSQS

	service *service.Service
}

func NewHarness(t testing.TB) *Harness {
	t.Helper()
	signer, err := signing.NewProvider("/__sns/certs/current.pem")
	if err != nil {
		t.Fatalf("signing.NewProvider() error = %v", err)
	}
	sqs := NewFakeSQS(t, DefaultRegion, DefaultAccountID)
	httpClient := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	svc := service.New(service.Config{
		Region:    DefaultRegion,
		AccountID: DefaultAccountID,
		BaseURL:   "http://127.0.0.1:0",
		PageSize:  100,
	}, memory.NewStore(), util.RealClock{}, signer, httpproto.NewAdapter(httpClient), sqsproto.NewHTTPClient(sqs.Endpoint(), http.DefaultClient))
	server := httptest.NewServer(api.New(svc, signer, DefaultAccountID).Routes())
	svc.SetBaseURL(server.URL)
	h := &Harness{
		T:       t,
		Server:  server,
		SQS:     sqs,
		service: svc,
	}
	t.Cleanup(func() {
		server.Close()
		sqs.Close()
	})
	return h
}

func (h *Harness) SNSClient(ctx context.Context) *snssdk.Client {
	h.T.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(DefaultRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "test")),
	)
	if err != nil {
		h.T.Fatalf("config.LoadDefaultConfig() error = %v", err)
	}
	return snssdk.NewFromConfig(cfg, func(o *snssdk.Options) {
		o.EndpointResolver = snssdk.EndpointResolverFromURL(h.Server.URL)
	})
}

type FakeSQS struct {
	t        testing.TB
	server   *httptest.Server
	region   string
	account  string
	mu       sync.RWMutex
	queues   map[string]*queueState
	urlIndex map[string]*queueState
}

type queueState struct {
	Name     string
	FIFO     bool
	ARN      string
	URL      string
	Policy   string
	Messages []QueueMessage
}

type QueueMessage struct {
	Body       string
	Attributes map[string]string
	GroupID    string
	DedupID    string
	MessageID  string
}

func NewFakeSQS(t testing.TB, region, account string) *FakeSQS {
	t.Helper()
	f := &FakeSQS{
		t:        t,
		region:   region,
		account:  account,
		queues:   map[string]*queueState{},
		urlIndex: map[string]*queueState{},
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	return f
}

func (f *FakeSQS) Endpoint() string { return f.server.URL }

func (f *FakeSQS) Close() { f.server.Close() }

func (f *FakeSQS) AddQueue(name string, fifo bool) string {
	f.t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if fifo && !strings.HasSuffix(name, ".fifo") {
		name += ".fifo"
	}
	arn := util.FormatARN("sqs", f.region, f.account, name)
	url := fmt.Sprintf("%s/%s/%s", f.server.URL, f.account, name)
	queue := &queueState{
		Name:   name,
		FIFO:   fifo,
		ARN:    arn,
		URL:    url,
		Policy: allowAllSNSPolicy(arn),
	}
	f.queues[arn] = queue
	f.urlIndex[url] = queue
	return arn
}

func (f *FakeSQS) QueueURL(queueARN string) string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if q, ok := f.queues[queueARN]; ok {
		return q.URL
	}
	return ""
}

func (f *FakeSQS) Messages(queueARN string) []QueueMessage {
	f.mu.RLock()
	defer f.mu.RUnlock()
	queue := f.queues[queueARN]
	if queue == nil {
		return nil
	}
	out := make([]QueueMessage, len(queue.Messages))
	copy(out, queue.Messages)
	return out
}

func (f *FakeSQS) Clear(queueARN string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if queue := f.queues[queueARN]; queue != nil {
		queue.Messages = nil
	}
}

func (f *FakeSQS) handle(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	action := r.Form.Get("Action")
	switch action {
	case "GetQueueUrl":
		f.handleGetQueueURL(w, r.Form.Get("QueueName"))
	case "GetQueueAttributes":
		f.handleGetQueueAttributes(w, r.Form.Get("QueueUrl"))
	case "SendMessage":
		f.handleSendMessage(w, r.Form.Get("QueueUrl"), r.Form)
	default:
		writeSQSError(w, "InvalidAction", "Unknown SQS Action")
	}
}

func (f *FakeSQS) handleGetQueueURL(w http.ResponseWriter, queueName string) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, queue := range f.queues {
		if queue.Name == queueName {
			writeSQSSuccess(w, "GetQueueUrlResponse", "GetQueueUrlResult", fmt.Sprintf("<QueueUrl>%s</QueueUrl>", xmlEscape(queue.URL)))
			return
		}
	}
	writeSQSError(w, "AWS.SimpleQueueService.NonExistentQueue", "The specified queue does not exist.")
}

func (f *FakeSQS) handleGetQueueAttributes(w http.ResponseWriter, queueURL string) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	queue := f.urlIndex[queueURL]
	if queue == nil {
		writeSQSError(w, "AWS.SimpleQueueService.NonExistentQueue", "The specified queue does not exist.")
		return
	}
	body := strings.Builder{}
	body.WriteString(attributeEntry("QueueArn", queue.ARN))
	body.WriteString(attributeEntry("FifoQueue", boolString(queue.FIFO)))
	body.WriteString(attributeEntry("Policy", queue.Policy))
	writeSQSSuccess(w, "GetQueueAttributesResponse", "GetQueueAttributesResult", "<Attribute>"+body.String()+"</Attribute>")
}

func (f *FakeSQS) handleSendMessage(w http.ResponseWriter, queueURL string, values url.Values) {
	f.mu.Lock()
	defer f.mu.Unlock()
	queue := f.urlIndex[queueURL]
	if queue == nil {
		writeSQSError(w, "AWS.SimpleQueueService.NonExistentQueue", "The specified queue does not exist.")
		return
	}
	queue.Messages = append(queue.Messages, QueueMessage{
		Body:       values.Get("MessageBody"),
		Attributes: parseSQSMessageAttributes(values),
		GroupID:    values.Get("MessageGroupId"),
		DedupID:    values.Get("MessageDeduplicationId"),
		MessageID:  util.UUID(),
	})
	writeSQSSuccess(w, "SendMessageResponse", "SendMessageResult", fmt.Sprintf("<MessageId>%s</MessageId>", util.UUID()))
}

func allowAllSNSPolicy(queueARN string) string {
	policy := map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{
			{
				"Sid":       "__allow_all_sns__",
				"Effect":    "Allow",
				"Principal": map[string]any{"Service": "sns.amazonaws.com"},
				"Action":    "SQS:SendMessage",
				"Resource":  queueARN,
			},
		},
	}
	data, _ := json.Marshal(policy)
	return string(data)
}

func writeSQSSuccess(w http.ResponseWriter, responseName, resultName, resultBody string) {
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	fmt.Fprintf(w, "%s<%s xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\"><%s>%s</%s><ResponseMetadata><RequestId>%s</RequestId></ResponseMetadata></%s>",
		xml.Header, responseName, resultName, resultBody, resultName, util.UUID(), responseName)
}

func writeSQSError(w http.ResponseWriter, code, message string) {
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusBadRequest)
	fmt.Fprintf(w, "%s<ErrorResponse xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\"><Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message></Error><RequestId>%s</RequestId></ErrorResponse>",
		xml.Header, xmlEscape(code), xmlEscape(message), util.UUID())
}

func attributeEntry(name, value string) string {
	return fmt.Sprintf("<Name>%s</Name><Value>%s</Value>", xmlEscape(name), xmlEscape(value))
}

func parseSQSMessageAttributes(values url.Values) map[string]string {
	out := map[string]string{}
	for i := 1; ; i++ {
		name := values.Get(fmt.Sprintf("MessageAttribute.%d.Name", i))
		if name == "" {
			break
		}
		if value := values.Get(fmt.Sprintf("MessageAttribute.%d.Value.StringValue", i)); value != "" {
			out[name] = value
			continue
		}
		if value := values.Get(fmt.Sprintf("MessageAttribute.%d.Value.BinaryValue", i)); value != "" {
			raw, _ := base64.StdEncoding.DecodeString(value)
			out[name] = string(raw)
		}
	}
	return out
}

type WebhookCapture struct {
	server *httptest.Server
	mu     sync.RWMutex
	events []WebhookEvent
}

type WebhookEvent struct {
	Headers http.Header
	Body    []byte
	JSON    map[string]any
}

func NewWebhookCapture(t testing.TB, tlsEnabled bool) *WebhookCapture {
	t.Helper()
	capture := &WebhookCapture{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		event := WebhookEvent{Headers: r.Header.Clone(), Body: body}
		var payload map[string]any
		if json.Unmarshal(body, &payload) == nil {
			event.JSON = payload
		}
		capture.mu.Lock()
		capture.events = append(capture.events, event)
		capture.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	if tlsEnabled {
		capture.server = httptest.NewTLSServer(handler)
	} else {
		capture.server = httptest.NewServer(handler)
	}
	t.Cleanup(capture.server.Close)
	return capture
}

func (c *WebhookCapture) URL() string { return c.server.URL }

func (c *WebhookCapture) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = nil
}

func (c *WebhookCapture) Events() []WebhookEvent {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]WebhookEvent, len(c.events))
	copy(out, c.events)
	return out
}

func (c *WebhookCapture) WaitForType(ctx context.Context, messageType string) (WebhookEvent, error) {
	for {
		c.mu.RLock()
		for _, event := range c.events {
			if event.Headers.Get("x-amz-sns-message-type") == messageType {
				c.mu.RUnlock()
				return event, nil
			}
		}
		c.mu.RUnlock()
		select {
		case <-ctx.Done():
			return WebhookEvent{}, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func boolString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func xmlEscape(v string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(v)
}
