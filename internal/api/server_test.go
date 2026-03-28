package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"

	"emulator-aws-sns/internal/api"
	"emulator-aws-sns/internal/auth"
	httpproto "emulator-aws-sns/internal/protocol/http"
	"emulator-aws-sns/internal/service"
	"emulator-aws-sns/internal/signing"
	"emulator-aws-sns/internal/store/memory"
	"emulator-aws-sns/internal/util"
)

type webhookEvent struct {
	Headers http.Header
	Body    []byte
}

func TestHTTPSubscriptionLifecycleAndPublish(t *testing.T) {
	serverURL := startSNSServer(t)
	events := make(chan webhookEvent, 4)
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		events <- webhookEvent{Headers: r.Header.Clone(), Body: body}
		w.WriteHeader(http.StatusOK)
	}))
	defer webhook.Close()

	topicArn := mustCreateTopic(t, serverURL, "orders")
	subArn := mustSubscribe(t, serverURL, topicArn, webhook.URL, true)
	if subArn == "" {
		t.Fatalf("expected subscription ARN")
	}
	confirm := waitEvent(t, events)
	if got := confirm.Headers.Get("x-amz-sns-message-type"); got != "SubscriptionConfirmation" {
		t.Fatalf("expected SubscriptionConfirmation, got %q", got)
	}
	token := extractJSONField(t, confirm.Body, "Token")
	mustConfirm(t, serverURL, topicArn, token)
	mustPublish(t, serverURL, topicArn, "hello world")
	notification := waitEvent(t, events)
	if got := notification.Headers.Get("x-amz-sns-message-type"); got != "Notification" {
		t.Fatalf("expected Notification, got %q", got)
	}
	if extractJSONField(t, notification.Body, "Message") != "hello world" {
		t.Fatalf("unexpected notification message: %s", notification.Body)
	}
	if extractJSONField(t, notification.Body, "SubscriptionArn") != subArn {
		t.Fatalf("unexpected subscription arn in payload")
	}
}

func TestAWSCLISmokeFlow(t *testing.T) {
	if _, err := exec.LookPath("aws"); err != nil {
		t.Skip("aws cli not installed")
	}
	serverURL := startSNSServer(t)
	out := runAWS(t, serverURL, "sns", "create-topic", "--name", "cli-topic")
	var create struct {
		TopicArn string `json:"TopicArn"`
	}
	if err := json.Unmarshal(out, &create); err != nil {
		t.Fatalf("json.Unmarshal(create-topic) error = %v; output=%s", err, out)
	}
	if create.TopicArn == "" {
		t.Fatalf("expected topic ARN from aws cli")
	}
	runAWS(t, serverURL, "sns", "get-topic-attributes", "--topic-arn", create.TopicArn)
	runAWS(t, serverURL, "sns", "delete-topic", "--topic-arn", create.TopicArn)
}

func startSNSServer(t *testing.T) string {
	t.Helper()
	signer, err := signing.NewProvider("/__sns/certs/current.pem")
	if err != nil {
		t.Fatalf("signing.NewProvider() error = %v", err)
	}
	svc := service.New(service.Config{
		Region:    "us-east-1",
		AccountID: "123456789012",
		BaseURL:   "http://127.0.0.1:0",
		PageSize:  100,
	}, memory.NewStore(), util.RealClock{}, signer, httpproto.NewAdapter(nil), nil)
	t.Cleanup(func() { _ = svc.Close() })
	verifier := auth.NewVerifier("123456789012", "us-east-1", auth.ModeBypass, []auth.Credential{{AccessKeyID: "test", SecretAccessKey: "test", SessionToken: "test"}})
	handler := api.New(svc, signer, verifier).Routes()
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	svc.SetBaseURL(ts.URL)
	return ts.URL
}

func mustCreateTopic(t *testing.T, serverURL, name string) string {
	t.Helper()
	body := doForm(t, serverURL, url.Values{
		"Action": {"CreateTopic"},
		"Name":   {name},
	})
	var resp struct {
		Result struct {
			TopicArn string `xml:"TopicArn"`
		} `xml:"CreateTopicResult"`
	}
	if err := xml.Unmarshal(body, &resp); err != nil {
		t.Fatalf("xml.Unmarshal(CreateTopic) error = %v; body=%s", err, body)
	}
	return resp.Result.TopicArn
}

func mustSubscribe(t *testing.T, serverURL, topicArn, endpoint string, returnArn bool) string {
	t.Helper()
	values := url.Values{
		"Action":                {"Subscribe"},
		"TopicArn":              {topicArn},
		"Protocol":              {"http"},
		"Endpoint":              {endpoint},
		"ReturnSubscriptionArn": {boolString(returnArn)},
	}
	body := doForm(t, serverURL, values)
	var resp struct {
		Result struct {
			SubscriptionArn string `xml:"SubscriptionArn"`
		} `xml:"SubscribeResult"`
	}
	if err := xml.Unmarshal(body, &resp); err != nil {
		t.Fatalf("xml.Unmarshal(Subscribe) error = %v; body=%s", err, body)
	}
	return resp.Result.SubscriptionArn
}

func mustConfirm(t *testing.T, serverURL, topicArn, token string) {
	t.Helper()
	doForm(t, serverURL, url.Values{
		"Action":   {"ConfirmSubscription"},
		"TopicArn": {topicArn},
		"Token":    {token},
	})
}

func mustPublish(t *testing.T, serverURL, topicArn, message string) {
	t.Helper()
	doForm(t, serverURL, url.Values{
		"Action":   {"Publish"},
		"TopicArn": {topicArn},
		"Message":  {message},
	})
}

func doForm(t *testing.T, serverURL string, values url.Values) []byte {
	t.Helper()
	resp, err := http.Post(serverURL+"/", "application/x-www-form-urlencoded", strings.NewReader(values.Encode()))
	if err != nil {
		t.Fatalf("http.Post() error = %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		t.Fatalf("unexpected http status %d: %s", resp.StatusCode, body)
	}
	return body
}

func waitEvent(t *testing.T, events <-chan webhookEvent) webhookEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for webhook event")
		return webhookEvent{}
	}
}

func extractJSONField(t *testing.T, body []byte, field string) string {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v; body=%s", err, body)
	}
	value, _ := payload[field].(string)
	return value
}

func runAWS(t *testing.T, endpoint string, args ...string) []byte {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), "aws", append([]string{"--endpoint-url", endpoint, "--region", "us-east-1", "--output", "json"}, args...)...)
	cmd.Env = append(cmd.Environ(),
		"AWS_ACCESS_KEY_ID=test",
		"AWS_SECRET_ACCESS_KEY=test",
		"AWS_SESSION_TOKEN=test",
		"AWS_EC2_METADATA_DISABLED=true",
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("aws %v failed: %v\nstdout=%s\nstderr=%s", args, err, stdout.String(), stderr.String())
	}
	return stdout.Bytes()
}

func boolString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
