//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"emulator-aws-sns/internal/testsupport"
)

func TestCLISuiteTopicAndHTTP(t *testing.T) {
	if _, err := exec.LookPath("aws"); err != nil {
		t.Skip("aws cli not installed")
	}
	h := testsupport.NewHarness(t)
	hook := testsupport.NewWebhookCapture(t, false)

	topicArn := awsText(t, h, "sns", "create-topic", "--name", "cli-topic", "--query", "TopicArn", "--output", "text")
	_ = awsText(t, h, "sns", "set-topic-attributes", "--topic-arn", topicArn, "--attribute-name", "SignatureVersion", "--attribute-value", "2")
	attrsJSON := awsJSON(t, h, "sns", "get-topic-attributes", "--topic-arn", topicArn)
	if attrsJSON["Attributes"].(map[string]any)["SignatureVersion"] != "2" {
		t.Fatalf("expected SignatureVersion=2, got %#v", attrsJSON)
	}
	topics := awsJSON(t, h, "sns", "list-topics")
	if len(topics["Topics"].([]any)) == 0 {
		t.Fatalf("expected list-topics to return at least one topic")
	}

	subArn := awsText(t, h, "sns", "subscribe",
		"--topic-arn", topicArn,
		"--protocol", "http",
		"--notification-endpoint", hook.URL(),
		"--return-subscription-arn",
		"--query", "SubscriptionArn", "--output", "text",
	)
	awsText(t, h, "sns", "publish", "--topic-arn", topicArn, "--message", "pending")
	time.Sleep(200 * time.Millisecond)
	if len(hook.Events()) != 1 {
		t.Fatalf("expected only confirmation event before confirm, got %d", len(hook.Events()))
	}
	confirmEvent, err := hook.WaitForType(timeoutContext(t), "SubscriptionConfirmation")
	if err != nil {
		t.Fatalf("WaitForType(SubscriptionConfirmation) error = %v", err)
	}
	token := stringField(confirmEvent.JSON, "Token")
	awsText(t, h, "sns", "confirm-subscription", "--topic-arn", topicArn, "--token", token, "--query", "SubscriptionArn", "--output", "text")

	awsText(t, h, "sns", "set-subscription-attributes", "--subscription-arn", subArn, "--attribute-name", "FilterPolicy", "--attribute-value", `{"event":["match"]}`)
	subAttrs := awsJSON(t, h, "sns", "get-subscription-attributes", "--subscription-arn", subArn)
	if subAttrs["Attributes"].(map[string]any)["FilterPolicy"] != `{"event":["match"]}` {
		t.Fatalf("unexpected filter policy in get-subscription-attributes: %#v", subAttrs)
	}
	hook.Clear()
	awsText(t, h, "sns", "publish", "--topic-arn", topicArn, "--message", "skip-me", "--message-attributes", `{"event":{"DataType":"String","StringValue":"miss"}}`)
	time.Sleep(200 * time.Millisecond)
	if len(hook.Events()) != 0 {
		t.Fatalf("expected filtered-out CLI publish to skip delivery")
	}
	awsText(t, h, "sns", "publish", "--topic-arn", topicArn, "--message", "deliver-me", "--message-attributes", `{"event":{"DataType":"String","StringValue":"match"}}`)
	if _, err := hook.WaitForType(timeoutContext(t), "Notification"); err != nil {
		t.Fatalf("expected CLI notification after confirm: %v", err)
	}

	awsText(t, h, "sns", "set-subscription-attributes", "--subscription-arn", subArn, "--attribute-name", "FilterPolicy", "--attribute-value", `{"kind":["receipt"]}`)
	awsText(t, h, "sns", "set-subscription-attributes", "--subscription-arn", subArn, "--attribute-name", "FilterPolicyScope", "--attribute-value", "MessageBody")
	hook.Clear()
	awsText(t, h, "sns", "publish", "--topic-arn", topicArn, "--message", `{"kind":"invoice"}`)
	time.Sleep(200 * time.Millisecond)
	if len(hook.Events()) != 0 {
		t.Fatalf("expected body filter miss to skip delivery")
	}
	awsText(t, h, "sns", "publish", "--topic-arn", topicArn, "--message", `{"kind":"receipt"}`)
	if _, err := hook.WaitForType(timeoutContext(t), "Notification"); err != nil {
		t.Fatalf("expected body-filter CLI notification: %v", err)
	}

	awsText(t, h, "sns", "set-subscription-attributes", "--subscription-arn", subArn, "--attribute-name", "FilterPolicy", "--attribute-value", "")
	awsText(t, h, "sns", "set-subscription-attributes", "--subscription-arn", subArn, "--attribute-name", "FilterPolicyScope", "--attribute-value", "MessageAttributes")
	awsText(t, h, "sns", "set-subscription-attributes", "--subscription-arn", subArn, "--attribute-name", "RawMessageDelivery", "--attribute-value", "true")
	hook.Clear()
	awsText(t, h, "sns", "publish", "--topic-arn", topicArn, "--message", "raw-cli")
	rawEvent, err := hook.WaitForType(timeoutContext(t), "Notification")
	if err != nil {
		t.Fatalf("expected raw CLI notification: %v", err)
	}
	if string(rawEvent.Body) != "raw-cli" {
		t.Fatalf("expected raw CLI body, got %q", rawEvent.Body)
	}

	batchInput := fmt.Sprintf(`{"TopicArn":"%s","PublishBatchRequestEntries":[{"Id":"one","Message":"batch-one"},{"Id":"two","Message":"batch-two"}]}`, topicArn)
	batchOut := awsJSON(t, h, "sns", "publish-batch", "--cli-input-json", batchInput)
	if len(batchOut["Successful"].([]any)) != 2 {
		t.Fatalf("expected 2 successful publish-batch entries, got %#v", batchOut)
	}

	awsText(t, h, "sns", "tag-resource", "--resource-arn", topicArn, "--tags", "Key=team,Value=cli", "Key=env,Value=test")
	tagOut := awsJSON(t, h, "sns", "list-tags-for-resource", "--resource-arn", topicArn)
	if len(tagOut["Tags"].([]any)) != 2 {
		t.Fatalf("expected 2 tags from cli list-tags, got %#v", tagOut)
	}
	awsText(t, h, "sns", "untag-resource", "--resource-arn", topicArn, "--tag-keys", "env")
	tagOut = awsJSON(t, h, "sns", "list-tags-for-resource", "--resource-arn", topicArn)
	if len(tagOut["Tags"].([]any)) != 1 {
		t.Fatalf("expected 1 tag after cli untag, got %#v", tagOut)
	}

	awsText(t, h, "sns", "add-permission", "--topic-arn", topicArn, "--label", "cli-allow", "--aws-account-id", testsupport.DefaultAccountID, "--action-name", "Publish")
	awsText(t, h, "sns", "remove-permission", "--topic-arn", topicArn, "--label", "cli-allow")

	listSubs := awsJSON(t, h, "sns", "list-subscriptions")
	if len(listSubs["Subscriptions"].([]any)) == 0 {
		t.Fatalf("expected subscriptions in list-subscriptions")
	}
	listByTopic := awsJSON(t, h, "sns", "list-subscriptions-by-topic", "--topic-arn", topicArn)
	if len(listByTopic["Subscriptions"].([]any)) != 1 {
		t.Fatalf("expected one subscription by topic, got %#v", listByTopic)
	}

	awsText(t, h, "sns", "unsubscribe", "--subscription-arn", subArn)
	awsText(t, h, "sns", "delete-topic", "--topic-arn", topicArn)
}

func TestCLISuiteSQSAndFIFO(t *testing.T) {
	if _, err := exec.LookPath("aws"); err != nil {
		t.Skip("aws cli not installed")
	}
	h := testsupport.NewHarness(t)
	queue1 := h.SQS.AddQueue("cli-orders", false)
	queue2 := h.SQS.AddQueue("cli-audit", false)
	fifoQueue := h.SQS.AddQueue("cli-orders.fifo", true)

	topicArn := awsText(t, h, "sns", "create-topic", "--name", "cli-sqs-topic", "--query", "TopicArn", "--output", "text")
	sub1 := awsText(t, h, "sns", "subscribe", "--topic-arn", topicArn, "--protocol", "sqs", "--notification-endpoint", queue1, "--return-subscription-arn", "--query", "SubscriptionArn", "--output", "text")
	sub2 := awsText(t, h, "sns", "subscribe", "--topic-arn", topicArn, "--protocol", "sqs", "--notification-endpoint", queue2, "--return-subscription-arn", "--query", "SubscriptionArn", "--output", "text")
	awsText(t, h, "sns", "set-subscription-attributes", "--subscription-arn", sub2, "--attribute-name", "RawMessageDelivery", "--attribute-value", "true")
	awsText(t, h, "sns", "publish", "--topic-arn", topicArn, "--message", "fanout-cli", "--message-attributes", `{"kind":{"DataType":"String","StringValue":"fanout"}}`)
	if len(h.SQS.Messages(queue1)) != 1 || len(h.SQS.Messages(queue2)) != 1 {
		t.Fatalf("expected CLI SQS fanout to both queues")
	}
	if h.SQS.Messages(queue2)[0].Body != "fanout-cli" {
		t.Fatalf("expected raw SQS body from CLI publish")
	}

	fifoTopicArn := awsText(t, h, "sns", "create-topic", "--name", "cli-orders.fifo", "--attributes", "FifoTopic=true,ContentBasedDeduplication=true", "--query", "TopicArn", "--output", "text")
	fifoSub := awsText(t, h, "sns", "subscribe", "--topic-arn", fifoTopicArn, "--protocol", "sqs", "--notification-endpoint", fifoQueue, "--return-subscription-arn", "--query", "SubscriptionArn", "--output", "text")
	awsText(t, h, "sns", "set-subscription-attributes", "--subscription-arn", fifoSub, "--attribute-name", "RawMessageDelivery", "--attribute-value", "true")

	awsExpectErrorCode(t, h, "InvalidParameter", "sns", "publish", "--topic-arn", fifoTopicArn, "--message", "missing-group")
	awsText(t, h, "sns", "publish", "--topic-arn", fifoTopicArn, "--message", "fifo-1", "--message-group-id", "group-a")
	awsText(t, h, "sns", "publish", "--topic-arn", fifoTopicArn, "--message", "fifo-1", "--message-group-id", "group-a")
	awsText(t, h, "sns", "publish", "--topic-arn", fifoTopicArn, "--message", "fifo-2", "--message-group-id", "group-a")
	messages := h.SQS.Messages(fifoQueue)
	if len(messages) != 2 {
		t.Fatalf("expected CLI FIFO dedup to leave 2 messages, got %d", len(messages))
	}
	if messages[0].Body != "fifo-1" || messages[1].Body != "fifo-2" {
		t.Fatalf("unexpected CLI FIFO ordering: %#v", messages)
	}

	h.SQS.Clear(fifoQueue)
	var batchEntries []string
	for i := 0; i < 10; i++ {
		batchEntries = append(batchEntries, fmt.Sprintf(`{"Id":"id-%d","Message":"fifo-batch-%d","MessageGroupId":"group-a","MessageDeduplicationId":"dedup-%d"}`, i, i, i))
	}
	awsText(t, h, "sns", "publish-batch", "--cli-input-json", fmt.Sprintf(`{"TopicArn":"%s","PublishBatchRequestEntries":[%s]}`, fifoTopicArn, strings.Join(batchEntries, ",")))
	messages = h.SQS.Messages(fifoQueue)
	if len(messages) != 10 {
		t.Fatalf("expected 10 FIFO CLI batch messages, got %d", len(messages))
	}
	awsExpectErrorCode(t, h, "TooManyEntriesInBatchRequest", "sns", "publish-batch", "--cli-input-json", fmt.Sprintf(`{"TopicArn":"%s","PublishBatchRequestEntries":[%s,%s]}`, fifoTopicArn, strings.Join(batchEntries, ","), `{"Id":"id-10","Message":"overflow","MessageGroupId":"group-a","MessageDeduplicationId":"dedup-10"}`))

	awsText(t, h, "sns", "unsubscribe", "--subscription-arn", sub1)
	awsText(t, h, "sns", "unsubscribe", "--subscription-arn", sub2)
	awsText(t, h, "sns", "unsubscribe", "--subscription-arn", fifoSub)
	awsText(t, h, "sns", "delete-topic", "--topic-arn", topicArn)
	awsText(t, h, "sns", "delete-topic", "--topic-arn", fifoTopicArn)
}

func awsText(t *testing.T, h *testsupport.Harness, args ...string) string {
	t.Helper()
	out, _ := runAWS(t, h, args...)
	return strings.TrimSpace(string(out))
}

func awsJSON(t *testing.T, h *testsupport.Harness, args ...string) map[string]any {
	t.Helper()
	out, _ := runAWS(t, h, append(args, "--output", "json")...)
	var payload map[string]any
	if err := json.Unmarshal(out, &payload); err != nil {
		t.Fatalf("json.Unmarshal(%v) error = %v; output=%s", args, err, out)
	}
	return payload
}

func awsExpectErrorCode(t *testing.T, h *testsupport.Harness, wantCode string, args ...string) {
	t.Helper()
	_, stderr := runAWSExpectFailure(t, h, args...)
	if !strings.Contains(stderr, wantCode) {
		t.Fatalf("expected stderr to contain %q, got %s", wantCode, stderr)
	}
}

func runAWS(t *testing.T, h *testsupport.Harness, args ...string) ([]byte, string) {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), "aws", append([]string{"--endpoint-url", h.Server.URL, "--region", testsupport.DefaultRegion}, args...)...)
	cmd.Env = append(os.Environ(),
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
	return stdout.Bytes(), stderr.String()
}

func runAWSExpectFailure(t *testing.T, h *testsupport.Harness, args ...string) ([]byte, string) {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), "aws", append([]string{"--endpoint-url", h.Server.URL, "--region", testsupport.DefaultRegion}, args...)...)
	cmd.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID=test",
		"AWS_SECRET_ACCESS_KEY=test",
		"AWS_SESSION_TOKEN=test",
		"AWS_EC2_METADATA_DISABLED=true",
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err == nil {
		t.Fatalf("expected aws %v to fail", args)
	}
	return stdout.Bytes(), stderr.String()
}
