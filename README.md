# Amazon SNS Emulator

AWS-compatible Amazon SNS emulator in Go, focused on application-to-application workflows for local development and integration testing.

## Supported now

- AWS Query API for:
  - `CreateTopic`
  - `DeleteTopic`
  - `GetTopicAttributes`
  - `SetTopicAttributes`
  - `ListTopics`
  - `Subscribe`
  - `ConfirmSubscription`
  - `Unsubscribe`
  - `GetSubscriptionAttributes`
  - `SetSubscriptionAttributes`
  - `ListSubscriptions`
  - `ListSubscriptionsByTopic`
  - `Publish`
  - `PublishBatch`
  - `TagResource`
  - `UntagResource`
  - `ListTagsForResource`
  - `AddPermission`
  - `RemovePermission`
- Subscription protocols:
  - `sqs`
  - `http`
  - `https`
- Standard and FIFO topics
- Filter policies for `MessageAttributes` and `MessageBody`
- Raw message delivery for SQS and HTTP/HTTPS
- HTTP/HTTPS confirmation and unsubscribe confirmation flows
- Local signing certificate route for SNS-style webhook signatures
- Strict SigV4 verification with configurable bypass mode
- In-memory and SQLite-backed storage behind service/store abstractions

## Intentionally deferred

- SMS, email, mobile push, and platform application endpoint families
- Archive replay behavior
- X-Ray side effects for `TracingConfig`
- Full KMS/data-protection semantics beyond attribute round-tripping

## Storage modes

The emulator now supports:

- `SNS_STORE=memory` for fast ephemeral tests
- `SNS_STORE=sqlite` with `SNS_SQLITE_PATH=/path/to/sns.sqlite` for durable local runs

SQLite preserves topics, subscriptions, confirmation tokens, FIFO counters, deduplication windows, delivery jobs, and signing material across process restarts.

## Build and run

```bash
make build
make run
```

Directly with Go:

```bash
SNS_ADDR=:4100 AWS_REGION=us-east-1 AWS_ACCOUNT_ID=123456789012 go run ./cmd/snsd
```

Environment variables:

- `SNS_ADDR`
- `SNS_BASE_URL`
- `SNS_STORE`
- `SNS_SQLITE_PATH`
- `SNS_AUTH_MODE`
- `SNS_CREDENTIALS`
- `AWS_REGION`
- `AWS_ACCOUNT_ID`
- `SQS_ENDPOINT`
- `SNS_PAGE_SIZE`

`SNS_BASE_URL` is useful when confirmation and unsubscribe URLs must be reachable from outside the process host.

Auth defaults to strict SigV4 verification. To allow unsigned local requests, set:

```bash
SNS_AUTH_MODE=bypass
```

Static credentials are configured with:

```bash
SNS_CREDENTIALS=test:test:test
```

## AWS CLI usage

Set dummy credentials and a region:

```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_SESSION_TOKEN=test
export AWS_REGION=us-east-1
```

Create a topic:

```bash
aws --endpoint-url http://127.0.0.1:4100 sns create-topic --name demo-topic
```

Create a FIFO topic:

```bash
aws --endpoint-url http://127.0.0.1:4100 sns create-topic \
  --name demo-orders.fifo \
  --attributes FifoTopic=true,ContentBasedDeduplication=true
```

Publish:

```bash
aws --endpoint-url http://127.0.0.1:4100 sns publish \
  --topic-arn arn:aws:sns:us-east-1:123456789012:demo-topic \
  --message 'hello'
```

## AWS SDK for Go v2 usage

```go
package main

import (
  "context"
  "log"

  "github.com/aws/aws-sdk-go-v2/aws"
  "github.com/aws/aws-sdk-go-v2/config"
  "github.com/aws/aws-sdk-go-v2/credentials"
  snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
)

func main() {
  ctx := context.Background()
  cfg, err := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "test")),
  )
  if err != nil {
    log.Fatal(err)
  }

  client := snssdk.NewFromConfig(cfg, func(o *snssdk.Options) {
    o.EndpointResolver = snssdk.EndpointResolverFromURL("http://127.0.0.1:4100")
  })

  out, err := client.CreateTopic(ctx, &snssdk.CreateTopicInput{Name: aws.String("sdk-topic")})
  if err != nil {
    log.Fatal(err)
  }

  log.Println(*out.TopicArn)
}
```

## Tests

Unit tests:

```bash
make test-unit
```

SDK integration tests:

```bash
make test-sdk
```

CLI integration tests:

```bash
make test-cli
```

All integration tests:

```bash
make test-integration
```

The CLI suite requires `aws` to be installed. The SDK and CLI integration suites run the SNS emulator in-process and drive it against an AWS-compatible SQS fixture over the existing SQS adapter boundary.

## Docker

Build:

```bash
make docker-build
```

Run:

```bash
make docker-run
```

Equivalent manual command:

```bash
docker run --rm -p 4100:4100 \
  -v $(pwd)/data:/data \
  -e SNS_ADDR=:4100 \
  -e SNS_STORE=sqlite \
  -e SNS_SQLITE_PATH=/data/sns.sqlite \
  -e AWS_REGION=us-east-1 \
  -e AWS_ACCOUNT_ID=123456789012 \
  emulator-aws-sns:latest
```

Then point AWS CLI at:

```bash
aws --endpoint-url http://127.0.0.1:4100 sns list-topics
```

Health and readiness endpoints:

```bash
curl http://127.0.0.1:4100/__health
curl http://127.0.0.1:4100/__ready
```
