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
- In-memory storage behind service/store abstractions

## Intentionally deferred

- SMS, email, mobile push, and platform application endpoint families
- Archive replay behavior
- X-Ray side effects for `TracingConfig`
- Additional filter operators such as `wildcard`, `equals-ignore-case`, and `cidr`
- Strict SigV4 request validation
- Durable persistence

## Why storage is still in-memory

SQLite was intentionally deferred for this stage. The emulator already has clean boundaries between API handling, services, policy/filter logic, and the storage layer in `internal/store/memory`, so SNS correctness and integration-test usability do not require durable persistence yet. A future SQLite backend can replace the current store implementation without changing the public HTTP behavior.

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
- `AWS_REGION`
- `AWS_ACCOUNT_ID`
- `SQS_ENDPOINT`
- `SNS_PAGE_SIZE`

`SNS_BASE_URL` is useful when confirmation and unsubscribe URLs must be reachable from outside the process host.

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
  -e SNS_ADDR=:4100 \
  -e AWS_REGION=us-east-1 \
  -e AWS_ACCOUNT_ID=123456789012 \
  emulator-aws-sns:latest
```

Then point AWS CLI at:

```bash
aws --endpoint-url http://127.0.0.1:4100 sns list-topics
```
