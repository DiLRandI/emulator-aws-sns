package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"emulator-aws-sns/internal/api"
	httpproto "emulator-aws-sns/internal/protocol/http"
	sqsproto "emulator-aws-sns/internal/protocol/sqs"
	"emulator-aws-sns/internal/service"
	"emulator-aws-sns/internal/signing"
	"emulator-aws-sns/internal/store/memory"
	"emulator-aws-sns/internal/util"
)

func main() {
	addr := env("SNS_ADDR", ":4100")
	baseURL := env("SNS_BASE_URL", "")
	if baseURL == "" {
		baseURL = "http://127.0.0.1" + addr
	}
	pageSize, _ := strconv.Atoi(env("SNS_PAGE_SIZE", "100"))
	store := memory.NewStore()
	signer, err := signing.NewProvider("/__sns/certs/current.pem")
	if err != nil {
		log.Fatalf("failed to create signing material: %v", err)
	}
	httpAdapter := httpproto.NewAdapter(&http.Client{Timeout: 10 * time.Second})
	var sqsClient *sqsproto.HTTPClient
	if endpoint := strings.TrimSpace(os.Getenv("SQS_ENDPOINT")); endpoint != "" {
		sqsClient = sqsproto.NewHTTPClient(endpoint, &http.Client{Timeout: 10 * time.Second})
	}
	svc := service.New(service.Config{
		Region:    env("AWS_REGION", "us-east-1"),
		AccountID: env("AWS_ACCOUNT_ID", "123456789012"),
		BaseURL:   baseURL,
		PageSize:  pageSize,
	}, store, util.RealClock{}, signer, httpAdapter, sqsClient)
	server := api.New(svc, signer, env("AWS_ACCOUNT_ID", "123456789012"))
	log.Printf("sns emulator listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, server.Routes()))
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
