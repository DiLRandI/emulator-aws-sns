package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"emulator-aws-sns/internal/api"
	"emulator-aws-sns/internal/auth"
	httpproto "emulator-aws-sns/internal/protocol/http"
	sqsproto "emulator-aws-sns/internal/protocol/sqs"
	"emulator-aws-sns/internal/service"
	"emulator-aws-sns/internal/signing"
	storepkg "emulator-aws-sns/internal/store"
	"emulator-aws-sns/internal/store/memory"
	sqlitestore "emulator-aws-sns/internal/store/sqlite"
	"emulator-aws-sns/internal/util"
)

func main() {
	addr := env("SNS_ADDR", ":4100")
	baseURL := env("SNS_BASE_URL", "")
	if baseURL == "" {
		baseURL = "http://127.0.0.1" + addr
	}
	pageSize, _ := strconv.Atoi(env("SNS_PAGE_SIZE", "100"))
	region := env("AWS_REGION", "us-east-1")
	accountID := env("AWS_ACCOUNT_ID", "123456789012")
	store, err := openStore(env("SNS_STORE", "memory"), env("SNS_SQLITE_PATH", "/data/sns.sqlite"))
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	material, err := store.LoadSigningMaterial()
	if err != nil {
		log.Fatalf("failed to load signing material: %v", err)
	}
	signer, err := signing.NewProviderFromMaterial("/__sns/certs/current.pem", material)
	if err != nil {
		log.Fatalf("failed to create signing material: %v", err)
	}
	if len(material.CertPEM) == 0 {
		freshMaterial, err := signer.Material()
		if err != nil {
			log.Fatalf("failed to serialize signing material: %v", err)
		}
		if err := store.SaveSigningMaterial(freshMaterial); err != nil {
			log.Fatalf("failed to persist signing material: %v", err)
		}
	}
	creds, err := auth.ParseStaticCredentials(env("SNS_CREDENTIALS", ""))
	if err != nil {
		log.Fatalf("failed to parse SNS_CREDENTIALS: %v", err)
	}
	verifier := auth.NewVerifier(accountID, region, env("SNS_AUTH_MODE", auth.ModeStrict), creds)
	httpAdapter := httpproto.NewAdapter(&http.Client{Timeout: 10 * time.Second})
	var sqsClient *sqsproto.HTTPClient
	if endpoint := strings.TrimSpace(os.Getenv("SQS_ENDPOINT")); endpoint != "" {
		sqsClient = sqsproto.NewHTTPClient(endpoint, &http.Client{Timeout: 10 * time.Second})
	}
	svc := service.New(service.Config{
		Region:    region,
		AccountID: accountID,
		BaseURL:   baseURL,
		PageSize:  pageSize,
	}, store, util.RealClock{}, signer, httpAdapter, sqsClient)
	defer func() {
		if err := svc.Close(); err != nil {
			log.Printf("service close error: %v", err)
		}
	}()
	server := api.New(svc, signer, verifier)
	log.Printf("sns emulator listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, server.Routes()))
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func openStore(kind, sqlitePath string) (storepkg.Store, error) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "", "memory":
		return memory.NewStore(), nil
	case "sqlite":
		if err := os.MkdirAll(filepath.Dir(sqlitePath), 0o755); err != nil {
			return nil, err
		}
		return sqlitestore.Open(sqlitePath)
	default:
		return nil, fmt.Errorf("unsupported SNS_STORE %q", kind)
	}
}
