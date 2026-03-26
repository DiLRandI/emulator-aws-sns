package util

import (
	"fmt"
	"regexp"
	"strings"
)

var topicNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}(\.fifo)?$`)

type ARN struct {
	Partition string
	Service   string
	Region    string
	AccountID string
	Resource  string
}

func ParseARN(v string) (ARN, error) {
	parts := strings.SplitN(v, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" {
		return ARN{}, fmt.Errorf("invalid ARN")
	}
	return ARN{
		Partition: parts[1],
		Service:   parts[2],
		Region:    parts[3],
		AccountID: parts[4],
		Resource:  parts[5],
	}, nil
}

func FormatARN(service, region, accountID, resource string) string {
	return fmt.Sprintf("arn:aws:%s:%s:%s:%s", service, region, accountID, resource)
}

func ValidateTopicName(name string, fifo bool) error {
	if !topicNamePattern.MatchString(name) {
		return fmt.Errorf("invalid topic name")
	}
	if fifo && !strings.HasSuffix(name, ".fifo") {
		return fmt.Errorf("FIFO topic names must end with .fifo")
	}
	if !fifo && strings.HasSuffix(name, ".fifo") {
		return fmt.Errorf("non-FIFO topics must not end with .fifo")
	}
	return nil
}

func ValidateFIFOIdentifier(v string, field string) error {
	if v == "" {
		return fmt.Errorf("%s is required", field)
	}
	if len(v) > 128 {
		return fmt.Errorf("%s exceeds maximum length", field)
	}
	return nil
}

func TopicResourceName(topicARN string) (string, error) {
	arn, err := ParseARN(topicARN)
	if err != nil {
		return "", err
	}
	return arn.Resource, nil
}
