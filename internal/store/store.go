package store

import (
	"context"
	"time"

	"emulator-aws-sns/internal/domain"
)

type Reader interface {
	GetTopic(arn string) (*domain.Topic, error)
	FindTopicByName(name string) (*domain.Topic, error)
	ListTopics(nextToken string, pageSize int) ([]*domain.Topic, string)
	GetSubscription(arn string) (*domain.Subscription, error)
	ListSubscriptions(nextToken string, pageSize int) ([]*domain.Subscription, string)
	ListSubscriptionsByTopic(topicARN, nextToken string, pageSize int) ([]*domain.Subscription, string)
	GetConfirmationToken(token string) (domain.ConfirmationToken, error)
}

type Writer interface {
	CreateTopic(topic *domain.Topic) error
	UpdateTopic(arn string, fn func(*domain.Topic) error) (*domain.Topic, error)
	DeleteTopic(arn string) ([]*domain.Subscription, error)
	CreateSubscription(sub *domain.Subscription) error
	UpdateSubscription(arn string, fn func(*domain.Subscription) error) (*domain.Subscription, error)
	DeleteSubscription(arn string) (*domain.Subscription, error)
	PutConfirmationToken(token domain.ConfirmationToken)
	DeleteConfirmationToken(token string)
	EnqueueDeliveryJob(job *domain.DeliveryJob) error
}

type Tx interface {
	Reader
	Writer
}

type Store interface {
	Reader
	Writer
	Tx(context.Context, func(Tx) error) error
	LoadSigningMaterial() (domain.SigningMaterial, error)
	SaveSigningMaterial(material domain.SigningMaterial) error
	ListReadyDeliveryJobs(now time.Time, limit int) ([]*domain.DeliveryJob, error)
	UpdateDeliveryJob(id string, fn func(*domain.DeliveryJob) error) (*domain.DeliveryJob, error)
	DeleteDeliveryJob(id string) error
	Health(context.Context) error
	Close() error
}
