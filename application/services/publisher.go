package services

import (
	"context"
	"rabbit-kafka-connector/application/model"
)

type MessagePublisher interface {
	Publish(ctx context.Context, msg model.Message) error
}
