package services

import (
	"context"
	"rabbit-kafka-connector/application/model"
)

type MessageSubscriber interface {
	Subscribe(ctx context.Context) chan model.Message
}
