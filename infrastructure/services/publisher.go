package services

import (
	"context"
	"rabbit-kafka-connector/application/model"

	log "github.com/sirupsen/logrus"
)

type StdOutMesssagePublisher struct {
}

func (pub StdOutMesssagePublisher) Publish(ctx context.Context, msg model.Message) error {
	log.WithField("Data", string(msg.Body)).Infoln("Message Received")
	return nil
}
