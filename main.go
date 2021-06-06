package main

import (
	"context"
	"rabbit-kafka-connector/application/services"
	"rabbit-kafka-connector/infrastructure/config"
	"rabbit-kafka-connector/infrastructure/env"
	infra "rabbit-kafka-connector/infrastructure/services"

	log "github.com/sirupsen/logrus"
)

type RabbitMqMessageProducer struct {
	Subscriber services.MessageSubscriber
	Publisher  services.MessagePublisher
}

func (app *RabbitMqMessageProducer) Execute(ctx context.Context) {
	for message := range app.Subscriber.Subscribe(ctx) {
		err := app.Publisher.Publish(ctx, message)
		if err != nil {
			log.WithContext(ctx).WithError(err).Fatalln("Error when trying publish message")
		}
	}
}

func StartRabbitToKafka() {
	// router := config.NewMessageRouter(*conf)
	log.Infoln("Start RabbitToKafka")
	conf := config.GetConf()
	ctx := context.TODO()
	client, err := infra.NewRabbitMqClient(env.GetEnvOrDefault("RabbitMq__Connection", "amqp://guest:guest@rabbitmq:5672/"))
	if err != nil {
		log.WithError(err).Fatalln("Error when trying connect to rabbitmq")
	}
	defer client.Close()

	if err != nil {
		log.Fatal(err)
	}
	subscriber := infra.NewRabbitMqSubscriber(client, conf.RabbitToKafka)

	if err != nil {
		log.Fatal(err)
	}
	publisher := infra.StdOutMesssagePublisher{}
	usecase := RabbitMqMessageProducer{Subscriber: subscriber, Publisher: publisher}

	usecase.Execute(ctx)
	log.Infoln("Finish RabbitToKafka")
}

func main() {
	StartRabbitToKafka()
}
