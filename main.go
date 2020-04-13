package main

import (
	"log"
	"rabbitmq-kafka-connenctor/app/rabbitmq"
)

func main() {
	client, err := rabbitmq.NewRabbitMqClient("amqp://guest:guest@rabbitmq:5672/")

	if err != nil {
		log.Fatal(err)
	}
	source, err := rabbitmq.NewRabbitMqSource(client, "Test", "test", "d.p")

	if err != nil {
		log.Fatal(err)
	}

	sink, err := rabbitmq.NewRabbitMqSink(client, "Test2")

	if err != nil {
		log.Fatal(err)
	}

	channel, err := source.Handle()

	if err != nil {
		log.Fatal(err)
	}

	sink.Handle(channel)
}
