package main

import (
	"fmt"
	"log"
	"rabbitmq-kafka-connenctor/app/config"
	"rabbitmq-kafka-connenctor/app/rabbitmq"
)

func main() {
	conf := config.GetConf()
	fmt.Println(conf)

	client, err := rabbitmq.NewRabbitMqClient("amqp://guest:guest@rabbitmq:5672/")

	if err != nil {
		log.Fatal(err)
	}
	source := rabbitmq.NewRabbitMqSource(client, conf.RabbitToKafka)

	if err != nil {
		log.Fatal(err)
	}
	channel := source.Handle()

	for msg := range channel {
		fmt.Println(msg)
	}
}
