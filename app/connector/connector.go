package connector

import (
	"log"
	"rabbitmq-kafka-connenctor/app/config"
	"rabbitmq-kafka-connenctor/app/rabbitmq"
	"rabbitmq-kafka-connenctor/app/rkafka"
)

func StartRabbitToKafka(conf *config.Config) {
	done := make(chan bool)
	router := config.NewMessageRouter(*conf)

	client, err := rabbitmq.NewRabbitMqClient("amqp://guest:guest@rabbitmq:5672/")

	if err != nil {
		log.Fatal(err)
	}
	source := rabbitmq.NewRabbitMqSource(client, conf.RabbitToKafka)

	if err != nil {
		log.Fatal(err)
	}
	channel := source.Handle()

	kafka := rkafka.NewKafkaProducer(rkafka.GetKafkaProducerConfig())

	sink := rkafka.NewKafkaSink(kafka, router)

	go sink.Start(channel)

	<-done
}
