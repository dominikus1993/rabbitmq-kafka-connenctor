package connector

import (
	"log"
	"rabbitmq-kafka-connenctor/app/config"
	"rabbitmq-kafka-connenctor/app/rabbitmq"
	"rabbitmq-kafka-connenctor/app/rkafka"
)

func Map(vs []config.KafkaToRabbitMqSubscription, f func(config.KafkaToRabbitMqSubscription) string) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}

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

func StartKafkaToRabbit(conf *config.Config) {
	done := make(chan bool)
	router := config.NewMessageRouter(*conf)

	client, err := rabbitmq.NewRabbitMqClient("amqp://guest:guest@rabbitmq:5672/")

	if err != nil {
		log.Fatal(err)
	}
	kafka := rkafka.NewConsumer(rkafka.GetKafkaConsumerConfig())

	source := rkafka.NewKafkaSource(kafka, router)

	if err != nil {
		log.Fatal(err)
	}

	channel := source.Start(Map(conf.KafkaToRabit, func(x config.KafkaToRabbitMqSubscription) string { return x.Topic }))

	sink, err := rabbitmq.NewRabbitMqSink(client, router)

	if err != nil {
		log.Fatalln(err)
	}

	go sink.Handle(channel)

	<-done
}
