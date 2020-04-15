package rkafka

import (
	"fmt"
	"rabbitmq-kafka-connenctor/app/bus"
	"rabbitmq-kafka-connenctor/app/config"
	"rabbitmq-kafka-connenctor/app/env"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Subscriptions = []config.RabbitMqToKafkaSubscription

type KafkaProducer struct {
	Producer *kafka.Producer
}

func GetKafkaServers() string {
	return env.GetEnvOrDefault("KAFKA_SERVERS", "kafka:9092")
}

func GetKafkaProducerConfig() *kafka.ConfigMap {
	return &kafka.ConfigMap{"bootstrap.servers": GetKafkaServers()}
}

func NewKafkaProducer(cfg *kafka.ConfigMap) *KafkaProducer {
	p, err := kafka.NewProducer(cfg)

	env.FailOnError(err, "Failed to connect to kafka")
	return &KafkaProducer{Producer: p}
}

func (producer *KafkaProducer) Close() {
	producer.Producer.Flush(15 * 1000)
	producer.Producer.Close()
}

type KafkaSink struct {
	Client *KafkaProducer
	Router *config.MessageRouter
}

func NewKafkaSink(client *KafkaProducer, router *config.MessageRouter) *KafkaSink {
	return &KafkaSink{Client: client, Router: router}
}

func (sink *KafkaSink) Start(messages bus.EventChannel) {
	for msg := range messages {
		topic := sink.Router.GetKafkaRouting(msg.Topic)
		fmt.Println(topic)
		sink.Client.Producer.ProduceChannel() <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic.Topic, Partition: int32(kafka.PartitionAny)},
			Value:          msg.Data,
		}
	}
}

func getKafkaGroupID() string {
	return env.GetNameWithEnvPrefix("test")
}

type KafkaConsumer struct {
	Consumer *kafka.Consumer
}

func NewConsumer() *KafkaConsumer {
	config := &kafka.ConfigMap{"bootstrap.servers": GetKafkaServers(),
		"client.id": getKafkaGroupID(),
		"group.id":  getKafkaGroupID()}

	c, err := kafka.NewConsumer(config)

	env.FailOnError(err, "Error when trying declare a kafka consumer")
	return &KafkaConsumer{Consumer: c}
}

func CloseKafka(consumer *kafka.Consumer) {
	consumer.Close()
}
