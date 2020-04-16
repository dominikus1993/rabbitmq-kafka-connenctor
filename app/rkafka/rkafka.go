package rkafka

import (
	"fmt"
	"log"
	"rabbitmq-kafka-connenctor/app/bus"
	"rabbitmq-kafka-connenctor/app/config"
	"rabbitmq-kafka-connenctor/app/env"
	"strings"

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

func GetKafkaConsumerConfig() *kafka.ConfigMap {
	return &kafka.ConfigMap{"bootstrap.servers": GetKafkaServers(),
		"client.id": getKafkaGroupID(),
		"group.id":  getKafkaGroupID()}
}

func NewConsumer(config *kafka.ConfigMap) *KafkaConsumer {

	c, err := kafka.NewConsumer(config)

	env.FailOnError(err, "Error when trying declare a kafka consumer")
	return &KafkaConsumer{Consumer: c}
}

type KafkaSource struct {
	Client *KafkaConsumer
}

func NewKafkaSource(client *KafkaConsumer, router *config.MessageRouter) *KafkaSource {
	return &KafkaSource{Client: client}
}

func getTopicNameWithoutEnv(topic, envName string) string {
	return strings.TrimPrefix(topic, fmt.Sprintf("%s.", envName))
}

func (source *KafkaSource) Start(topics []string) chan *bus.Event {
	consumer := source.Client.Consumer
	messages := make(chan *bus.Event)
	go func(channel bus.EventChannel) {
		consumer.SubscribeTopics(topics, nil)
		for {
			msg, err := consumer.ReadMessage(-1)
			if err == nil {
				channel <- &bus.Event{Topic: *msg.TopicPartition.Topic, Data: msg.Value}
			} else {
				// The client will automatically try to recover from all errors.
				log.Printf("Consumer error: %v (%v)\n", err, msg)
			}
		}
	}(messages)
	return messages
}

func CloseKafka(consumer *kafka.Consumer) {
	consumer.Close()
}
