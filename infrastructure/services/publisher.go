package services

import (
	"context"
	"rabbit-kafka-connector/application/model"
	"rabbit-kafka-connector/infrastructure/config"
	"rabbit-kafka-connector/infrastructure/env"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type KafkaProducer struct {
	Producer *kafka.Producer
}

func GetKafkaServers() string {
	return env.GetEnvOrDefault("KAFKA_SERVERS", "kafka:9092")
}

func GetKafkaProducerConfig() *kafka.ConfigMap {
	return &kafka.ConfigMap{"bootstrap.servers": GetKafkaServers(),
		"client.id":                  "rossmannpl",
		"security.protocol":          "sasl_plaintext",
		"sasl.kerberos.principal":    "kafka/pd.rossmann.com.pl@PD.ROSSMANN.COM.PL",
		"sasl.kerberos.service.name": "kafka",
		"sasl.mechanisms":            "GSSAPI",
		"auto.offset.reset":          "earliest",
		"sasl.kerberos.keytab":       "kafka_aws@ROSSMANN.COM.PL.keytab",
		"sasl.kerberos.kinit.cmd":    "kinit -kt kafka_aws@ROSSMANN.COM.PL.keytab kafka_aws@PD.ROSSMANN.COM.PL"}
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

func (sink *KafkaSink) Publish(ctx context.Context, msg model.Message) error {
	topic := sink.Router.GetKafkaRouting(msg.Key)

	sink.Client.Producer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic.Topic, Partition: int32(kafka.PartitionAny)},
		Value:          msg.Body,
	}
	return nil
}
