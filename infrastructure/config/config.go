package config

import (
	"fmt"
	"io/ioutil"
	"log"
	"rabbit-kafka-connector/infrastructure/env"
	"strings"

	"gopkg.in/yaml.v2"
)

type Topic = string

type RabbitMqSubscription struct {
	Exchange string
	Queue    string
	Topic    string
}

type Kafka struct {
	Topic Topic `yaml:"topic"`
}

type RabbitMqToKafkaSubscription struct {
	Topic Topic                `yaml:"topic"`
	From  RabbitMqSubscription `yaml:"from"`
	To    Kafka                `yaml:"to"`
}

type Config struct {
	RabbitToKafka []RabbitMqToKafkaSubscription `yaml:"rabbitmq-to-kafka"`
}

func GetConf() *Config {
	var c Config
	yamlFile, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	return &c
}

func GetTopicNameWithEnvName(topic string) string {
	return env.GetNameWithEnvPrefix(topic)
}

func GetTopicNameWithoutEnvName(topic string) string {
	env := env.GetEnvName()
	return strings.TrimPrefix(topic, fmt.Sprintf("%s.", env))
}

type MessageRouter struct {
	KafkaSinkRouting map[string]Kafka
}

func NewMessageRouter(cfg Config) *MessageRouter {
	kafkaR := make(map[string]Kafka, len(cfg.RabbitToKafka))
	for _, c := range cfg.RabbitToKafka {
		kafkaR[c.Topic] = c.To
	}

	return &MessageRouter{KafkaSinkRouting: kafkaR}
}

func (r *MessageRouter) GetKafkaRouting(topic string) Kafka {
	return r.KafkaSinkRouting[topic]
}
