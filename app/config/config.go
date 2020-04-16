package config

import (
	"fmt"
	"io/ioutil"
	"log"
	"rabbitmq-kafka-connenctor/app/env"
	"strings"

	"gopkg.in/yaml.v2"
)

type Topic = string

type RabbitMq struct {
	Topic    Topic  `yaml:"topic"`
	Queue    string `yaml:"queue"`
	Exchange string `yaml:"exchange"`
}

type Kafka struct {
	Topic Topic `yaml:"topic"`
}

type RabbitMqToKafkaSubscription struct {
	Topic Topic    `yaml:"topic"`
	From  RabbitMq `yaml:"from"`
	To    Kafka    `yaml:"to"`
}

type KafkaToRabbitMqSubscription struct {
	Topic Topic    `yaml:"topic"`
	From  Kafka    `yaml:"from"`
	To    RabbitMq `yaml:"to"`
}

type Config struct {
	KafkaToRabit  []KafkaToRabbitMqSubscription `yaml:"kafka-to-rabbitmq"`
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
	KafkaSinkRouting    map[string]Kafka
	RabbitMqSinkRouting map[string]RabbitMq
}

func NewMessageRouter(cfg Config) *MessageRouter {
	kafkaR := make(map[string]Kafka, len(cfg.RabbitToKafka))
	for _, c := range cfg.RabbitToKafka {
		kafkaR[c.Topic] = c.To
	}

	rabbitR := make(map[string]RabbitMq, len(cfg.KafkaToRabit))
	for _, c := range cfg.KafkaToRabit {
		rabbitR[c.Topic] = c.To
	}
	return &MessageRouter{KafkaSinkRouting: kafkaR, RabbitMqSinkRouting: rabbitR}
}

func (r *MessageRouter) GetKafkaRouting(topic string) Kafka {
	return r.KafkaSinkRouting[topic]
}

func (r *MessageRouter) GetRabbitRouting(topic string) RabbitMq {
	return r.RabbitMqSinkRouting[topic]
}
