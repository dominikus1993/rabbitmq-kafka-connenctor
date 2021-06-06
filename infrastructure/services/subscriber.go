package services

import (
	"context"
	"rabbit-kafka-connector/application/model"
	"rabbit-kafka-connector/application/services"
	"rabbit-kafka-connector/infrastructure/config"
	"sync"

	log "github.com/sirupsen/logrus"
)

type Subscription struct {
	RabbitMq config.RabbitMqSubscription
	Topic    config.Topic
}

func NewSubscription(cfg config.RabbitMqToKafkaSubscription) Subscription {
	return Subscription{Topic: cfg.Topic, RabbitMq: cfg.From}
}

func NewSubscriptions(cfg config.Subscriptions) []Subscription {
	res := make([]Subscription, len(cfg))
	for i, elem := range cfg {
		res[i] = NewSubscription(elem)
	}
	return res
}

type RabbitMqMessageSubscriber struct {
	Client        *RabbitMqClient
	Subscriptions []Subscription
}

func (source *RabbitMqMessageSubscriber) declareSubscription(ch chan model.Message, cfg Subscription, wg *sync.WaitGroup) {
	err := source.Client.Channel.ExchangeDeclare(
		cfg.RabbitMq.Exchange, // name
		"topic",               // type
		true,                  // durable
		false,                 // auto-deleted
		false,                 // internal
		false,                 // no-wait
		nil,                   // arguments
	)

	if err != nil {
		log.WithError(err).Fatal("Error when trying exchange declare")
	}

	q, err := source.Client.Channel.QueueDeclare(
		cfg.RabbitMq.Queue, // name
		true,               // durable
		false,              // delete when usused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)

	if err != nil {
		log.WithError(err).Fatal("Error when trying queue declare")
	}

	err = source.Client.Channel.QueueBind(
		q.Name,                // queue name
		cfg.RabbitMq.Topic,    // routing key
		cfg.RabbitMq.Exchange, // exchange
		false,
		nil,
	)

	if err != nil {
		log.WithError(err).Fatal("Error when trying queue bind")
	}

	msgs, err := source.Client.Channel.Consume(
		q.Name, // queue
		cfg.RabbitMq.Exchange+"-"+cfg.RabbitMq.Queue+"-"+cfg.RabbitMq.Topic, // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)

	if err != nil {
		log.WithError(err).Fatal("Error when trying queue bind")
	}

	go func(evtChan chan model.Message, wg *sync.WaitGroup) {
		defer wg.Done()
		for msg := range msgs {
			evtChan <- model.Message{Body: msg.Body, Key: cfg.Topic}
		}
	}(ch, wg)
}

func (source RabbitMqMessageSubscriber) Subscribe(ctx context.Context) chan model.Message {
	stream := make(chan model.Message)
	wait := &sync.WaitGroup{}

	for _, subscription := range source.Subscriptions {
		wait.Add(1)
		source.declareSubscription(stream, subscription, wait)
	}

	go func(evtChan chan model.Message, wg *sync.WaitGroup) {
		wg.Wait()
		close(evtChan)
	}(stream, wait)

	return stream
}

func NewRabbitMqSubscriber(client *RabbitMqClient, subscriptions config.Subscriptions) services.MessageSubscriber {
	return RabbitMqMessageSubscriber{Client: client, Subscriptions: NewSubscriptions(subscriptions)}
}
