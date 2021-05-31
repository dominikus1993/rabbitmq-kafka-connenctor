package main

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type Message struct {
	Key  string
	Body []byte
}

type RabbitMqSubscription struct {
	Exchange, Queue, Topic string
}

type MessagePublisher interface {
	Publish(ctx context.Context, msg Message) error
}

type MessageSubscriber interface {
	Subscribe(ctx context.Context) chan Message
}

type RabbitMqClient struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
}

type RabbitMqMessageSubscriber struct {
	Client        *RabbitMqClient
	Subscriptions []RabbitMqSubscription
}

func (source *RabbitMqMessageSubscriber) declareSubscription(ch chan Message, cfg RabbitMqSubscription) {
	err := source.Client.Channel.ExchangeDeclare(
		cfg.Exchange, // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)

	if err != nil {
		log.Fatal(err)
	}

	q, err := source.Client.Channel.QueueDeclare(
		cfg.Queue, // name
		true,      // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	if err != nil {
		log.Fatal(err)
	}

	err = source.Client.Channel.QueueBind(
		q.Name,       // queue name
		cfg.Topic,    // routing key
		cfg.Exchange, // exchange
		false,
		nil,
	)

	if err != nil {
		log.Fatal(err)
	}

	msgs, err := source.Client.Channel.Consume(
		q.Name, // queue
		cfg.Exchange+"-"+cfg.Queue+"-"+cfg.Topic, // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)

	if err != nil {
		log.Fatal(err)
	}

	go func(evtChan chan Message) {
		for msg := range msgs {
			evtChan <- Message{Body: msg.Body, Key: cfg.Topic}
		}
	}(ch)
}

func (source *RabbitMqMessageSubscriber) Subscribe(ctx context.Context) chan Message {
	for subscription := range source.Subscriptions {
		source.declareSubscription()
	}
}

type App struct {
	logger log.Logger
}

func (app *App) subscribeAndPublishMessage(ctx context.Context, sub MessageSubscriber, pub MessagePublisher) {
	for message := range sub.Subscribe(ctx) {
		err := pub.Publish(ctx, message)
		if err != nil {
			app.logger.WithContext(ctx).WithError(err).Fatalln("Error when trying publish message")
		}
	}
}

func main() {
	fmt.Println("xD")
}
