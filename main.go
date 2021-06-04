package main

import (
	"context"
	"fmt"
	"rabbit-kafka-connector/infrastructure/config"
	"rabbit-kafka-connector/infrastructure/env"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type Message struct {
	Key  string
	Body []byte
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

func NewRabbitMqClient(connStr string) (*RabbitMqClient, error) {
	conn, err := amqp.Dial(connStr)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return &RabbitMqClient{Connection: conn, Channel: ch}, nil
}

func (client *RabbitMqClient) Close() error {
	err1 := client.Channel.Close()
	err2 := client.Connection.Close()
	if err1 != nil || err2 != nil {
		return fmt.Errorf("Channel Close Error %w; Client Connection Close Error %w", err1, err2)
	}
	return nil
}

type StdOutMesssagePublisher struct {
}

func (pub StdOutMesssagePublisher) Publish(ctx context.Context, msg Message) error {
	log.WithField("Data", string(msg.Body)).Infoln("Message Received")
	return nil
}

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

func (source *RabbitMqMessageSubscriber) declareSubscription(ch chan Message, cfg Subscription, wg *sync.WaitGroup) {
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

	go func(evtChan chan Message, wg *sync.WaitGroup) {
		defer wg.Done()
		for msg := range msgs {
			evtChan <- Message{Body: msg.Body, Key: cfg.Topic}
		}
	}(ch, wg)
}

func (source RabbitMqMessageSubscriber) Subscribe(ctx context.Context) chan Message {
	stream := make(chan Message)
	wait := &sync.WaitGroup{}

	for _, subscription := range source.Subscriptions {
		wait.Add(1)
		source.declareSubscription(stream, subscription, wait)
	}

	go func(evtChan chan Message, wg *sync.WaitGroup) {
		wg.Wait()
		close(evtChan)
	}(stream, wait)

	return stream
}

func NewRabbitMqSubscriber(client *RabbitMqClient, subscriptions config.Subscriptions) MessageSubscriber {
	return RabbitMqMessageSubscriber{Client: client, Subscriptions: NewSubscriptions(subscriptions)}
}

type RabbitMqMessageProducer struct {
	Subscriber MessageSubscriber
	Publisher  MessagePublisher
}

func (app *RabbitMqMessageProducer) Execute(ctx context.Context) {
	for message := range app.Subscriber.Subscribe(ctx) {
		err := app.Publisher.Publish(ctx, message)
		if err != nil {
			log.WithContext(ctx).WithError(err).Fatalln("Error when trying publish message")
		}
	}
}

func StartRabbitToKafka() {
	// router := config.NewMessageRouter(*conf)
	log.Infoln("Start RabbitToKafka")
	conf := config.GetConf()
	ctx := context.TODO()
	client, err := NewRabbitMqClient(env.GetEnvOrDefault("RabbitMq__Connection", "amqp://guest:guest@rabbitmq:5672/"))
	if err != nil {
		log.WithError(err).Fatalln("Error when trying connect to rabbitmq")
	}
	defer client.Close()

	if err != nil {
		log.Fatal(err)
	}
	subscriber := NewRabbitMqSubscriber(client, conf.RabbitToKafka)

	if err != nil {
		log.Fatal(err)
	}
	publisher := StdOutMesssagePublisher{}
	usecase := RabbitMqMessageProducer{Subscriber: subscriber, Publisher: publisher}

	usecase.Execute(ctx)
	log.Infoln("Finish RabbitToKafka")
}

func main() {
	StartRabbitToKafka()
}
