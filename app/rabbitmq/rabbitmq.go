package rabbitmq

import (
	"rabbitmq-kafka-connenctor/app/bus"

	"github.com/streadway/amqp"
)

type Topic string

type Subscriptions map[Topic]RabbitMqSubscription

type RabbitMqSubscription struct {
	Exchange string
	Queue    string
	Route    string
	Body     []byte
}

type IRabbitMqSource interface {
	Handle(chan *bus.EventChannel) error
}

type IRabbitMqSink interface {
	Consume(chan *bus.EventChannel)
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
	err := client.Channel.Close()
	err = client.Connection.Close()
	return err
}

type RabbitMqSource struct {
	Client        *RabbitMqClient
	Subscriptions Subscriptions
}

func NewRabbitMqSource(client *RabbitMqClient, subscriptions Subscriptions) *RabbitMqSource {
	return &RabbitMqSource{Client: client, Subscriptions: subscriptions}
}

func (source *RabbitMqSource) Handle() (bus.EventChannel, error) {
	err := source.Client.Channel.ExchangeDeclare(
		source.Exchange, // name
		"topic",         // type
		true,            // durable
		false,           // auto-deleted
		false,           // internal
		false,           // no-wait
		nil,             // arguments
	)

	if err != nil {
		return nil, err
	}

	q, err := source.Client.Channel.QueueDeclare(
		source.Exchange+"-"+source.Queue, // name
		true,                             // durable
		true,                             // delete when usused
		false,                            // exclusive
		false,                            // no-wait
		nil,                              // arguments
	)

	if err != nil {
		return nil, err
	}

	err = source.Client.Channel.QueueBind(
		q.Name,          // queue name
		source.Topic,    // routing key
		source.Exchange, // exchange
		false,
		nil,
	)

	if err != nil {
		return nil, err
	}

	msgs, err := source.Client.Channel.Consume(
		q.Name,                           // queue
		source.Exchange+"-"+source.Queue, // consumer
		true,                             // auto-ack
		false,                            // exclusive
		false,                            // no-local
		false,                            // no-wait
		nil,                              // args
	)

	if err != nil {
		return nil, err
	}

	messages := make(chan *bus.Event)

	go func(evtChan bus.EventChannel) {
		for msg := range msgs {
			evtChan <- &bus.Event{Data: msg.Body, Topic: source.Topic}
		}
	}(messages)

	return messages, nil
}

func declareSubscriber(channel bus.EventChannel) {
	err := source.Client.Channel.ExchangeDeclare(
		source.Exchange, // name
		"topic",         // type
		true,            // durable
		false,           // auto-deleted
		false,           // internal
		false,           // no-wait
		nil,             // arguments
	)

	if err != nil {
		return nil, err
	}

	q, err := source.Client.Channel.QueueDeclare(
		source.Exchange+"-"+source.Queue, // name
		true,                             // durable
		true,                             // delete when usused
		false,                            // exclusive
		false,                            // no-wait
		nil,                              // arguments
	)

	if err != nil {
		return nil, err
	}

	err = source.Client.Channel.QueueBind(
		q.Name,          // queue name
		source.Topic,    // routing key
		source.Exchange, // exchange
		false,
		nil,
	)

	if err != nil {
		return nil, err
	}

	msgs, err := source.Client.Channel.Consume(
		q.Name,                           // queue
		source.Exchange+"-"+source.Queue, // consumer
		true,                             // auto-ack
		false,                            // exclusive
		false,                            // no-local
		false,                            // no-wait
		nil,                              // args
	)

	if err != nil {
		return nil, err
	}

	messages := make(chan *bus.Event)

	go func(evtChan bus.EventChannel) {
		for msg := range msgs {
			evtChan <- &bus.Event{Data: msg.Body, Topic: source.Topic}
		}
	}(messages)

	return messages, nil
}

type RabbitMqSink struct {
	Client   *RabbitMqClient
	Exchange string
}

func NewRabbitMqSink(client *RabbitMqClient, exchange string) (*RabbitMqSink, error) {
	return &RabbitMqSink{Client: client, Exchange: exchange}, nil
}

func (source *RabbitMqSink) Handle(messages bus.EventChannel) {
	for msg := range messages {
		source.Client.Channel.ExchangeDeclare(
			source.Exchange, // name
			"topic",         // type
			true,            // durable
			false,           // auto-deleted
			false,           // internal
			false,           // no-wait
			nil,             // arguments
		)

		source.Client.Channel.Publish(source.Exchange, msg.Topic, false, false, amqp.Publishing{ContentType: "application/json", Body: msg.Data})
	}
}
