package rabbitmq

import (
	"fmt"
	"rabbitmq-kafka-connenctor/app/bus"

	"github.com/streadway/amqp"
)

type RabbitMqMessage struct {
	Exchange string
	Queue    string
	Topic    string
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
	Client                 *RabbitMqClient
	Exchange, Queue, Topic string
}

func NewRabbitMqSource(client *RabbitMqClient, exchange, queue, topic string) (*RabbitMqSource, error) {
	return &RabbitMqSource{Client: client, Exchange: exchange, Queue: queue, Topic: topic}, nil
}

func (source *RabbitMqSource) Handle() error {
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
		return err
	}

	q, err := source.Client.Channel.QueueDeclare(
		source.Exchange+"-"+source.Queue, // name
		true,                             // durable
		false,                            // delete when usused
		false,                            // exclusive
		false,                            // no-wait
		nil,                              // arguments
	)

	if err != nil {
		return err
	}

	err = source.Client.Channel.QueueBind(
		q.Name,   // queue name
		topic,    // routing key
		exchange, // exchange
		false,
		nil,
	)

	if err != nil {
		return err
	}

	msgs, err := client.Channel.Consume(
		q.Name,             // queue
		exchange+"-"+queue, // consumer
		true,               // auto-ack
		false,              // exclusive
		false,              // no-local
		false,              // no-wait
		nil,                // args
	)

	if err != nil {
		return err
	}

	for msg := range msgs {
		fmt.Println(msg)
	}
}

func (client *rabbitMqClient) Publish(exchange, topic string) {
}
