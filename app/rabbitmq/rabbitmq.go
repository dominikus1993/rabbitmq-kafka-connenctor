package rabbitmq

import (
	"fmt"

	"github.com/streadway/amqp"
)

type RabbitMqMessage struct {
	Exchange string
	Queue    string
	Topic    string
	Body     []byte
}

type IRabbitMqClient interface {
	Close() error
	Subscribe(exchange, queue, topic string)
	Publish(exchange, topic string)
}

type rabbitMqClient struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
}

func NewRabbitMqClient(connStr string) (*IRabbitMqClient, error) {
	conn, err := amqp.Dial(connStr)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	var client IRabbitMqClient = &rabbitMqClient{Connection: conn, Channel: ch}
	return &client, nil
}

func (client *rabbitMqClient) Close() error {
	err := client.Channel.Close()
	err = client.Connection.Close()
	return err
}

func (client *rabbitMqClient) Subscribe(exchange, queue, topic string) error {
	err := client.Channel.ExchangeDeclare(
		exchange, // name
		"topic",  // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	if err != nil {
		return err
	}

	q, err := client.Channel.QueueDeclare(
		exchange+"-"+queue, // name
		true,               // durable
		false,              // delete when usused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)

	if err != nil {
		return err
	}

	err = client.Channel.QueueBind(
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
