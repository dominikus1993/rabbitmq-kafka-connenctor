package rabbitmq

import "github.com/streadway/amqp"

type IRabbitMqClient interface {
	Connect()
	CreateChannel()
	Close()
	Subscribe(exchange, queue, topic string)
	Publish(exchange, topic string)
}

type RabbitMqClient struct {
	Connection *amqp.Connection
}

func NewRabbitMqClient(connStr string) (*RabbitMqClient, error) {
	conn, err := amqp.Dial(connStr)
	env.FailOnError(err, "Failed to connect to RabbitMQ")
	return conn
}
