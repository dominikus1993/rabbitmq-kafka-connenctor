package services

import (
	"fmt"

	"github.com/streadway/amqp"
)

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
