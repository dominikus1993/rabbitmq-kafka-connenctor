package rabbitmq

type IRabbitMqClient interface {
	Connect(connStr string)
	CreateChannel()
	Close()
}

type RabbitMqClient struct {
}
