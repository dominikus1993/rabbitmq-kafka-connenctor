package main

import (
	"rabbitmq-kafka-connenctor/app/config"
	"rabbitmq-kafka-connenctor/app/connector"
)

func main() {
	done := make(chan bool)

	conf := config.GetConf()
	go connector.StartRabbitToKafka(conf)

	<-done
}
