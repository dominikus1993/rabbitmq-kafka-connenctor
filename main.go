package main

import (
	"fmt"
	"rabbitmq-kafka-connenctor/app/config"
)

func main() {
	conf := config.GetConf()
	fmt.Println(conf)
}
