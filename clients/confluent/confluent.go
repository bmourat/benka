package confluent

import (
	"benka/clients"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

type Client struct {
	Brokers string
}

type Producer struct {
	producer *kafka.Producer
}

type Consumer struct {
	consumer *kafka.Consumer
}

func (client Client) NewProducer() *clients.KafkaProducer {
	config := &kafka.ConfigMap{"bootstrap.servers": client.Brokers, "linger.ms": 100}
	producer, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatal("Unable to start the producer")
	}
	var newProducer clients.KafkaProducer = Producer{producer: producer}
	return &newProducer
}

func (client Client) NewConsumer() *clients.KafkaConsumer {

	var newConsumer clients.KafkaConsumer = Consumer{}
	return &newConsumer
}

func (producer Producer) Produce() {

}

func (consumer Consumer) Consume() {

}
