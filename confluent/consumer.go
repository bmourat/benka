package confluent

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"math/rand"
)

func NewConsumer(brokers string, topic string) *kafka.Consumer {
	config := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          fmt.Sprintf("group-%d", rand.Intn(10000)),
		"auto.offset.reset": "earliest",
	}
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.WithError(err).Panic("Unable to start the consumer")
	}
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.WithError(err).Panic("Unable to subscribe to the topic")
	}
	return consumer
}

func PrepareConsumer(consumer *kafka.Consumer, numMessages int) func() {
	return func() {
		var messageCount = numMessages
		for {
			event := consumer.Poll(100)
			if event == nil {
				continue
			}

			switch e := event.(type) {
			case *kafka.Message:
				messageCount--
			case kafka.Error:
				log.WithError(e).Panic("Unable to consume messages")
			}
			if messageCount == 0 {
				break
			}
		}
	}
}
