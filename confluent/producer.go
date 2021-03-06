package confluent

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
)

var (
	Done = make(chan bool)
)

// NewProducer returns a new confluent producer.
func NewProducer(brokers string) *kafka.Producer {
	config := &kafka.ConfigMap{"bootstrap.servers": brokers, "linger.ms": 100}
	producer, err := kafka.NewProducer(config)
	if err != nil {
		log.WithError(err).Panic("Unable to start the producer")
	}
	return producer
}

func Prepare(producer *kafka.Producer, topic string, message []byte, numMessages int) func() {
	go func() {
		var msgCount int
		for e := range producer.Events() {
			switch msg := e.(type) {
			case *kafka.Message:
				if msg.TopicPartition.Error != nil {
					log.WithError(msg.TopicPartition.Error).Panic("Unable to deliver the message")
				}
				msgCount++
				if msgCount >= numMessages {
					Done <- true
				}
			}
		}
	}()

	return func() {
		for j := 0; j < numMessages; j++ {
			producer.ProduceChannel() <- &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: kafka.PartitionAny,
				},
				Value: message,
			}
		}
	}
}
