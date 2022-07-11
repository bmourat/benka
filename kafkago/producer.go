package kafkago

import (
	"context"
	"fmt"
	kafkago "github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"time"
)

func NewProducer(brokers string, topic string) *kafkago.Writer {
	return kafkago.NewWriter(kafkago.WriterConfig{
		Brokers:       []string{brokers},
		Topic:         topic,
		Balancer:      &kafkago.Hash{},
		BatchTimeout:  time.Duration(100) * time.Millisecond,
		QueueCapacity: 10000,
		BatchSize:     1000000,
		// Async doesn't allow us to know if message has been successfully sent to Kafka.
		// Async:         true,
	})

	/*
		return &kafkago.Writer{
			Addr:         kafkago.TCP(brokers),
			Topic:        topic,
			Balancer:     &kafkago.Hash{},
			BatchTimeout: time.Duration(100) * time.Millisecond,
			BatchSize:    1000000,
		}

	*/
}

func Prepare(producer *kafkago.Writer, message []byte, numMessages int) func() {
	return func() {
		for j := 0; j < numMessages; j++ {
			err := producer.WriteMessages(context.Background(), kafkago.Message{Key: []byte(fmt.Sprintf("kafkago %d", j)), Value: message})
			if err != nil {
				log.WithError(err).Panic("Unable to deliver the message")
			}
		}
	}
}
