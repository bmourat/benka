package sarama

import (
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
)

var (
	Done = make(chan bool)
)

// NewProducer returns a new Sarama async producer.
func NewProducer(brokers string) sarama.AsyncProducer {
	config := sarama.NewConfig()
	config.Version = sarama.V3_1_0_0
	config.Producer.Return.Successes = true
	config.Producer.Flush.Frequency = time.Duration(100) * time.Millisecond
	sarama.MaxRequestSize = 999000

	log.Infof("Connecting to %s", brokers)
	producer, err := sarama.NewAsyncProducer(strings.Split(brokers, ","), config)
	if err != nil {
		log.WithError(err).Panic("Unable to start the producer")
	}
	return producer
}

// Prepare returns a function that can be used during the benchmark as it only
// performs the sending of messages, checking that the sending was successful.
func Prepare(producer sarama.AsyncProducer, topic string, message []byte, numMessages int) func() {
	log.Infof("Preparing to send message of %d bytes %d times", len(message), numMessages)

	go func() {
		var nomessages int
		for range producer.Successes() {
			nomessages++
			if nomessages%numMessages == 0 {
				log.Infof("Sent %d messages... stopping...", nomessages)
				Done <- true
			}
		}
	}()

	go func() {
		for err := range producer.Errors() {
			log.WithError(err).Panic("Unable to deliver the message")
		}
	}()

	return func() {
		for j := 0; j < numMessages; j++ {
			producer.Input() <- &sarama.ProducerMessage{
				Topic:     topic,
				Partition: kafka.PartitionAny,
				Value:     sarama.ByteEncoder(message),
			}
		}
	}
}
