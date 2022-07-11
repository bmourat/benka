package franz

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
	"sync/atomic"
)

var (
	Done = make(chan bool)
)

func NewProducer(brokers string, topic string) *kgo.Client {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers),
		kgo.DefaultProduceTopic(topic),
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.WithError(err).Panicf("unable to initialize client: %v", err)
	}
	return cl
}

func Prepare(producer *kgo.Client, message []byte, numMessages int) func() {
	return func() {
		var messagesSent int64 = 0
		for i := 0; i < numMessages; i++ {
			record := kgo.Record{Value: message}
			producer.Produce(context.Background(), &record, func(r *kgo.Record, err error) {
				if err != nil {
					log.WithError(err).Panic("Unable to produce with Franz")
				}
				atomic.AddInt64(&messagesSent, 1)
				if messagesSent >= int64(numMessages) {
					Done <- true
				}
			})
		}
	}
}
