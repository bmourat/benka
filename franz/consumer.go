package franz

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
	"math/rand"
)

func NewConsumer(brokers string, topic string, autoCommit bool) *kgo.Client {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(fmt.Sprintf("group-%d", rand.Intn(10000))),
	}
	if !autoCommit {
		opts = append(opts, kgo.DisableAutoCommit())
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.WithError(err).Panicf("unable to initialize client: %v", err)
	}
	return cl
}

func PrepareConsumer(consumer *kgo.Client, numMessages int, autoCommit bool) func() {
	return func() {
		var messageCount = 0
		var records []*kgo.Record
	MainLoop:
		for {
			fetches := consumer.PollFetches(context.Background())
			if err := fetches.Err(); err != nil {
				log.WithError(err).Panic("Error consuming with Franz client")
			}
			for iter := fetches.RecordIter(); !iter.Done(); {
				records = append(records, iter.Next())
				messageCount++
				if messageCount >= numMessages {
					break MainLoop
				}
			}
		}

		if !autoCommit {
			if err := consumer.CommitRecords(context.Background(), records...); err != nil {
				log.WithError(err).Panic("Committing records with Franz failed")
			}
		}
	}
}
