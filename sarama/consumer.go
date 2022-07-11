package sarama

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"strings"
	"sync"
)

type Consumer struct {
	Client      sarama.ConsumerGroup
	ctx         context.Context
	cancel      context.CancelFunc
	counter     int
	numMessages int
	Ready       chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.Ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		consumer.counter++
		if consumer.counter >= consumer.numMessages {
			consumer.cancel()
			return nil
		}
		session.MarkMessage(message, "")
	}
	return nil
}

func NewConsumer(brokers string, numMessages int) *Consumer {
	config := sarama.NewConfig()
	config.Version = sarama.V3_1_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	ctx, cancel := context.WithCancel(context.Background())
	group := fmt.Sprintf("group-%d", rand.Intn(10000))
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.WithError(err).Panic("Error creating consumer group client")
	}

	consumer := &Consumer{
		Client:      client,
		ctx:         ctx,
		cancel:      cancel,
		numMessages: numMessages,
		Ready:       make(chan bool),
	}
	return consumer
}

func PrepareConsumer(consumer *Consumer, topic string) func() {
	return func() {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if err := consumer.Client.Consume(consumer.ctx, []string{topic}, consumer); err != nil {
					log.WithError(err).Panic("Error from consumer")
				}
				if consumer.ctx.Err() != nil {
					return
				}
				consumer.Ready = make(chan bool)
			}
		}()

		<-consumer.Ready
		wg.Wait()
	}
}
