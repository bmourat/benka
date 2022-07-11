package main

import (
	"benka/confluent"
	"benka/franz"
	"benka/sarama"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	. "github.com/onsi/ginkgo/v2"
	_ "github.com/onsi/gomega"
	"github.com/onsi/gomega/gmeasure"
	log "github.com/sirupsen/logrus"
	"time"
)

var _ = Describe("Producing values", func() {
	Specify("Producer", func() {

		experiment := gmeasure.NewExperiment(fmt.Sprintf("%s client producing values", Client))
		AddReportEntry(experiment.Name, experiment)

		experiment.Sample(func(idx int) {
			switch Client {
			case "confluent":
				var producer = confluent.NewProducer(Brokers)
				var processor = confluent.Prepare(producer, Topic, GenMessage(), NumMessages)
				defer producer.Close()

				experiment.MeasureDuration("producing", func() {
					processor()
					<-confluent.Done
				})
			case "sarama":
				var producer = sarama.NewProducer(Brokers)
				var processor = sarama.Prepare(producer, Topic, GenMessage(), NumMessages)
				defer producer.Close()

				experiment.MeasureDuration("producing", func() {
					processor()
					<-sarama.Done
				})
			case "franz":
				var producer = franz.NewProducer(Brokers, Topic)
				var processor = franz.Prepare(producer, GenMessage(), NumMessages)
				defer producer.Close()

				experiment.MeasureDuration("producing", func() {
					processor()
					<-franz.Done
				})
			default:
				log.Panic("Client not specified")
			}
		}, gmeasure.SamplingConfig{N: Samples, Duration: time.Minute * 10})

	})
	/*
		Specify("Confluent producer", func() {
			experiment := gmeasure.NewExperiment("Confluent client producing values")
			AddReportEntry(experiment.Name, experiment)

			var producer = confluent.NewProducer(Brokers)
			var processor = confluent.Prepare(producer, Topic, GenMessage(), NumMessages)
			defer producer.Close()

			experiment.MeasureDuration("producing", func() {
				processor()
				<-confluent.Done
			})
		})

		Specify("Sarama producer", func() {
			experiment := gmeasure.NewExperiment("Sarama client producing values")
			AddReportEntry(experiment.Name, experiment)

			var producer = sarama.NewProducer(Brokers)
			var processor = sarama.Prepare(producer, Topic, GenMessage(), NumMessages)
			defer producer.Close()

			experiment.MeasureDuration("producing", func() {
				processor()
				<-sarama.Done
			})
		})


		Specify("Kafka-go producer", func() {
			experiment := gmeasure.NewExperiment("Kafka-go client producing values")
			AddReportEntry(experiment.Name, experiment)

			var producer = kafkago.NewProducer(Brokers, Topic)
			var processor = kafkago.Prepare(producer, GenMessage(), NumMessages)

			experiment.MeasureDuration("producing", func() {
				processor()
			})

			err := producer.Close()
			if err != nil {
				log.WithError(err).Panic("Unable to close the producer")
			}
		})

		Specify("Franz producer", func() {
			experiment := gmeasure.NewExperiment("Franz client producing values")
			AddReportEntry(experiment.Name, experiment)

			var producer = franz.NewProducer(Brokers, Topic)
			var processor = franz.Prepare(producer, GenMessage(), NumMessages)
			defer producer.Close()

			experiment.MeasureDuration("producing", func() {
				processor()
				<-franz.Done
			})
		})
	*/
})

var _ = Describe("Consuming values", func() {

	BeforeEach(func() {
		PopulateTopic(Topic, NumMessages*Samples)
	})
	Specify("Consumer", func() {

		experiment := gmeasure.NewExperiment(fmt.Sprintf("%s client consuming values", Client))
		AddReportEntry(experiment.Name, experiment)

		experiment.Sample(func(idx int) {
			switch Client {
			case "confluent":
				var consumer = confluent.NewConsumer(Brokers, Topic)
				var process = confluent.PrepareConsumer(consumer, NumMessages)
				defer consumer.Close()

				experiment.MeasureDuration("consuming", func() {
					process()
				})
			case "sarama":
				var consumer = sarama.NewConsumer(Brokers, NumMessages)
				var process = sarama.PrepareConsumer(consumer, Topic)

				experiment.MeasureDuration("consuming", func() {
					process()
				})
			case "franz":
				autoCommit := false
				var consumer = franz.NewConsumer(Brokers, Topic, autoCommit)
				var process = franz.PrepareConsumer(consumer, NumMessages, autoCommit)
				experiment.MeasureDuration("consuming", func() {
					process()
				})
			case "franz-autocommit":
				autoCommit := true
				var consumer = franz.NewConsumer(Brokers, Topic, autoCommit)
				var process = franz.PrepareConsumer(consumer, NumMessages, autoCommit)
				experiment.MeasureDuration("consuming", func() {
					process()
				})
			default:
				log.Panic("Client not specified")
			}
		}, gmeasure.SamplingConfig{N: Samples, Duration: time.Minute * 10})

	})

	/*
		Specify("Confluent consumer", func() {
			experiment := gmeasure.NewExperiment("Confluent client consuming values")
			AddReportEntry(experiment.Name, experiment)
			var consumer = confluent.NewConsumer(Brokers, Topic)
			var process = confluent.PrepareConsumer(consumer, NumMessages)
			defer consumer.Close()

			experiment.MeasureDuration("consuming", func() {
				process()
			})
		})

		Specify("Sarama consumer", func() {
			experiment := gmeasure.NewExperiment("Sarama client consuming values")
			AddReportEntry(experiment.Name, experiment)
			var consumer = sarama.NewConsumer(Brokers, NumMessages)
			var process = sarama.PrepareConsumer(consumer, Topic)

			experiment.MeasureDuration("consuming", func() {
				process()
			})
		})

		Specify("Franz consumer with autocommit", func() {
			experiment := gmeasure.NewExperiment("Franz client with autocommit consuming values")
			AddReportEntry(experiment.Name, experiment)
			autoCommit := true
			var consumer = franz.NewConsumer(Brokers, Topic, autoCommit)
			var process = franz.PrepareConsumer(consumer, NumMessages, autoCommit)
			experiment.MeasureDuration("consuming", func() {
				process()
			})
		})

		Specify("Franz consumer without autocommit", func() {
			experiment := gmeasure.NewExperiment("Franz client without autocommit consuming values")
			AddReportEntry(experiment.Name, experiment)
			autoCommit := false
			var consumer = franz.NewConsumer(Brokers, Topic, autoCommit)
			var process = franz.PrepareConsumer(consumer, NumMessages, autoCommit)
			experiment.MeasureDuration("consuming", func() {
				process()
			})
		})

	*/
})

/*
func CreateTopic() {
	config := &kafka.ConfigMap{"bootstrap.servers": Brokers, "linger.ms": 100}
	admin, err := kafka.NewAdminClient(config)
	defer admin.Close()
	if err != nil {
		log.WithError(err).Panic("Unable to get Admin client")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	timeout, err := time.ParseDuration("60s")
	if err != nil {
		panic("ParseDuration(60s)")
	}

	deleteResult, deleteError := admin.DeleteTopics(
		ctx,
		[]string{Topic},
		kafka.SetAdminOperationTimeout(timeout),
	)
	if deleteError != nil || (deleteResult[0].Error.Code() != kafka.ErrNoError && deleteResult[0].Error.Code() != kafka.ErrUnknownTopicOrPart) {
		log.WithError(err).Panic("Unable to delete test topics")
	}
	createResult, createError := admin.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{Topic: Topic, NumPartitions: 1}},
		kafka.SetAdminOperationTimeout(timeout),
	)
	if createError != nil || createResult[0].Error.Code() != kafka.ErrNoError {
		log.WithError(err).Panic("Unable to create test topics")
	}
}
*/

func PopulateTopic(topic string, messageCount int) {
	var message = GenMessage()
	var producer = confluent.NewProducer(Brokers)

	go func() {
		for e := range producer.Events() {
			switch msg := e.(type) {
			case *kafka.Message:
				if msg.TopicPartition.Error != nil {
					log.WithError(msg.TopicPartition.Error).Panic("Unable to deliver the message")
				}
			}
		}
	}()

	for j := 0; j < messageCount; j++ {
		_ = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value: message,
		}, nil)

	}
	for producer.Flush(1000) > 0 {
	}
	producer.Close()
}
