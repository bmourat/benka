package main

import (
	"benka/confluent"
	"benka/sarama"
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	. "github.com/onsi/ginkgo/v2"
	_ "github.com/onsi/gomega"
	"github.com/onsi/gomega/gmeasure"
	log "github.com/sirupsen/logrus"
)

var _ = BeforeSuite(func() {
})

var _ = Describe("Benchmark", func() {
	BeforeEach(func() {
		config := &kafka.ConfigMap{"bootstrap.servers": Brokers, "linger.ms": 100}
		admin, err := kafka.NewAdminClient(config)
		defer admin.Close()
		if err != nil {
			log.WithError(err).Panic("Unable to get Admin client")
		}
		_, err = admin.DeleteTopics(context.Background(), []string{Topic})
		if err != nil {
			log.WithError(err).Panic("Unable to delete test topics")
		}
		_, err = admin.CreateTopics(context.Background(), []kafka.TopicSpecification{{Topic: Topic}})
		if err != nil {
			log.WithError(err).Panic("Unable to create test topics")
		}
	})

	It("Sarama Producer", func() {
		experiment := gmeasure.NewExperiment("Sarama client producing values")
		AddReportEntry(experiment.Name, experiment)

		var producer = sarama.NewProducer(Brokers)
		var processor = sarama.Prepare(producer, Topic, GenMessage(), NumMessages)

		experiment.MeasureDuration("producing", func() {
			processor()
			<-sarama.Done
		})
	})

	It("Confluent Producer", func() {
		experiment := gmeasure.NewExperiment("Confluent client producing values")
		AddReportEntry(experiment.Name, experiment)

		var producer = confluent.NewProducer(Brokers)
		var processor = confluent.Prepare(producer, Topic, GenMessage(), NumMessages)

		experiment.MeasureDuration("producing", func() {
			processor()
			<-confluent.Done
		})
	})
})
