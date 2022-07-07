package clients

type KafkaClient interface {
	NewConsumer() *KafkaConsumer
	NewProducer() *KafkaProducer
}

type KafkaConsumer interface {
	Consume()
}

type KafkaProducer interface {
	Produce()
}
