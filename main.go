package main

import (
	"flag"
	"math/rand"
	"time"
)

var (
	Client string
	// Brokers contains the list of brokers, comma-separated, to use.
	Brokers string
	// Topic contains the topic to use in this test.
	Topic string
	// NumMessages contains the number of messages to send.
	Samples     int
	NumMessages int
	// MessageSize contains the size of the message to send.
	MessageSize int
	// Characters Rune to use in the random string generator
	Characters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func GenMessage() []byte {
	b := make([]rune, MessageSize)
	for i := range b {
		b[i] = Characters[rand.Intn(len(Characters))]
	}
	return []byte(string(b))
}

func main() {
	flag.Parse()
}

func init() {
	rand.Seed(time.Now().UnixNano())
	flag.StringVar(&Client, "client", "", "Client to test")
	flag.StringVar(&Brokers, "brokers", "", "Brokers to use for this benchmark")
	flag.StringVar(&Topic, "topic", "", "Topic to use for this benchmark")
	flag.IntVar(&NumMessages, "num", 1000, "Number of messages to send")
	flag.IntVar(&Samples, "samples", 10, "Number of test samples")
	flag.IntVar(&MessageSize, "size", 1000, "Message size to send")
}
