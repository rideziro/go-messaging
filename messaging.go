package go_messaging

import "time"

type Message struct {
	Data      []byte
	Topic     string
	Timestamp time.Time
}

type ConsumerConfig struct {
	Group          string
	Topics         []string
	OverrideConfig map[string]interface{}
	ReadTimeout    time.Duration
}

type ProducerConfig struct {
	Topic          string
	OverrideConfig map[string]interface{}
	SchemaPath     string
}

type Messaging interface {
	NewConsumer(config ConsumerConfig) (Consumer, error)
	NewProducer(config ProducerConfig) (Producer, error)
}

type Consumer interface {
	Consume() (*Message, error)
	Commit() error
}

type Producer interface {
	Produce(id string, data []byte) error
	Flush(timeoutMs int) int
}
