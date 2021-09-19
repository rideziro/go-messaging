package memory

import (
	"errors"
	go_messaging "github.com/rideziro/go-messaging"
)

type Memory struct{}

func (m *Memory) NewConsumer(config go_messaging.ConsumerConfig) (go_messaging.Consumer, error) {
	panic("implement me")
}

func (m *Memory) NewProducer(config go_messaging.ProducerConfig) (go_messaging.Producer, error) {
	panic("implement me")
}

func (m *Memory) Produce(id string, data []byte) error {
	return nil
}

func (m *Memory) Flush(timeoutMs int) int {
	return 0
}

func (m *Memory) Consume() (*go_messaging.Message, error) {
	return nil, errors.New("no new message")
}

func (m *Memory) Commit() error {
	return nil
}

func NewMemoryClient() go_messaging.Messaging {
	return &Memory{}
}
