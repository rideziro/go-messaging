package kafka

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	go_messaging "github.com/rideziro/go-messaging"
	"github.com/riferrei/srclient"
	"time"
)

var (
	ErrTimeout = errors.New("local timeout")
)

type Consumer struct {
	consumer     *kafka.Consumer
	readTimeout  time.Duration
	schemaClient *srclient.SchemaRegistryClient
}

func (c *Consumer) Consume() (*go_messaging.Message, error) {
	msg, err := c.consumer.ReadMessage(c.readTimeout)
	if err != nil {
		return nil, ErrTimeout
	}

	schemaID := binary.BigEndian.Uint32(msg.Value[1:5])
	schema, err := c.schemaClient.GetSchema(int(schemaID))
	if err != nil {
		return nil, fmt.Errorf("error getting the schema with id '%d' %s", schemaID, err)
	}
	native, _, err := schema.Codec().NativeFromBinary(msg.Value[5:])
	if err != nil {
		return nil, fmt.Errorf("unable to decode from binary: %w", err)
	}
	value, err := schema.Codec().TextualFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("unable to decode from native: %w", err)
	}

	topic := ""
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}
	return &go_messaging.Message{
		Data:      value,
		Topic:     topic,
		Timestamp: msg.Timestamp,
	}, nil
}

func (c *Consumer) Commit() error {
	_, err := c.consumer.Commit()
	return err
}
