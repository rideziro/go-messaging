package kafka

import (
	"encoding/binary"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/riferrei/srclient"
)

type Producer struct {
	topic    string
	producer *kafka.Producer

	schema *srclient.Schema
}

func (p *Producer) Produce(id string, data []byte) error {
	topic := p.topic
	schema := p.schema

	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))
	native, _, err := schema.Codec().NativeFromTextual(data)
	if err != nil {
		return err
	}
	valueBytes, err := schema.Codec().BinaryFromNative(nil, native)
	if err != nil {
		return err
	}
	var recordValue []byte
	recordValue = append(recordValue, byte(0))
	recordValue = append(recordValue, schemaIDBytes...)
	recordValue = append(recordValue, valueBytes...)

	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic, Partition: kafka.PartitionAny,
		},
		Value: recordValue,
		Key:   []byte(id),
	}, nil)
	if err != nil {
		return err
	}

	return nil
}

func (p *Producer) Flush(timeoutMs int) int {
	return p.producer.Flush(timeoutMs)
}
