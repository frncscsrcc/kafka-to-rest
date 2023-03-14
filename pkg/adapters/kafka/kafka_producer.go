package kafka

import (
	"fmt"
	"kafka-to-rest/pkg/config"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaProducerInterface interface {
	Init(cnf config.KafkaConnectorConfig) error
	Produce(message []byte, key []byte) error
	Close() error
}

// --

type RealKafkaProducer struct {
	dlq      string
	producer *kafka.Producer
}

func (p *RealKafkaProducer) Init(cnf config.KafkaConnectorConfig) error {
	kafkaProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": fmt.Sprintf("%s:%d", cnf.Host, cnf.Port),
		"acks":              "all",
	})

	if err != nil {
		return err
	}

	p.producer = kafkaProducer
	p.dlq = cnf.DLQ

	return nil
}

func (p *RealKafkaProducer) Produce(message []byte, key []byte) error {
	delivery_chan := make(chan kafka.Event, 10000)
	defer close(delivery_chan)

	err := p.producer.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &p.dlq, Partition: kafka.PartitionAny},
			Value:          message,
			Key:            key,
		},
		delivery_chan,
	)
	if err != nil {
		return err
	}

	e := <-delivery_chan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("Delivery to DLQ %s failed: %v\n", p.dlq, m.TopicPartition.Error)
		return m.TopicPartition.Error
	}

	return nil
}

func (p *RealKafkaProducer) Close() error {
	p.producer.Close()
	return nil
}

// --

type KafkaProducerFactoryInterface interface {
	Build() KafkaProducerInterface
}

type RealKafkaProducerFactory struct{}

func (f RealKafkaProducerFactory) Build() KafkaProducerInterface {
	return &RealKafkaProducer{}
}
