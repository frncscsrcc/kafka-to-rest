package kafkaconnector

import (
	"fmt"
	"kafka-to-rest/pkg/adapters/kafka"
	"kafka-to-rest/pkg/config"
	"kafka-to-rest/pkg/dependencies"
	"log"
	"os"
)

type KafkaConnector struct {
	config        config.KafkaConnectorConfig
	kafkaConsumer kafka.KafkaConsumerInterface
	kafkaProducer kafka.KafkaProducerInterface
	shouldConsume bool
	dataChannel   chan []byte
	commitChannel chan struct{}
}

func NewKafkaConnector(
	cnf config.KafkaConnectorConfig,
	dataChannel chan []byte,
	commitChannel chan struct{},
) *KafkaConnector {
	return &KafkaConnector{
		config:        cnf,
		dataChannel:   dataChannel,
		commitChannel: commitChannel,
		shouldConsume: true,
	}
}

func (c *KafkaConnector) Init(group string) error {
	depInj := dependencies.DI()

	c.kafkaConsumer = depInj.KafkaConsumerFactory.Build()
	if err := c.kafkaConsumer.Init(c.config, group); err != nil {
		return err
	}

	c.kafkaProducer = depInj.KafkaProducerFactory.Build()
	if err := c.kafkaProducer.Init(c.config); err != nil {
		return err
	}

	return nil
}

func (c *KafkaConnector) Consume() {
	for c.shouldConsume {
		if message, err := c.kafkaConsumer.Poll(100); err != nil {
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", err)
		} else if len(message) == 0 {
			continue
		} else {
			c.dataChannel <- message
			<-c.commitChannel
			c.kafkaConsumer.Commit()
		}
	}
}

func (c *KafkaConnector) SendToDLQ(message []byte) error {
	log.Print("Delivering message to DLQ")
	return c.kafkaProducer.Produce(message, []byte{})
}

func (c *KafkaConnector) Close() {
	c.shouldConsume = false
	c.kafkaConsumer.Close()
	c.kafkaProducer.Close()
	log.Printf("Kafka connector was closed")
}
