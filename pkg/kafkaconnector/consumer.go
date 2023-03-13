package kafkaconnector

import (
	"fmt"
	"kafka-to-rest/pkg/config"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaConnecotr struct {
	config        config.KafkaConnectorConfig
	kafkaConsumer *kafka.Consumer
	kafkaProducer *kafka.Producer
	shouldConsume bool
	dataChannel   chan []byte
	commitChannel chan struct{}
}

func NewKafkaConnector(
	cnf config.KafkaConnectorConfig,
	dataChannel chan []byte,
	commitChannel chan struct{},
) *KafkaConnecotr {
	return &KafkaConnecotr{
		config:        cnf,
		dataChannel:   dataChannel,
		commitChannel: commitChannel,
		shouldConsume: true,
	}
}

func (c *KafkaConnecotr) Init(group string) error {
	kafkaConsumer, errKafkaConsumerConnection := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  fmt.Sprintf("%s:%d", c.config.Host, c.config.Port),
		"group.id":           group,
		"enable.auto.commit": false,
		"auto.offset.reset":  "latest",
	})
	c.kafkaConsumer = kafkaConsumer

	if errKafkaConsumerConnection != nil {
		return errKafkaConsumerConnection
	}

	errTopicSubscription := c.kafkaConsumer.SubscribeTopics(c.config.Topics, nil)
	if errTopicSubscription != nil {
		return errTopicSubscription
	}

	kafkaProducer, errKafkaProducerConnection := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": fmt.Sprintf("%s:%d", c.config.Host, c.config.Port),
		"acks":              "all",
	})
	c.kafkaProducer = kafkaProducer

	if errKafkaProducerConnection != nil {
		return errKafkaProducerConnection
	}
	return nil
}

func (c *KafkaConnecotr) Consume() {
	for c.shouldConsume {
		ev := c.kafkaConsumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			c.dataChannel <- e.Value
			<-c.commitChannel
			c.kafkaConsumer.Commit()
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
		default:
		}
	}
}

func (c *KafkaConnecotr) SendToDLQ(message []byte) error {
	log.Print("Delivering message to DLQ")
	delivery_chan := make(chan kafka.Event, 10000)
	defer close(delivery_chan)

	err := c.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &c.config.DLQ, Partition: kafka.PartitionAny},
		Value:          message},
		delivery_chan,
	)
	if err != nil {
		return err
	}

	e := <-delivery_chan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("Delivery to DLQ %s failed: %v\n", c.config.DLQ, m.TopicPartition.Error)
		return m.TopicPartition.Error
	}

	return nil
}

func (c *KafkaConnecotr) Close() {
	c.shouldConsume = false
	c.kafkaConsumer.Close()
	c.kafkaProducer.Close()
	log.Printf("Kafka connector was closed")
}
