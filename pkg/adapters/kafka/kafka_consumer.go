package kafka

import (
	"fmt"
	"kafka-to-rest/pkg/config"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaConsumerInterface interface {
	Init(cnf config.KafkaConnectorConfig, group string) error
	Poll(int) ([]byte, error)
	Commit()
	Close() error
}

// ---

type RealKafkaConsumer struct {
	consumer *kafka.Consumer
}

func (c *RealKafkaConsumer) Init(cnf config.KafkaConnectorConfig, group string) error {
	kafkaConsumer, errKafkaConsumerConnection := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  fmt.Sprintf("%s:%d", cnf.Host, cnf.Port),
		"group.id":           group,
		"enable.auto.commit": false,
		"auto.offset.reset":  "latest",
	})
	c.consumer = kafkaConsumer

	if errKafkaConsumerConnection != nil {
		return errKafkaConsumerConnection
	}

	errTopicSubscription := c.consumer.SubscribeTopics(cnf.Topics, nil)
	if errTopicSubscription != nil {
		return errTopicSubscription
	}

	return nil
}

func (c *RealKafkaConsumer) Poll(timeoutMs int) ([]byte, error) {
	ev := c.consumer.Poll(timeoutMs)
	switch e := ev.(type) {
	case *kafka.Message:
		return e.Value, nil
	case kafka.Error:
		return []byte{}, e
	default:
	}

	return []byte{}, nil
}

func (c *RealKafkaConsumer) Commit() {
	c.consumer.Commit()
}

func (c *RealKafkaConsumer) Close() error {
	return c.consumer.Close()
}

// ---

type KafkaConsumerFactoryInterface interface {
	Build() KafkaConsumerInterface
}

type RealKafkaConsumerFactory struct{}

func (f RealKafkaConsumerFactory) Build() KafkaConsumerInterface {
	return &RealKafkaConsumer{}
}
