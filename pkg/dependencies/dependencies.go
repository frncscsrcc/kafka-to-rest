package dependencies

import (
	"kafka-to-rest/pkg/adapters/kafka"
)

type Dependencies struct {
	KafkaConsumerFactory kafka.KafkaConsumerFactoryInterface
	KafkaProducerFactory kafka.KafkaProducerFactoryInterface
}

var dependencies Dependencies
var originalDependencies Dependencies

func init() {
	dependencies = Dependencies{
		KafkaConsumerFactory: kafka.RealKafkaConsumerFactory{},
		KafkaProducerFactory: kafka.RealKafkaProducerFactory{},
	}
	originalDependencies = dependencies
}

func DI() Dependencies {
	return dependencies
}

func Overwrite(newDependecies Dependencies) Dependencies {
	dependencies = newDependecies
	return newDependecies
}

func Reset() {
	dependencies = originalDependencies
}
