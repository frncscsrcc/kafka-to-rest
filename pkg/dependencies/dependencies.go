package dependencies

import (
	"kafka-to-rest/pkg/adapters/kafka"
	"kafka-to-rest/pkg/adapters/restapi"
)

type Dependencies struct {
	KafkaConsumerFactory kafka.KafkaConsumerFactoryInterface
	KafkaProducerFactory kafka.KafkaProducerFactoryInterface
	RestAPICallerFactory restapi.RestAPICallerFactoryInterface
}

var dependencies Dependencies
var originalDependencies Dependencies

func init() {
	dependencies = Dependencies{
		KafkaConsumerFactory: kafka.RealKafkaConsumerFactory{},
		KafkaProducerFactory: kafka.RealKafkaProducerFactory{},
		RestAPICallerFactory: restapi.RealRestAPICallerFactory{},
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
