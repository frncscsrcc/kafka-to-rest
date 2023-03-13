package proxy

import (
	"fmt"
	"kafka-to-rest/pkg/apiconnector"
	"kafka-to-rest/pkg/config"
	"kafka-to-rest/pkg/kafkaconnector"
	"log"
	"sync"
	"time"
)

type Proxy struct {
	name      string
	config    config.ProxyConfig
	consumer  *kafkaconnector.KafkaConnecotr
	apiCaller *apiconnector.APIConnector

	retryStatusList  map[int]bool
	commitStatusList map[int]bool

	dataChannelFromConsumer chan []byte
	dataChannelToApiCaller  chan []byte
	apiResponseChannel      chan apiconnector.APIResponse
	commitChannel           chan struct{}

	wg *sync.WaitGroup
}

func NewProxy() *Proxy {
	return &Proxy{}
}

func NewProxyFromConfig(group string, cnf config.ProxyConfig, wg *sync.WaitGroup) (*Proxy, error) {
	dataChannelFromConsumer := make(chan []byte)
	dataChannelToApiCaller := make(chan []byte)
	apiResponseChannel := make(chan apiconnector.APIResponse)
	commitChannel := make(chan struct{})

	p := &Proxy{
		wg:                      wg,
		name:                    cnf.Name,
		config:                  cnf,
		retryStatusList:         getStatusList(cnf.RetryOnStatusCode),
		commitStatusList:        getStatusList(cnf.CommitOnStatusCode),
		dataChannelFromConsumer: dataChannelFromConsumer,
		dataChannelToApiCaller:  dataChannelToApiCaller,
		apiResponseChannel:      apiResponseChannel,
		commitChannel:           commitChannel,
		consumer: kafkaconnector.NewKafkaConnector(
			cnf.Consumer,
			dataChannelFromConsumer,
			commitChannel,
		),
		apiCaller: apiconnector.NewApiConnector(
			cnf.APICaller,
			dataChannelToApiCaller,
			apiResponseChannel,
		),
	}

	// init consumer
	err := p.consumer.Init(group)
	return p, err
}

func getStatusList(statusList []string) map[int]bool {
	outputMap := make(map[int]bool)

	inputMap := make(map[string]bool)
	for _, status := range statusList {
		inputMap[status] = true
	}

	// 2**
	for i := 1; i <= 9; i += 1 {
		if _, defined := inputMap[fmt.Sprintf("%d**", i)]; defined {
			for s := 0; s <= 99; s += 1 {
				outputMap[i*100+s] = true
			}
		}
	}

	// 21*
	for i := 10; i <= 99; i += 1 {
		if _, defined := inputMap[fmt.Sprintf("%d*", i)]; defined {
			for s := 0; s <= 9; s += 1 {
				outputMap[i*10+s] = true
			}
		}
	}

	// 223
	for i := 100; i <= 999; i += 1 {
		if _, defined := inputMap[fmt.Sprintf("%d", i)]; defined {
			outputMap[i] = true
		}
	}

	return outputMap
}

func (p *Proxy) GetName() string {
	return p.name
}

func (p *Proxy) GetTopics() string {
	topics := ""
	for _, topic := range p.config.Consumer.Topics {
		topics += topic + " "
	}
	return topics
}

func (p *Proxy) RunSync() {
	go p.forwardToAPI()
	go p.consumer.Consume()
	go p.apiCaller.ForwardMessage()
}

func (p *Proxy) shouldRetry(apiResponse apiconnector.APIResponse, retryCount int) bool {
	if apiResponse.Error != nil {
		return true
	}
	if _, retry := p.retryStatusList[apiResponse.Code]; !retry {
		return false
	}

	maxRetries := p.config.APICaller.MaxRetries
	if maxRetries == 0 {
		return true
	}

	return retryCount < maxRetries

}

func (p *Proxy) shouldCommit(apiResponse apiconnector.APIResponse) bool {
	if _, commit := p.commitStatusList[apiResponse.Code]; commit {
		return true
	}
	return false
}

func (p *Proxy) forwardToAPI() {
	delay := time.Duration(p.config.APICaller.RetryDelay) * time.Second
	if delay == 0 {
		delay = 5 * time.Second
	}

	for true {
		message := <-p.dataChannelFromConsumer

		retryCount := 0
		for true {
			retryCount++

			p.dataChannelToApiCaller <- message
			apiResponse := <-p.apiResponseChannel

			log.Printf("API returned status %d", apiResponse.Code)

			if p.shouldRetry(apiResponse, retryCount) {
				time.Sleep(delay)
				continue
			} else if p.shouldCommit(apiResponse) {
				p.commitChannel <- struct{}{}
				break
			} else if p.config.Consumer.DLQ != "" {
				if dlqError := p.consumer.SendToDLQ(message); dlqError != nil {
					time.Sleep(delay)
					continue
				}
				p.commitChannel <- struct{}{}
				break
			}
		}
	}
}

func (p *Proxy) Close() {
	p.consumer.Close()
	p.apiCaller.Close()
	defer p.wg.Done()
}
