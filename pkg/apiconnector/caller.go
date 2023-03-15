package apiconnector

import (
	"fmt"
	"kafka-to-rest/pkg/adapters/restapi"
	"kafka-to-rest/pkg/config"
	"kafka-to-rest/pkg/dependencies"
	"log"
	"net/http"
	"os"
	"time"
)

type APIResponse struct {
	Error error
	Code  int
}

type APIConnector struct {
	config             config.APIConnectorConfig
	caller             restapi.RestAPICallerInterface
	dataChannel        chan []byte
	apiResponseChannel chan APIResponse
	authToken          string
	doneChannel        chan struct{}
}

func (p *APIConnector) setToken() {
	if p.config.Auth.StaticToken != "" {
		p.authToken = p.config.Auth.StaticToken
		return
	}
	if p.config.Auth.StoredTokenFilename != "" {
		tokenBytes, err := os.ReadFile(p.config.Auth.StoredTokenFilename)
		if err != nil {
			log.Printf("[ERROR] %s", err)
		}
		p.authToken = string(tokenBytes)
	}
}

func NewApiConnector(cnf config.APIConnectorConfig, dataChannel chan []byte, apiResponseChannel chan APIResponse) *APIConnector {
	url := fmt.Sprintf("%s://%s:%d/%s", cnf.Protocol, cnf.Host, cnf.Port, cnf.Path)

	caller := dependencies.DI().RestAPICallerFactory.Build()
	caller.Init(url)

	connector := &APIConnector{
		config:             cnf,
		doneChannel:        make(chan struct{}),
		dataChannel:        dataChannel,
		apiResponseChannel: apiResponseChannel,
		caller:             caller,
	}
	connector.setToken()
	return connector
}

func (p *APIConnector) ForwardMessage() {
	for true {
		select {
		case message := <-p.dataChannel:
			log.Printf("Forwarding message %s", message)
			p.callAPI(message)
		case <-p.doneChannel:
			log.Printf("API Caller closed")
			return
		}
	}
}

func (p *APIConnector) callAPI(message []byte) {
	// Try twice
	// if the first time it fails due to an invalid token, try to refresh the token
	for range []int{1, 2} {
		statusCode, err := p.caller.Call(message, p.authToken)

		// If the call was unautorized, try again once after setting again the token
		// (the token may have changed in the meantime)
		if statusCode == http.StatusUnauthorized {
			p.setToken()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		p.apiResponseChannel <- APIResponse{
			Code:  statusCode,
			Error: err,
		}
		break
	}
}

func (p *APIConnector) Close() {
	p.doneChannel <- struct{}{}
}
