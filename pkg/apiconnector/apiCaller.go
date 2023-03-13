package apiconnector

import (
	"bytes"
	"fmt"
	"kafka-to-rest/pkg/config"
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
	caller := &APIConnector{
		config:             cnf,
		doneChannel:        make(chan struct{}),
		dataChannel:        dataChannel,
		apiResponseChannel: apiResponseChannel,
	}
	caller.setToken()
	return caller
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
	url := fmt.Sprintf("%s://%s:%d/%s", p.config.Protocol, p.config.Host, p.config.Port, p.config.Path)

	// Try twice
	for range []int{1, 2} {
		req, errPreparingRequest := http.NewRequest(
			"POST",
			url,
			bytes.NewReader(message),
		)

		if p.authToken != "" {
			req.Header.Add("Authorization", "Bearer "+p.authToken)
		}

		if errPreparingRequest != nil {
			log.Printf("[ERROR] %s", errPreparingRequest)
			p.apiResponseChannel <- APIResponse{Code: 0, Error: errPreparingRequest}
			return
		}

		client := &http.Client{}
		res, ererrPerformingRequest := client.Do(req)
		if ererrPerformingRequest != nil {
			p.apiResponseChannel <- APIResponse{Code: 0, Error: ererrPerformingRequest}
			return
		}

		defer res.Body.Close()

		// If the call was unautorized, try again once after setting again the token
		// (the token may have changed in the meantime)
		if res.StatusCode == http.StatusUnauthorized {
			p.setToken()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		p.apiResponseChannel <- APIResponse{Code: res.StatusCode, Error: nil}
		break
	}
}

func (p *APIConnector) Close() {
	p.doneChannel <- struct{}{}
}
