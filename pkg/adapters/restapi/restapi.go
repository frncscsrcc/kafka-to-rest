package restapi

import (
	"bytes"
	"log"
	"net/http"
)

type RestAPICallerInterface interface {
	Init(url string)
	Call(data []byte, token string) (int, error)
}

// --

type RealRestAPICaller struct {
	url string
}

func (c *RealRestAPICaller) Init(url string) {
	c.url = url
}

func (c *RealRestAPICaller) Call(data []byte, token string) (int, error) {
	req, errPreparingRequest := http.NewRequest(
		"POST",
		c.url,
		bytes.NewReader(data),
	)

	if token != "" {
		req.Header.Add("Authorization", "Bearer "+token)
	}

	if errPreparingRequest != nil {
		log.Printf("[ERROR] %s", errPreparingRequest)
		return 0, errPreparingRequest
	}

	client := &http.Client{}
	res, ererrPerformingRequest := client.Do(req)
	defer res.Body.Close()

	if ererrPerformingRequest != nil {
		return 0, ererrPerformingRequest
	}

	return res.StatusCode, nil
}

// --

type RestAPICallerFactoryInterface interface {
	Build() RestAPICallerInterface
}

type RealRestAPICallerFactory struct{}

func (f RealRestAPICallerFactory) Build() RestAPICallerInterface {
	return &RealRestAPICaller{}
}
