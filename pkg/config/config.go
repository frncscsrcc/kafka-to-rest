package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type ConsumerConfig struct {
	Host   string   `json:"host"`
	Port   int      `json:"port"`
	Topics []string `json:"topics"`
	DLQ    string   `json:"dlq"`
}

type AuthConfig struct {
	StaticToken         string `json:"static-token"`
	StoredTokenFilename string `json:"stored-token-filename"`
}

type APICallerConfig struct {
	Protocol   string     `json:"protocol"`
	Host       string     `json:"host"`
	Port       int        `json:"port"`
	Path       string     `json:"path"`
	RetryDelay int        `json:"retry-delay"`
	MaxRetries int        `json:"retry-max"`
	Auth       AuthConfig `json:"auth"`
}

type ProxyConfig struct {
	Name               string          `json:"name"`
	Consumer           ConsumerConfig  `json:"consumer"`
	APICaller          APICallerConfig `json:"api-caller"`
	RetryOnStatusCode  []string        `json:"retry-on-status"`
	CommitOnStatusCode []string        `json:"commit-on-status"`
}

type Config struct {
	Group   string        `json:"group"`
	Proxies []ProxyConfig `json:"proxies"`
}

func NewConfigFromFile(file string) (Config, error) {
	var cnf Config

	jsonFile, errOpeningFile := os.Open(file)
	if errOpeningFile != nil {
		return Config{}, errOpeningFile
	}

	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)
	json.Unmarshal(byteValue, &cnf)

	fmt.Printf("%+v\n", cnf)

	return cnf, nil
}
