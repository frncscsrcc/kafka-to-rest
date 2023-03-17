package restapi

type FakeResponses struct {
	Code  int
	Error error
}

type FakeRealRestAPICaller struct {
	url       string
	Responses chan FakeResponses
}

func (c *FakeRealRestAPICaller) Init(url string) {
	c.url = url
}

func (c *FakeRealRestAPICaller) Call(data []byte, token string) (int, error) {
	response := <-c.Responses
	return response.Code, response.Error
}

// --

type FakeRestAPICallerFactory struct {
	Responses chan FakeResponses
}

func (f FakeRestAPICallerFactory) Build() RestAPICallerInterface {
	return &FakeRealRestAPICaller{
		Responses: f.Responses,
	}
}
