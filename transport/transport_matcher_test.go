package transport

import (
	"net/http"
	"net/url"
	"testing"

	"fmt"

	httphandlerConfig "github.com/allegro/akubra/httphandler/config"
	"github.com/allegro/akubra/log"
	transportConfig "github.com/allegro/akubra/transport/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type LoggerMock struct {
	mock.Mock
	log.Logger
}

func (log *LoggerMock) Print(v ...interface{}) {
	log.Called(v)
}

func TestShouldSelectTransport(t *testing.T) {
	expectedTransportName := "TestTransport2"
	testMethod := "GET"
	logger := log.DefaultLogger
	clientConfig := prepareClientCoinfig(expectedTransportName, testMethod)
	url, _ := url.Parse("http://localhost/")
	testRequest := &http.Request{URL: url, Method: testMethod}
	unit := &Matcher{
		TransportsConfig: clientConfig.Transports,
	}

	selectedTransport, err := unit.SelectTransport(testRequest.Method, testRequest.URL.Path, testRequest.URL.RawQuery, logger)

	assert.Equal(t, expectedTransportName, selectedTransport.Name)
	assert.Nil(t, err)
}

func TestShouldFailSelectTransportWhenNoMatches(t *testing.T) {
	testMethod := "HEAD"
	testPath := "/bucket/"
	expectedErrorMsg := fmt.Sprintf(
		"Transport not matched with args. method: %s, path: %s, queryParams: ", testMethod, testPath)
	clientConfig := prepareClientCoinfig("TestTransport3", "POST")
	unit := &Matcher{
		TransportsConfig: clientConfig.Transports,
	}

	logMock := &LoggerMock{}
	logMock.On("Print", []interface{}{expectedErrorMsg}).Return()

	_, err := unit.SelectTransport(testMethod, testPath, "", logMock)

	assert.Errorf(t, err, expectedErrorMsg)
	logMock.AssertNumberOfCalls(t, "Print", 1)
}

func prepareClientCoinfig(transportName, method string) httphandlerConfig.Client {
	testConfig := transportConfig.Transports{transportConfig.Transport{
		Name: transportName,
		Matchers: transportConfig.ClientTransportMatchers{
			Method: method,
		},
		Properties: transportConfig.ClientTransportProperties{},
	},
	}
	return httphandlerConfig.Client{
		Transports: testConfig,
	}
}
