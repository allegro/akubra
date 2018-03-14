package transport

import (
	"net/http"
	"net/url"
	"testing"

	httphandlerConfig "github.com/allegro/akubra/httphandler/config"
	"github.com/allegro/akubra/transport/config"
	"github.com/stretchr/testify/assert"
)

func TestShouldSetTransportsConfig(t *testing.T) {
	clientConfig := prepareClientCoinfig("TestTransport", "GET|POST")
	unit := &Matcher{}

	unit.SetTransportsConfig(clientConfig)

	assert.Equal(t, unit.TransportsConfig, clientConfig.Transports)
}

func TestShouldSelectTransportName(t *testing.T) {
	expectedTransportName := "TestTransport2"
	testMethod := "GET"

	configTransport := config.Transport{
		Name: expectedTransportName,
		Matchers: config.ClientTransportMatchers{
			Method: testMethod,
		},
		Details: config.ClientTransportDetail{},
	}
	url, _ := url.Parse("http://localhost/")
	testRequest := &http.Request{URL: url, Method: testMethod}
	unit := &Matcher{
		TransportsConfig: config.Transports{
			configTransport,
		},
	}
	unit.SetTransportsConfig(prepareClientCoinfig(expectedTransportName, testMethod))

	selectedTransportName := unit.SelectTransportName(testRequest.Method, testRequest.URL.Path, testRequest.URL.RawQuery)

	assert.Equal(t, expectedTransportName, selectedTransportName)
}

func prepareClientCoinfig(transportName, method string) httphandlerConfig.Client {
	testConfig := config.Transports{config.Transport{
		Name: transportName,
		Matchers: config.ClientTransportMatchers{
			Method: method,
		},
	},
	}
	return httphandlerConfig.Client{
		Transports: testConfig,
	}
}
