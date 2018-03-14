package transport

import (
	"net/http"
	"net/url"
	"testing"

	httphandlerConfig "github.com/allegro/akubra/httphandler/config"
	"github.com/allegro/akubra/transport/config"
	"github.com/stretchr/testify/assert"
)

func TestShouldSelectTransport(t *testing.T) {
	expectedTransportName := "TestTransport2"
	testMethod := "GET"

	clientConfig := prepareClientCoinfig(expectedTransportName, testMethod)
	url, _ := url.Parse("http://localhost/")
	testRequest := &http.Request{URL: url, Method: testMethod}
	unit := &Matcher{
		TransportsConfig: clientConfig.Transports,
	}

	selectedTransport := unit.SelectTransport(testRequest.Method, testRequest.URL.Path, testRequest.URL.RawQuery)

	assert.Equal(t, expectedTransportName, selectedTransport.Name)
}

func prepareClientCoinfig(transportName, method string) httphandlerConfig.Client {
	testConfig := config.Transports{config.Transport{
		Name: transportName,
		Matchers: config.ClientTransportMatchers{
			Method: method,
		},
		Details: config.ClientTransportDetail{},
	},
	}
	return httphandlerConfig.Client{
		Transports: testConfig,
	}
}
