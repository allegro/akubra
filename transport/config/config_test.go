package config

import (
	"fmt"
	"testing"

	"github.com/allegro/akubra/metrics"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

const testDataWithDefaultEmptyRules = `
---
Transports:
  -
    Name: Transport1
    Rules:
      Method: GET|POST
    Properties:
      MaxIdleConns: 200
      MaxIdleConnsPerHost: 1000
      IdleConnTimeout: 2s
      ResponseHeaderTimeout: 5s
  -
    Name: Transport2
    Rules:
      Method: GET|POST|PUT
      QueryParam: acl
    Properties:
      MaxIdleConns: 200
      MaxIdleConnsPerHost: 500
      IdleConnTimeout: 5s
      ResponseHeaderTimeout: 5s
  -
    Name: Transport3
    Rules:
      Path: /bucket.*
      QueryParam: clientId=.*
    Properties:
      MaxIdleConns: 500
      MaxIdleConnsPerHost: 500
      IdleConnTimeout: 2s
      ResponseHeaderTimeout: 2s
  -
    Name: DefaultTransport
    Rules:
    Properties:
      MaxIdleConns: 500
      MaxIdleConnsPerHost: 500
      IdleConnTimeout: 2s
      ResponseHeaderTimeout: 2s

`

// TransportsTestCfg Transports configuration
type TransportsTestCfg struct {
	Transports Transports `yaml:"Transports"`
}

// TransportMatcherDefinitionTest for tests defaults
type TransportMatcherDefinitionTest struct {
	TransportMatcherDefinition
}

// NewTransportConfigTest tests func for updating fields values in tests cases
func (t *TransportMatcherDefinition) NewTransportConfigTest() *TransportMatcherDefinition {
	t.Rules = prepareTransportConfig("^GET|POST$", "/path/aa", "")
	return t
}

func TestShouldCompileRules(t *testing.T) {
	testConfig := TransportMatcherDefinitionTest{}
	err := testConfig.compileRules()
	assert.NoError(t, err, "Should be correct")
}

func TestShouldNotCompileRules(t *testing.T) {
	testConfig := TransportMatcherDefinitionTest{TransportMatcherDefinition{
		Rules: ClientTransportRules{
			Method: "\\p",
		},
	},
	}
	err := testConfig.compileRules()
	assert.Error(t, err, "Should be incorrect")
}

func TestShouldGetMatchedTransport(t *testing.T) {
	testProperties := ClientTransportProperties{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout: metrics.Interval{
			Duration: 1,
		},
		ResponseHeaderTimeout: metrics.Interval{
			Duration: 1,
		},
		DisableKeepAlives: false,
	}
	transportsWithRules := []map[string]TransportMatcherDefinition{
		{
			"Transport1": TransportMatcherDefinition{
				Name: "Transport1",
				Rules: ClientTransportRules{
					Method: "POST",
					Path:   "/aaa/bbb",
				},
				Properties: testProperties,
			},
		},
		{
			"Transport2": TransportMatcherDefinition{
				Name: "Transport2",
				Rules: ClientTransportRules{
					Method:     "PUT",
					QueryParam: "acl",
				},
				Properties: testProperties,
			},
		},
		{
			"Transport3": TransportMatcherDefinition{
				Name: "Transport3",
				Rules: ClientTransportRules{
					Method:     "HEAD",
					Path:       "/bucket102",
					QueryParam: "clientId=123",
				},
				Properties: testProperties,
			},
		},
		{
			"DefaultTransport": TransportMatcherDefinition{
				Name: "DefaultTransport",
				Rules: ClientTransportRules{
					Method:     "",
					Path:       "",
					QueryParam: "",
				},
				Properties: testProperties,
			},
		},
	}
	transports := prepareTransportsTestData(testDataWithDefaultEmptyRules)

	for _, transportMatcherKV := range transportsWithRules {
		transportNameKey, methodPrepared, pathPrepared, queryParamPrepared := extractProperties(transportMatcherKV)
		transport, ok := transports.GetMatchedTransportDefinition(methodPrepared, pathPrepared, queryParamPrepared)
		assert.True(t, ok)
		assert.Equal(t, transportNameKey, transport.Name, "Should be equal")
	}
}

func extractProperties(transportMatcherKV map[string]TransportMatcherDefinition) (transportName string, method string, path string, queryParam string) {
	for _, emulatedTransportProps := range transportMatcherKV {
		transportName = emulatedTransportProps.Name
		method = emulatedTransportProps.Rules.Method
		path = emulatedTransportProps.Rules.Path
		queryParam = emulatedTransportProps.Rules.QueryParam
	}
	return
}

func prepareTransportsTestData(dataYaml string) Transports {
	var ttc TransportsTestCfg
	if err := yaml.Unmarshal([]byte(dataYaml), &ttc); err != nil {
		fmt.Println(err.Error())
	}
	return ttc.Transports
}

func prepareTransportConfig(method, path, queryParam string) ClientTransportRules {
	return ClientTransportRules{
		Method:     method,
		Path:       path,
		QueryParam: queryParam,
	}
}
