package config

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

const testDataWithDefaultEmptyTriggers = `
---
Transports:
  Transport1:
    Triggers:
      Method: GET|POST
      Path: .*
    MergingStrategy: Default
    Details:
      MaxIdleConns: 200
      MaxIdleConnsPerHost: 1000
      IdleConnTimeout: 2s
      ResponseHeaderTimeout: 5s
  Transport2:
    Triggers:
      Method: GET|POST|PUT
      QueryParam: acl
    MergingStrategy: ListV1
    Details:
      MaxIdleConns: 200
      MaxIdleConnsPerHost: 500
      IdleConnTimeout: 5s
      ResponseHeaderTimeout: 5s
  DefaultTransport:
    Triggers:
    MergingStrategy: Default
    Details:
      MaxIdleConns: 500
      MaxIdleConnsPerHost: 500
      IdleConnTimeout: 2s
      ResponseHeaderTimeout: 2s

`

// TransportsTestCfg Transports configuration
type TransportsTestCfg struct {
	Transports Transports `yaml:"Transports"`
}

// TransportConfigTest for tests defaults
type TransportConfigTest struct {
	Transport
}

// testConfig temporary test properties
var testConfig TransportConfigTest

// NewTransportConfigTest tests func for updating fields values in tests cases
func (t *Transport) NewTransportConfigTest() *Transport {
	t.Triggers = prepareTransportConfig("^GET|POST$", "/path/aa", "")
	t.MergingStrategy = ""
	return t
}

func TestShouldCompileRules(t *testing.T) {
	testConfig := TransportConfigTest{}
	err := testConfig.compileRules()
	assert.NoError(t, err, "Should be correct")
}

func TestShouldNotCompileRules(t *testing.T) {
	testConfig := TransportConfigTest{Transport{
		Triggers: ClientTransportTriggers{
			Method: "\\p",
		},
	},
	}
	err := testConfig.compileRules()
	assert.Error(t, err, "Should be incorrect")
}

func TestShouldGetMatchedTransport(t *testing.T) {
	transportsWithTriggers := []map[string]Transport{
		{
			"Transport1": Transport{
				Triggers: ClientTransportTriggers{
					Method: "POST",
					Path:   "/aaa/bbb",
				},
			},
		},
		{
			"Transport2": Transport{
				Triggers: ClientTransportTriggers{
					Method:     "PUT",
					QueryParam: "acl",
				},
			},
		},
		{
			"DefaultTransport": Transport{
				Triggers: ClientTransportTriggers{
					Method:     "PUT",
					QueryParam: "clientId=123",
				},
			},
		},
		{
			"DefaultTransport": Transport{
				Triggers: ClientTransportTriggers{
					Method:     "",
					Path:       "",
					QueryParam: "",
				},
			},
		},
	}
	transports := prepareTransportsTestData(testDataWithDefaultEmptyTriggers)

	for _, transportTriggerKV := range transportsWithTriggers {
		transportNameKey, methodPrepared, pathPrepared, queryParamPrepared := extractProperties(transportTriggerKV)
		_, transportName, ok := transports.GetMatchedTransport(methodPrepared, pathPrepared, queryParamPrepared)
		assert.True(t, ok)
		assert.Equal(t, transportNameKey, transportName, "Should be equal")
	}
}

func extractProperties(transportTriggerKV map[string]Transport) (transportName string, method string, path string, queryParam string) {
	for transportNameKey, emulatedTransportProps := range transportTriggerKV {
		transportName = transportNameKey
		method = emulatedTransportProps.Triggers.Method
		path = emulatedTransportProps.Triggers.Path
		queryParam = emulatedTransportProps.Triggers.QueryParam
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

func prepareTransportConfig(method, path, queryParam string) ClientTransportTriggers {
	return ClientTransportTriggers{
		Method:     method,
		Path:       path,
		QueryParam: queryParam,
	}
}
