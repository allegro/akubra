package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// TransportConfigTest for tests defaults
type TransportConfigTest struct {
	Transport
}

var testConfig TransportConfigTest

// NewTransportConfigTest tests func for updating fields values in tests cases
func (t *Transport) NewTransportConfigTest() *Transport {

	t.Triggers = PrepareTransportConfig("^GET|POST$", "/path/aa", "")
	t.MergingStrategy = ""
	return t
}

func TestShoulValidateWithEmptyQueryParam(t *testing.T) {
	transportCfg := testConfig.NewTransportConfigTest()
	err := transportCfg.Validate()
	assert.NoError(t, err, "Should be correct")
}

func TestShouldValidateWithNotEmptyQueryParam(t *testing.T) {
	transportCfg := TransportConfigTest{Transport{
		Triggers: ClientTransportTriggers{
			Method: "^GET$",
			Path: "^/path/abc$",
			QueryParam: "?acl",
		},
	},
	}
	err := transportCfg.Validate()
	assert.NoError(t, err, "Should be correct")
}

func TestShouldNotValidate(t *testing.T) {
	transportCfg := TransportConfigTest{Transport{
		Triggers: ClientTransportTriggers{
			Method: "",
		},
	},
	}
	err := transportCfg.Validate()
	assert.Error(t, err, "Should be incorrect")
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

func TestShouldNotDetailsMatchedWhenMethodHasWrongRegExp(t *testing.T) {
	testConfig := TransportConfigTest{Transport{
		Triggers: ClientTransportTriggers{
			Method: "^GET|POST$",
		},
	},
	}
	matched := testConfig.DetailsMatched("PUT", "", "")
	assert.False(t, matched, "Should be false")
}

func TestShouldDetailsMatchedForPath(t *testing.T) {
	testConfig := TransportConfigTest{Transport{
		Triggers: ClientTransportTriggers{
			Method:     "^GET|POST|PUT|DELETE$",
			Path:       "^.*$",
			QueryParam: "^.*$",
		},
	},
	}
	matched := testConfig.DetailsMatched("GET", "^/bucket/object", "acl")
	assert.True(t, matched, "Should be true")
}

func TestShouldDetailsMatchedForQueryParam(t *testing.T) {
	testConfig := TransportConfigTest{Transport{
		Triggers: ClientTransportTriggers{
			Method:     "^.*$",
			Path:       "^/.*$",
			QueryParam: "^acl$",
		},
	},
	}
	matched := testConfig.DetailsMatched("PUT", "/bucket2", "acl")
	assert.True(t, matched, "Should be true")
}

func PrepareTransportConfig(method, path, queryParam string) ClientTransportTriggers {
	return ClientTransportTriggers{
		Method:     method,
		Path:       path,
		QueryParam: queryParam,
	}
}
