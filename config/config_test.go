package config

import (
	"testing"

	"github.com/go-yaml/yaml"
	"github.com/stretchr/testify/assert"
	//"github.com/allegro/akubra/log"
)

type TestYaml struct {
	Field YAMLURL
}

func TestYAMLURLParsingSuccessful(t *testing.T) {
	correct := []byte(`field: http://golang.org:80/pkg/net`)
	testyaml := TestYaml{}
	err := yaml.Unmarshal(correct, &testyaml)
	assert.NoError(t, err, "Should be correct")
}

func TestYAMLURLParsingFailure(t *testing.T) {
	incorrect := []byte(`field: golang.org:80/pkg/net`)
	testyaml := TestYaml{}
	err := yaml.Unmarshal(incorrect, &testyaml)
	assert.Error(t, err, "Missing protocol should return error")
}

//func TestYAMLURLParsingEmpty(t *testing.T) {
//	incorrect := []byte(`field: "1"`)
//	testyaml := TestYaml{}
//	err := yaml.Unmarshal(incorrect, &testyaml)
//	assert.NoError(t, err, "Should not even try to parse")
//	assert.Nil(t, testyaml.Field.URL, "Should be nil")
//}

func TestListenYamlParameterValidation(t *testing.T) {
	incorrect := []byte(`Listen: ":8080"`)
	testyaml := TestYaml{}
	err := yaml.Unmarshal(incorrect, &testyaml)
	assert.NoError(t, err, "Should not even try to parse")
	assert.Nil(t, testyaml.Field.URL, "Should be nil")
}
/*

func TestShouldValidateListenConf(t *testing.T) {
	testListenData := []string{"aaa", "127.0.0.1", "127.0.0.1:8080", ":8080", ":80"}

	for _, listenValue := range testListenData {
		testConfData := prepareYamlConfig("20", "31", "45", "", listenValue)
		result, _ := ValidateConf(testConfData)
		assert.True(t, result, "Should be true")
	}
}
func TestShouldNotValidateListenConf(t *testing.T) {
	testWrongListenData := []string{"", "-", " ", "aaa", ":1234567", "0:0"}

	for _, listenWrongValue := range testWrongListenData {
		testConfData := prepareYamlConfig("20", "31", "45", "", listenWrongValue)
		result, _ := ValidateConf(testConfData)
		assert.False(t, result, "Should be false")
	}
}

func TestShouldValidateConfWithRegexp(t *testing.T) {
	testConfData := prepareYamlConfig("1", "10", "91", "", "")

	result, _ := ValidateConf(testConfData)

	assert.True(t, result, "Should be true")
}

func TestShouldNotValidateConfWithWrongValuesinRegexp(t *testing.T) {
	testConfData := prepareYamlConfig("0", "aa", "", "", "")

	result, validationErrors := ValidateConf(testConfData)

	assert.Contains(t, validationErrors, "ConnLimit")
	assert.Contains(t, validationErrors, "ConnectionTimeout")
	assert.Contains(t, validationErrors, "ConnectionDialTimeout")
	assert.False(t, result, "Should be false")
}

func TestShouldValidateConfMaintainedBackendWhenNotEmpty(t *testing.T) {
	maintainedBackend := "127.0.0.1:8044ąąą"
	testConfData := prepareYamlConfig("11", "22", "33", maintainedBackend, "")

	result, _ := ValidateConf(testConfData)

	assert.True(t, result, "Should be true")
}

func prepareYamlConfig(bodyMaxSize string, connectionTimeout string, connectionDialTimeout string,
	maintainedBackend string, listen string) (YamlConfig) {

	backends := []YAMLURL{}

	additionalRequestHeaders := map[string]string{
		"Cache-Control": "public, s-maxage=600, max-age=600",
	}

	additionalResponseHeaders := map[string]string{
		"Access-Control-Allow-Methods" : "GET, POST, OPTIONS",
	}

	syncLogMethods := make([]SYNCLOGMETHOD, 1) {
		"POST",
	}

	maxIdleConns := 1
	maxIdleConnsPerHost := 2
	idleConnTimeout := "1s"
	responseHeaderTimeout := "2s"
	clusters := map[string]ClusterConfig{"test": {
		{"127.0.0.1:8080"},
		"replicator",
		1,
		map[string]string{},
	}}
	maintainedBackends := map[]{YAMLURL{"http://127.0.0.2:8081",}}
	client := ClientConfig{"a", []string{"test"},}
	tmpLogger := log.LoggerConfig{false,false,false,"/tmp/1","/tmp/2","/tmp/3",}
	logging := LoggingConfig{tmpLogger, tmpLogger, tmpLogger, tmpLogger,}
	metrics := nil
	disableKeepAlive := false
	return YamlConfig{
		listen,
		backends,
		bodyMaxSize,
		maxIdleConns,
		maxIdleConnsPerHost,
		idleConnTimeout,
		responseHeaderTimeout,
		clusters,
		additionalRequestHeaders,
		additionalResponseHeaders,
		maintainedBackends,
		syncLogMethods,
		&client,
		logging,
		metrics,
		disableKeepAlive,
	}
}
*/
