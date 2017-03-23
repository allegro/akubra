package config

import (
	"testing"

	"github.com/go-yaml/yaml"
	"github.com/stretchr/testify/assert"
	"net/url"
	"github.com/allegro/akubra/metrics"
	"time"
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

func TestShouldValidateListenConf(t *testing.T) {
	testListenData := []string{":aaa", "127.0.0.1", "127.0.0.1:8080", ":8080", ":80"}

	for _, listenValue := range testListenData {
		testConfData := prepareYamlConfig("20Mb", 31, 45, "127.0.0.1:81", listenValue)
		result, _ := ValidateConf(testConfData)
		assert.True(t, result, "Should be true")
	}
}
func TestShouldNotValidateListenConf(t *testing.T) {
	testWrongListenData := []string{"", "-", " ", "aaa", ":1234567", "0:0", ":80aa"}

	for _, listenWrongValue := range testWrongListenData {
		testConfData := prepareYamlConfig("20", 31, 45, "127.0.0.1:82", listenWrongValue)
		result, _ := ValidateConf(testConfData)
		assert.False(t, result, "Should be false")
	}
}

func TestShouldValidateConfWithRegexp(t *testing.T) {
	testConfData := prepareYamlConfig("300kb", 10, 91, "127.0.0.1:83", "")

	result, _ := ValidateConf(testConfData)

	assert.True(t, result, "Should be true")
}

func TestShouldNotValidateConfWithWrongBodyMaxSizeValueinRegexp(t *testing.T) {
	testConfData := prepareYamlConfig("0", 0, 1, "127.0.0.1:84", "")

	result, validationErrors := ValidateConf(testConfData)

	assert.Contains(t, validationErrors, "BodyMaxSize")
	assert.False(t, result, "Should be false")
}

//func TestShouldValidateConfMaintainedBackendWhenNotEmpty(t *testing.T) {
//	maintainedBackendHost := "127.0.0.1:85"
//	testConfData := prepareYamlConfig("10Mb", 22, 33, maintainedBackendHost, "")
//
//	result, _ := ValidateConf(testConfData)
//
//	assert.True(t, result, "Should be true")
//}

func prepareYamlConfig(bodyMaxSize string, idleConnTimeoutInp time.Duration, responseHeaderTimeoutInp time.Duration,
	maintainedBackendHost string, listen string) (YamlConfig) {

	backends := []YAMLURL{}

	additionalRequestHeaders := map[string]string{
		"Cache-Control": "public, s-maxage=600, max-age=600",
	}

	additionalResponseHeaders := map[string]string{
		"Access-Control-Allow-Methods" : "GET, POST, OPTIONS",
	}

	syncLogMethods := []SYNCLOGMETHOD{{"POST"}}

	url1 := url.URL {
		Scheme: "http",
		Host: "127.0.0.1:8080",
	}
	yamlUrl := []YAMLURL{{&url1}}

	maxIdleConns := 1
	maxIdleConnsPerHost := 2
	idleConnTimeout := metrics.Interval{idleConnTimeoutInp,}
	responseHeaderTimeout := metrics.Interval{responseHeaderTimeoutInp,}
	clusters := map[string]ClusterConfig{"test": {
		yamlUrl,
		"replicator",
		1,
		map[string]string{},
	}}

	url2 := url.URL{ Scheme: "http", Host: maintainedBackendHost, }
	maintainedBackends := []YAMLURL{{&url2, }}
	metrics := metrics.Config{}
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
		&ClientConfig{},
		LoggingConfig{},
		metrics,
		disableKeepAlive,
	}
}
