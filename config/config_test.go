package config

import (
	"testing"

	"net/url"
	"time"

	"github.com/allegro/akubra/metrics"
	"github.com/go-yaml/yaml"
	"github.com/stretchr/testify/assert"
)

type TestYaml struct {
	Field YAMLUrl
}

type TestAdditionalHeadersYaml struct {
	HeaderField AdditionalHeaders
}

func TestYAMLUrlParsingSuccessful(t *testing.T) {
	correct := []byte(`field: http://golang.org:80/pkg/net`)
	testyaml := TestYaml{}
	err := yaml.Unmarshal(correct, &testyaml)
	assert.NoError(t, err, "Should be correct")
}

func TestYAMLUrlParsingFailure(t *testing.T) {
	incorrect := []byte(`field: golang.org:80/pkg/net`)
	testyaml := TestYaml{}
	err := yaml.Unmarshal(incorrect, &testyaml)
	assert.Error(t, err, "Missing protocol should return error")
}

//func TestYAMLUrlParsingEmpty(t *testing.T) {
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
	testListenData := []string{"127.0.0.1:8080", ":8080", ":80"}

	for _, listenValue := range testListenData {
		testConfData := prepareYamlConfig("100MB", 31, 45, "127.0.0.1:81", listenValue, nil)
		result, _ := ValidateConf(testConfData)
		assert.True(t, result, "Should be true")
	}
}
func TestShouldNotValidateListenConf(t *testing.T) {
	testWrongListenData := []string{"", "-", " ", "aaa", ":bbb", "c:"}

	for _, listenWrongValue := range testWrongListenData {
		testConfData := prepareYamlConfig("20MB", 31, 45, "127.0.0.1:82", listenWrongValue, nil)
		result, _ := ValidateConf(testConfData)
		assert.False(t, result, "Should be false")
	}
}

func TestShouldValidateConfWithRegexp(t *testing.T) {
	testConfData := prepareYamlConfig("40MB", 17, 51, "127.0.0.1:83", ":80", nil)

	result, _ := ValidateConf(testConfData)

	assert.True(t, result, "Should be true")
}

func TestShouldNotValidateConfWithWrongBodyMaxSizeValueinRegexp(t *testing.T) {
	testConfData := prepareYamlConfig("0", 0, 1, "127.0.0.1:84", ":80", nil)

	result, validationErrors := ValidateConf(testConfData)

	assert.Contains(t, validationErrors, "BodyMaxSize")
	assert.False(t, result, "Should be false")
}

func TestShouldValidateConfMaintainedBackendWhenNotEmpty(t *testing.T) {
	maintainedBackendHost := "127.0.0.1:85"
	testConfData := prepareYamlConfig("112MB", 21, 32, maintainedBackendHost, ":80", nil)

	result, _ := ValidateConf(testConfData)

	assert.True(t, result, "Should be true")
}

func TestShouldValidateConfMaintainedBackendWhenEmpty(t *testing.T) {
	maintainedBackendHost := ""
	testConfData := prepareYamlConfig("113MB", 22, 33, maintainedBackendHost, ":80", nil)

	result, _ := ValidateConf(testConfData)

	assert.True(t, result, "Should be true")
}

/*
func TestShouldValidateAllPossibleSyncLogMethods(t *testing.T) {
	SyncLogMethodsTestData := []SyncLogMethod{{method: "GET",}, {method: "POST",}}
	testConfData := prepareYamlConfig("40MB", 10, 11, "127.0.0.1:86", ":80", SyncLogMethodsTestData)

	result, _ := ValidateConf(testConfData)

	assert.True(t, result, "Should be true")
}

func TestShouldNotValidateWrongSyncLogMethod(t *testing.T) {
	SyncLogMethodsTestData := []SyncLogMethod{{method: "WRONG",}}
	testConfData := prepareYamlConfig("50MB", 12, 31, "127.0.0.1:86", ":80", SyncLogMethodsTestData)

	result, _ := ValidateConf(testConfData)

	assert.False(t, result, "Should be false")
}
*/

/*
func TestAdditionalHeadersYamlParsingSuccessful(t *testing.T) {
	correct := []byte(`HeaderField:\n  'FieldKey': "FieldValue"`)
	testyaml := TestAdditionalHeadersYaml{}
	err := yaml.Unmarshal(correct, &testyaml)
	assert.NoError(t, err, "Should be correct")
}


func TestAdditionalHeadersYamlParsingFailureWhenKeyIsEmpty(t *testing.T) {
	incorrect := []byte(`HeaderField:\n  '': "FieldValue2"`)
	testyaml := TestYaml{}
	err := yaml.Unmarshal(incorrect, &testyaml)
	assert.Error(t, err, "Missing protocol should return error")
}

func TestAdditionalHeadersYamlParsingFailureWhenValueIsEmpty(t *testing.T) {
	incorrect := []byte(`HeaderField:\n  'FieldKey3': ""`)
	testyaml := TestYaml{}
	err := yaml.Unmarshal(incorrect, &testyaml)
	assert.Error(t, err, "Missing protocol should return error")
}
*/

func prepareYamlConfig(bodyMaxSize string, idleConnTimeoutInp time.Duration, responseHeaderTimeoutInp time.Duration,
	maintainedBackendHost string, listen string, SyncLogMethods []SyncLogMethod) YamlConfig {

	if SyncLogMethods == nil {
		SyncLogMethods = []SyncLogMethod{{method: "POST"}}
	}

	url1 := url.URL{
		Scheme: "http",
		Host:   "127.0.0.1:8080",
	}
	yamlURL := []YAMLUrl{{&url1}}

	maxIdleConns := 1
	maxIdleConnsPerHost := 2
	clusters := map[string]ClusterConfig{"test": {
		yamlURL,
		"replicator",
		1,
		map[string]string{},
	}}

	url2 := url.URL{Scheme: "http", Host: maintainedBackendHost}
	maintainedBackends := []YAMLUrl{{&url2}}

	additionalRequestHeaders := AdditionalHeaders{
		"Cache-Control": "public, s-maxage=600, max-age=600",
	}

	additionalResponseHeaders := AdditionalHeaders{
		"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
	}

	return YamlConfig{
		listen,
		[]YAMLUrl{},
		bodyMaxSize,
		maxIdleConns,
		maxIdleConnsPerHost,
		metrics.Interval{idleConnTimeoutInp},
		metrics.Interval{responseHeaderTimeoutInp},
		clusters,
		additionalRequestHeaders,
		additionalResponseHeaders,
		maintainedBackends,
		SyncLogMethods,
		&ClientConfig{},
		LoggingConfig{},
		metrics.Config{},
		false,
	}
}
