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
		testConfData := prepareYamlConfig("100MB", 31, 45, "127.0.0.1:81", listenValue, nil, "client1", []string{"dev"})
		result, _ := ValidateConf(testConfData)
		assert.True(t, result, "Should be true")
	}
}
func TestShouldNotValidateListenConf(t *testing.T) {
	testWrongListenData := []string{"", "-", " ", "aaa", ":bbb", "c:"}

	for _, listenWrongValue := range testWrongListenData {
		testConfData := prepareYamlConfig("20MB", 31, 45, "127.0.0.1:82", listenWrongValue, nil, "client1", []string{"dev"})
		result, _ := ValidateConf(testConfData)
		assert.False(t, result, "Should be false")
	}
}

func TestShouldValidateConfWithRegexp(t *testing.T) {
	testConfData := prepareYamlConfig("40MB", 17, 51, "127.0.0.1:83", ":80", nil, "client1", []string{"dev"})

	result, _ := ValidateConf(testConfData)

	assert.True(t, result, "Should be true")
}

func TestShouldNotValidateConfWithWrongBodyMaxSizeValueinRegexp(t *testing.T) {
	testConfData := prepareYamlConfig("0", 0, 1, "127.0.0.1:84", ":80", nil, "client1", []string{"dev"})

	result, validationErrors := ValidateConf(testConfData)

	assert.Contains(t, validationErrors, "BodyMaxSize")
	assert.False(t, result, "Should be false")
}

func TestShouldValidateConfMaintainedBackendWhenNotEmpty(t *testing.T) {
	maintainedBackendHost := "127.0.0.1:85"
	testConfData := prepareYamlConfig("112MB", 21, 32, maintainedBackendHost, ":80", nil, "client1", []string{"dev"})

	result, _ := ValidateConf(testConfData)

	assert.True(t, result, "Should be true")
}

func TestShouldValidateConfMaintainedBackendWhenEmpty(t *testing.T) {
	maintainedBackendHost := ""
	testConfData := prepareYamlConfig("113MB", 22, 33, maintainedBackendHost, ":80", nil, "client1", []string{"dev"})

	result, _ := ValidateConf(testConfData)

	assert.True(t, result, "Should be true")
}

func TestShouldValidateConfClientNameWithMinLenght(t *testing.T) {
	clientName := "c"
	testConfData := prepareYamlConfig("114MB", 22, 33, "", ":80", nil, clientName, []string{"dev"})

	result, _ := ValidateConf(testConfData)

	assert.True(t, result, "Should be true")
}

func TestShouldNotValidateConfClientNameWhenEmpty(t *testing.T) {
	clientName := ""
	testConfData := prepareYamlConfig("115MB", 22, 33, "", ":80", nil, clientName, []string{"dev"})

	result, _ := ValidateConf(testConfData)

	assert.False(t, result, "Should be false")
}

func TestShouldValidateConfClientClustersValues(t *testing.T) {
	testConfData := prepareYamlConfig("115MB", 22, 33, "", ":80", nil, "client", []string{"prod", "jprod"})

	result, _ := ValidateConf(testConfData)

	assert.True(t, result, "Should be true")
}

func TestShouldNotValidateConfClientClustersValuesWhenEmpty(t *testing.T) {
	testConfData := prepareYamlConfig("116MB", 22, 33, "", ":80", nil, "client", []string{"prod", "  "})

	result, validationErrors := ValidateConf(testConfData)

	assert.Contains(t, validationErrors, "Client.Clusters")
	assert.False(t, result, "Should be false")
}

func TestShouldNotValidateConfClientClustersValuesWhenDuplicated(t *testing.T) {
	testConfData := prepareYamlConfig("117MB", 22, 33, "", ":80", nil, "client", []string{"jprod", "jprod"})

	result, validationErrors := ValidateConf(testConfData)

	assert.Contains(t, validationErrors, "Client.Clusters")
	assert.False(t, result, "Should be false")
}

func TestShouldValidateAllPossibleSyncLogMethods(t *testing.T) {
	data := `
- GET
- POST
- PUT
- DELETE
- HEAD
- OPTIONS
`
	syncLogMethodsTestData := []SyncLogMethod{}
	errors := yaml.Unmarshal([]byte(data), &syncLogMethodsTestData)

	assert.Nil(t, errors)
}

func TestShouldNotValidateWrongSyncLogMethod(t *testing.T) {
	data := `
- WRONG
`
	syncLogMethodsTestData := []SyncLogMethod{}
	errors := yaml.Unmarshal([]byte(data), &syncLogMethodsTestData)

	assert.NotNil(t, errors)
}

/*
func TestAdditionalHeadersYamlParsingSuccessful(t *testing.T) {
	correct := []byte(`HeaderField:\n  'FieldKey': "FieldValue"`)
	testyaml := TestAdditionalHeadersYaml{}
	err := yaml.Unmarshal(correct, &testyaml)
	assert.NoError(t, err, "Should be correct")
}

func TestAdditionalHeadersYamlParsingFailureWhenKeyIsEmpty(t *testing.T) {
	incorrect := []byte(`HeaderField:\n  '': "FieldValue2"`)
	testyaml := TestAdditionalHeadersYaml{}
	err := yaml.Unmarshal(incorrect, &testyaml)
	assert.Error(t, err, "Missing protocol should return error")
}

func TestAdditionalHeadersYamlParsingFailureWhenValueIsEmpty(t *testing.T) {
	incorrect := []byte(`HeaderField:\n  'FieldKey3': ""`)
	testyaml := TestAdditionalHeadersYaml{}
	err := yaml.Unmarshal(incorrect, &testyaml)
	assert.Error(t, err, "Missing protocol should return error")
}
*/

func prepareYamlConfig(bodyMaxSize string, idleConnTimeoutInp time.Duration, responseHeaderTimeoutInp time.Duration,
	maintainedBackendHost string, listen string, syncLogMethods []SyncLogMethod, clientCfgName string,
	clientClusters []string) YamlConfig {

	if syncLogMethods == nil {
		syncLogMethods = []SyncLogMethod{{method: "POST"}}
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

	clientCfg := &ClientConfig{
		Name:     clientCfgName,
		Clusters: clientClusters,
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
		syncLogMethods,
		clientCfg,
		LoggingConfig{},
		metrics.Config{},
		false,
	}
}
