package config

import (
	"net/url"
	"testing"
	"time"

	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"

	"strings"

	logconfig "github.com/allegro/akubra/log/config"
	"github.com/allegro/akubra/metrics"
	shardingconfig "github.com/allegro/akubra/sharding/config"
	"github.com/stretchr/testify/assert"
	yaml "gopkg.in/yaml.v2"
)

const (
	yamlValidateEndpointURL         string = "http://127.0.0.1:8071/configuration/validate"
	yamlConfigWithoutRegionsSection string = `Listen: :80
TechnicalEndpointListen: :81
HealthCheckEndpoint: /status/ping
BodyMaxSize: 2048
MaxIdleConns: 1
MaxIdleConnsPerHost: 2
IdleConnTimeout: 3s
ResponseHeaderTimeout: 4s
MaxConcurrentRequests: 200
Clusters:
  cluster1test:
    Backends:
      - "http://127.0.0.1:8080"
AdditionalRequestHeaders:
  Cache-Control: public, s-maxage=600, max-age=600
AdditionalResponseHeaders:
  Access-Control-Allow-Methods: GET, POST, OPTIONS
SyncLogMethods:
  - POST
DisableKeepAlives: false
`
)

type TestYaml struct {
	Field shardingconfig.YAMLUrl
}

// YamlConfigTest for tests defaults
type YamlConfigTest struct {
	YamlConfig
}

// NewYamlConfigTest tests func for updating fields values in tests cases
func (t *YamlConfigTest) NewYamlConfigTest() *YamlConfig {
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	regionConfig := &shardingconfig.RegionConfig{}
	t.YamlConfig = PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", ":80", ":81",
		map[string]shardingconfig.RegionConfig{"region": *regionConfig})
	return &t.YamlConfig
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

func TestYAMLUrlParsingEmpty(t *testing.T) {
	incorrect := []byte(`field: "1"`)
	testyaml := shardingconfig.YAMLUrl{}
	err := yaml.Unmarshal(incorrect, &testyaml)
	assert.Error(t, err, "Should not even try to parse")
	assert.Nil(t, testyaml.URL, "Should be nil")
}

func TestListenYamlParameterValidation(t *testing.T) {
	incorrect := []byte(`Listen: ":8080"`)
	testyaml := TestYaml{}
	err := yaml.Unmarshal(incorrect, &testyaml)
	assert.NoError(t, err, "Should not even try to parse")
	assert.Nil(t, testyaml.Field.URL, "Should be nil")
}

func TestShouldValidateListenConf(t *testing.T) {
	var testConf YamlConfigTest
	testListenData := []string{"127.0.0.1:8080", ":8080"}

	for _, listenValue := range testListenData {
		testConf.NewYamlConfigTest().Listen = listenValue
		result, _ := ValidateConf(testConf.YamlConfig, false)
		assert.True(t, result, "Should be true")
	}
}

func TestTechnicalEndpointListenYamlParameterValidation(t *testing.T) {
	incorrect := []byte(`TechnicalEndpointListen: ":8080"`)
	testyaml := TestYaml{}
	err := yaml.Unmarshal(incorrect, &testyaml)
	assert.NoError(t, err, "Should not even try to parse")
	assert.Nil(t, testyaml.Field.URL, "Should be nil")
}

func TestShouldValidateTechnicalEndpointListenConf(t *testing.T) {
	var testConf YamlConfigTest
	testListenData := []string{"127.0.0.1:8080", ":8080"}

	for _, listenValue := range testListenData {
		testConf.NewYamlConfigTest().Listen = listenValue
		result, _ := ValidateConf(testConf.YamlConfig, false)
		assert.True(t, result, "Should be true")
	}
}
func TestShouldNotValidateListenConf(t *testing.T) {
	var testConf YamlConfigTest
	testWrongListenData := []string{"", "-", " ", "aaa", ":bbb", "c:"}

	for _, listenWrongValue := range testWrongListenData {
		testConf.NewYamlConfigTest().Listen = listenWrongValue
		result, _ := ValidateConf(testConf.YamlConfig, false)
		assert.False(t, result, "Should be false")
	}
}

func TestShouldValidateConfMaintainedBackendWhenNotEmpty(t *testing.T) {
	maintainedBackendHost := "127.0.0.1:85"
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	regionConfig := &shardingconfig.RegionConfig{}
	testConfData := PrepareYamlConfig(size, 21, 32, maintainedBackendHost, ":80", ":81", map[string]shardingconfig.RegionConfig{"region": *regionConfig})

	result, _ := ValidateConf(testConfData, false)

	assert.True(t, result, "Should be true")
}

func TestShouldValidateConfMaintainedBackendWhenEmpty(t *testing.T) {
	maintainedBackendHost := ""
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 4096
	regionConfig := &shardingconfig.RegionConfig{}
	testConfData := PrepareYamlConfig(size, 22, 33, maintainedBackendHost, ":80", ":81", map[string]shardingconfig.RegionConfig{"region": *regionConfig})

	result, _ := ValidateConf(testConfData, false)

	assert.True(t, result, "Should be true")
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
	syncLogMethodsTestData := []shardingconfig.SyncLogMethod{}
	errors := yaml.Unmarshal([]byte(data), &syncLogMethodsTestData)

	assert.Nil(t, errors)
}

func TestShouldNotValidateWrongSyncLogMethod(t *testing.T) {
	data := `
- WRONG
`
	syncLogMethodsTestData := []shardingconfig.SyncLogMethod{}
	errors := yaml.Unmarshal([]byte(data), &syncLogMethodsTestData)

	assert.NotNil(t, errors)
}

func TestAdditionalHeadersYamlParsingSuccessful(t *testing.T) {
	correct := `
'Access-Control-Allow-Credentials': "true"
'Access-Control-Allow-Methods': "GET, POST, OPTIONS"
`
	testyaml := shardingconfig.AdditionalHeaders{}
	err := yaml.Unmarshal([]byte(correct), &testyaml)

	assert.NoError(t, err, "Should be correct")
}

func TestAdditionalHeadersYamlParsingFailureWhenKeyIsEmpty(t *testing.T) {
	incorrect := []byte(`
'Access-Control-Allow-Credentials': "true"
'': "GET, POST, OPTIONS"
`)
	testyaml := shardingconfig.AdditionalHeaders{}
	err := yaml.Unmarshal(incorrect, &testyaml)

	assert.Error(t, err, "Empty key should return error")
}

func TestAdditionalHeadersYamlParsingFailureWhenValueIsEmpty(t *testing.T) {
	incorrect := []byte(`
'Access-Control-Allow-Methods': ""
'Access-Control-Allow-Credentials': "true"
`)
	testyaml := shardingconfig.AdditionalHeaders{}
	err := yaml.Unmarshal(incorrect, &testyaml)

	assert.Error(t, err, "Empty value should return error")
}

func TestDurationYamlParsingWithSuccess(t *testing.T) {
	correct := []byte(`1s`)
	testyaml := metrics.Interval{}
	err := yaml.Unmarshal(correct, &testyaml)

	assert.NoError(t, err, "Should be correct")
}

func TestDurationYamlParsingWithIncorrectValue(t *testing.T) {
	correct := []byte(`23ss`)
	testyaml := metrics.Interval{}
	err := yaml.Unmarshal(correct, &testyaml)

	assert.Error(t, err, "Missing duration should return error")
}

func TestShouldValidateBodyMaxSizeWithCorrectSize(t *testing.T) {
	correct := []byte(`10MB`)
	testyaml := shardingconfig.HumanSizeUnits{}
	err := yaml.Unmarshal(correct, &testyaml)

	assert.NoError(t, err, "Should be correct size")
}

func TestShouldNotValidateBodyMaxSizeWithIncorrectValue(t *testing.T) {
	correct := []byte(`GB`)
	testyaml := shardingconfig.HumanSizeUnits{}
	err := yaml.Unmarshal(correct, &testyaml)

	assert.Error(t, err, "Missing BodyMaxSize should return error")
}

func TestShouldNotValidateBodyMaxSizeWithZero(t *testing.T) {
	correct := []byte(`0`)
	testyaml := shardingconfig.HumanSizeUnits{}
	err := yaml.Unmarshal(correct, &testyaml)

	assert.Error(t, err, "Missing BodyMaxSize should return error")
}

func TestShouldPassValidateConfigurationHTTPHandler(t *testing.T) {
	correctYamlData := yamlConfigWithoutRegionsSection +
		`Regions:
  testregion:
    Clusters:
      - Cluster: cluster1test
        Weight: 1
    Domains:
      - endpoint.dc
DisableKeepAlives: false
`
	writer, request := callConfigValidateRequest(correctYamlData)

	ValidateConfigurationHTTPHandler(writer, request)

	assert.Equal(t, http.StatusOK, writer.Code)
	assert.Contains(t, "Configuration checked - OK.", writer.Body.String())
}

func TestShouldNotValidateConfigurationHTTPHandlerWithoutRegionsSection(t *testing.T) {
	incorrectYamlData := yamlConfigWithoutRegionsSection

	writer, request := callConfigValidateRequest(incorrectYamlData)

	ValidateConfigurationHTTPHandler(writer, request)

	assert.Equal(t, http.StatusBadRequest, writer.Code)
	assert.Contains(t, "map[RegionsEntryLogicalValidator:[Empty regions definition]]", writer.Body.String())
}

func TestShouldNotPassValidateConfigurationHTTPHandlerWithWrongRequests(t *testing.T) {
	correctEndpointURL := "http://127.0.0.1:8071/configuration/validate"
	correctContentType := "application/yaml"
	incorrectRequestData := []struct {
		expectedStatusCode int
		method             string
		contentType        string
		url                string
		body               string
		contentLen         int
	}{
		{http.StatusBadRequest, http.MethodPost, correctContentType, correctEndpointURL, "", 0},
		{http.StatusBadRequest, http.MethodPost, "", correctEndpointURL, "", 0},
		{http.StatusBadRequest, http.MethodPost, correctContentType, correctEndpointURL + "/wrongpath", "", 0},
		{http.StatusRequestEntityTooLarge, http.MethodPost, correctContentType, correctEndpointURL, strings.Repeat("#", TechnicalEndpointBodyMaxSize+1), TechnicalEndpointBodyMaxSize},
		{http.StatusUnsupportedMediaType, http.MethodPost, "application/json", correctEndpointURL, "Listen: :8080\n", 14},
		{http.StatusMethodNotAllowed, http.MethodDelete, correctContentType, correctEndpointURL, "", 0},
		{http.StatusMethodNotAllowed, http.MethodPut, correctContentType, correctEndpointURL, "", 0},
		{http.StatusMethodNotAllowed, http.MethodGet, correctContentType, correctEndpointURL, "", 0},
	}

	for _, testData := range incorrectRequestData {
		bodyReader := bytes.NewBufferString(testData.body)
		request := httptest.NewRequest(testData.method, testData.url, bodyReader)
		request.Header.Set("Content-Length", fmt.Sprintf("%d", testData.contentLen))
		request.Header.Set("Content-Type", testData.contentType)
		writer := httptest.NewRecorder()

		ValidateConfigurationHTTPHandler(writer, request)

		assert.Equal(t, testData.expectedStatusCode, writer.Code)
	}
}

func TestShouldNotPassWhenNoRegionIsDefined(t *testing.T) {
	maintainedBackendHost := "127.0.0.1:85"
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	testConfData := PrepareYamlConfig(size, 21, 32, maintainedBackendHost, ":80", ":81", map[string]shardingconfig.RegionConfig{})

	result, _ := ValidateConf(testConfData, true)

	assert.False(t, result)
}

func PrepareYamlConfig(bodyMaxSize shardingconfig.HumanSizeUnits, idleConnTimeoutInp time.Duration, responseHeaderTimeoutInp time.Duration,
	maintainedBackendHost string, listen string, technicalEndpointListen string,
	regions map[string]shardingconfig.RegionConfig) YamlConfig {

	syncLogMethods := []shardingconfig.SyncLogMethod{{Method: "POST"}}

	url1 := url.URL{
		Scheme: "http",
		Host:   "127.0.0.1:8080",
	}
	yamlURL := []shardingconfig.YAMLUrl{{URL: &url1}}

	maxIdleConns := 1
	maxIdleConnsPerHost := 2
	maxConcurrentRequests := int32(200)
	clusters := map[string]shardingconfig.ClusterConfig{"cluster1test": {
		Backends: yamlURL,
	}}

	url2 := url.URL{Scheme: "http", Host: maintainedBackendHost}
	maintainedBackends := []shardingconfig.YAMLUrl{{URL: &url2}}

	additionalRequestHeaders := shardingconfig.AdditionalHeaders{
		"Cache-Control": "public, s-maxage=600, max-age=600",
	}

	additionalResponseHeaders := shardingconfig.AdditionalHeaders{
		"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
	}

	return YamlConfig{
		Listen:                    listen,
		HealthCheckEndpoint:       "/status/ping",
		TechnicalEndpointListen:   technicalEndpointListen,
		Backends:                  []shardingconfig.YAMLUrl{},
		BodyMaxSize:               bodyMaxSize,
		MaxIdleConns:              maxIdleConns,
		MaxIdleConnsPerHost:       maxIdleConnsPerHost,
		IdleConnTimeout:           metrics.Interval{Duration: idleConnTimeoutInp},
		ResponseHeaderTimeout:     metrics.Interval{Duration: responseHeaderTimeoutInp},
		MaxConcurrentRequests:     maxConcurrentRequests,
		Clusters:                  clusters,
		Regions:                   regions,
		AdditionalRequestHeaders:  additionalRequestHeaders,
		AdditionalResponseHeaders: additionalResponseHeaders,
		MaintainedBackends:        maintainedBackends,
		SyncLogMethods:            syncLogMethods,
		Logging:                   logconfig.LoggingConfig{},
		Metrics:                   metrics.Config{},
		DisableKeepAlives:         false,
	}
}

func callConfigValidateRequest(data string) (writer *httptest.ResponseRecorder, request *http.Request) {
	bodyReader := bytes.NewBufferString(string(data))
	request = httptest.NewRequest(http.MethodPost, yamlValidateEndpointURL, bodyReader)
	request.Header.Set("Content-Length", fmt.Sprintf("%d", len(data)))
	request.Header.Set("Content-Type", "application/yaml")
	writer = httptest.NewRecorder()
	return
}
