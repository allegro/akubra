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

	httpHandlerConfig "github.com/allegro/akubra/httphandler/config"
	logconfig "github.com/allegro/akubra/log/config"
	"github.com/allegro/akubra/metrics"
	regionsConfig "github.com/allegro/akubra/regions/config"
	shardingconfig "github.com/allegro/akubra/sharding/config"
	storageconfig "github.com/allegro/akubra/storages/config"
	transportConfig "github.com/allegro/akubra/transport/config"
	"github.com/stretchr/testify/assert"

	"gopkg.in/yaml.v2"
)

const (
	yamlValidateEndpointURL         string = "http://127.0.0.1:8071/configuration/validate"
	yamlConfigWithoutRegionsSection string = `Service:
  Server:
    Listen: ":80"
    TechnicalEndpointListen: ":81"
    HealthCheckEndpoint: "/status/ping"
    MaxConcurrentRequests: 200
  Client:
    BodyMaxSize: 2048
    MaxIdleConns: 1
    MaxIdleConnsPerHost: 2
    IdleConnTimeout: 3s
    ResponseHeaderTimeout: 4s
    AdditionalRequestHeaders:
      Cache-Control: public, s-maxage=600, max-age=600
    AdditionalResponseHeaders:
      Access-Control-Allow-Methods: GET, POST, OPTIONS
    Transports:
      - Name: TransportForMethods:GET|PUT|POST
        Rules:
          Method: GET|PUT|POST
        Properties:
          MaxIdleConns: 500
          MaxIdleConnsPerHost: 500
          IdleConnTimeout: 2s
          ResponseHeaderTimeout: 2s
Storages:
  dummy:
    Endpoint: "http://127.0.0.1:8080"
    Type: "passthrough"
    Maintenance: false
Shards:
  cluster1test:
    Backends:
      - dummy
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
	var size httpHandlerConfig.HumanSizeUnits
	size.SizeInBytes = 2048
	region := regionsConfig.Policies{}
	t.YamlConfig = PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", ":80", ":81",
		map[string]regionsConfig.Policies{"region": region}, nil)
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
		testConf.NewYamlConfigTest().Service.Server.Listen = listenValue
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
		testConf.NewYamlConfigTest().Service.Server.Listen = listenValue
		result, _ := ValidateConf(testConf.YamlConfig, false)
		assert.True(t, result, "Should be true")
	}
}
func TestShouldNotValidateListenConf(t *testing.T) {
	var testConf YamlConfigTest
	testWrongListenData := []string{"", "-", " ", "aaa", ":bbb", "c:"}

	for _, listenWrongValue := range testWrongListenData {
		testConf.NewYamlConfigTest().Service.Server.Listen = listenWrongValue
		result, _ := ValidateConf(testConf.YamlConfig, false)
		assert.False(t, result, "Should be false")
	}
}

func TestShouldValidateConfMaintainedBackendWhenNotEmpty(t *testing.T) {
	maintainedBackendHost := "127.0.0.1:85"
	var size httpHandlerConfig.HumanSizeUnits
	size.SizeInBytes = 2048
	region := regionsConfig.Policies{}
	testConfData := PrepareYamlConfig(size, 21, 32, maintainedBackendHost, ":80", ":81",
		map[string]regionsConfig.Policies{"region": region}, nil)

	result, _ := ValidateConf(testConfData, false)

	assert.True(t, result, "Should be true")
}

func TestShouldValidateConfMaintainedBackendWhenEmpty(t *testing.T) {
	maintainedBackendHost := ""
	var size httpHandlerConfig.HumanSizeUnits
	size.SizeInBytes = 4096
	region := regionsConfig.Policies{}
	testConfData := PrepareYamlConfig(size, 22, 33, maintainedBackendHost,
		":80", ":81",
		map[string]regionsConfig.Policies{"region": region}, nil)

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
	testyaml := httpHandlerConfig.AdditionalHeaders{}
	err := yaml.Unmarshal([]byte(correct), &testyaml)

	assert.NoError(t, err, "Should be correct")
}

func TestAdditionalHeadersYamlParsingFailureWhenKeyIsEmpty(t *testing.T) {
	incorrect := []byte(`
'Access-Control-Allow-Credentials': "true"
'': "GET, POST, OPTIONS"
`)
	testyaml := httpHandlerConfig.AdditionalHeaders{}
	err := yaml.Unmarshal(incorrect, &testyaml)

	assert.Error(t, err, "Empty key should return error")
}

func TestAdditionalHeadersYamlParsingFailureWhenValueIsEmpty(t *testing.T) {
	incorrect := []byte(`
'Access-Control-Allow-Methods': ""
'Access-Control-Allow-Credentials': "true"
`)
	testyaml := httpHandlerConfig.AdditionalHeaders{}
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
	testyaml := httpHandlerConfig.HumanSizeUnits{}
	err := yaml.Unmarshal(correct, &testyaml)

	assert.NoError(t, err, "Should be correct size")
}

func TestShouldNotValidateBodyMaxSizeWithIncorrectValue(t *testing.T) {
	correct := []byte(`GB`)
	testyaml := httpHandlerConfig.HumanSizeUnits{}
	err := yaml.Unmarshal(correct, &testyaml)

	assert.Error(t, err, "Missing BodyMaxSize should return error")
}

func TestShouldNotValidateBodyMaxSizeWithZero(t *testing.T) {
	correct := []byte(`0`)
	testyaml := httpHandlerConfig.HumanSizeUnits{}
	err := yaml.Unmarshal(correct, &testyaml)

	assert.Error(t, err, "Missing BodyMaxSize should return error")
}

func TestShouldPassValidateConfigurationHTTPHandler(t *testing.T) {
	correctYamlData := yamlConfigWithoutRegionsSection +
		`ShardingPolicies:
  testregion:
    Shards:
      - ShardName: cluster1test
        Weight: 1
    Domains:
      - endpoint.dc
DisableKeepAlives: false
`
	writer, request := callConfigValidateRequest(correctYamlData)

	ValidateConfigurationHTTPHandler(writer, request)

	assert.Equal(t, http.StatusOK, writer.Code)
	assert.Contains(t, writer.Body.String(), "Configuration checked - OK.")
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
	var size httpHandlerConfig.HumanSizeUnits
	size.SizeInBytes = 2048
	testConfData := PrepareYamlConfig(size, 21, 32, maintainedBackendHost,
		":80", ":81", regionsConfig.ShardingPolicies{}, nil)

	result, _ := ValidateConf(testConfData, true)

	assert.False(t, result)
}

func PrepareYamlConfig(
	bodyMaxSize httpHandlerConfig.HumanSizeUnits,
	idleConnTimeoutInp time.Duration,
	responseHeaderTimeoutInp time.Duration,
	maintainedBackendHost string,
	listen string,
	technicalEndpointListen string,
	policies regionsConfig.ShardingPolicies, transports transportConfig.Transports) YamlConfig {

	url1 := url.URL{Scheme: "http", Host: "127.0.0.1:8080"}
	yamlURL := shardingconfig.YAMLUrl{URL: &url1}

	url2 := url.URL{Scheme: "http", Host: maintainedBackendHost}
	maintainedBackends := shardingconfig.YAMLUrl{URL: &url2}

	maxIdleConns := 1
	maxIdleConnsPerHost := 2
	maxConcurrentRequests := int32(200)
	storageMap := make(storageconfig.StoragesMap)
	storageMap["default"] = storageconfig.Storage{
		Backend: yamlURL,
	}

	storageMap["maintained"] = storageconfig.Storage{
		Backend: maintainedBackends,
	}
	shardsMap := make(storageconfig.ShardsMap)
	shardsMap["cluster1test"] = storageconfig.Shard{
		Storages: storageconfig.Storages{{Name: "default"}},
	}

	additionalRequestHeaders := httpHandlerConfig.AdditionalHeaders{
		"Cache-Control": "public, s-maxage=600, max-age=600",
	}

	additionalResponseHeaders := httpHandlerConfig.AdditionalHeaders{
		"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
	}

	clientTransportRules := transportConfig.ClientTransportRules{
		Method:     "GET",
		Path:       "/path",
		QueryParam: "?acl",
	}

	clientTransportDetail := transportConfig.ClientTransportProperties{
		MaxIdleConns:          maxIdleConns,
		MaxIdleConnsPerHost:   maxIdleConnsPerHost,
		IdleConnTimeout:       metrics.Interval{Duration: idleConnTimeoutInp},
		ResponseHeaderTimeout: metrics.Interval{Duration: responseHeaderTimeoutInp},
		DisableKeepAlives:     false,
	}

	if transports == nil {
		transports = transportConfig.Transports{
			transportConfig.TransportMatcherDefinition{
				Name:       "TestTransport",
				Rules:      clientTransportRules,
				Properties: clientTransportDetail,
			},
		}
	}

	return YamlConfig{
		Service: httpHandlerConfig.Service{
			Server: httpHandlerConfig.Server{
				Listen:                  listen,
				HealthCheckEndpoint:     "/status/ping",
				TechnicalEndpointListen: technicalEndpointListen,
				MaxConcurrentRequests:   maxConcurrentRequests,
				BodyMaxSize:             bodyMaxSize,
			},
			Client: httpHandlerConfig.Client{
				Transports: transports,

				AdditionalRequestHeaders:  additionalRequestHeaders,
				AdditionalResponseHeaders: additionalResponseHeaders,
			},
		},

		Storages:         storageMap,
		Shards:           shardsMap,
		ShardingPolicies: policies,
		Logging:          logconfig.LoggingConfig{},
		Metrics:          metrics.Config{},
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
