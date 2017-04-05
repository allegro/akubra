package config

import (
	"net/url"
	"testing"
	"time"

	logconfig "github.com/allegro/akubra/log/config"
	"github.com/allegro/akubra/metrics"
	shardingconfig "github.com/allegro/akubra/sharding/config"
	"github.com/stretchr/testify/assert"
	yaml "gopkg.in/yaml.v2"
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
	t.YamlConfig = PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", ":80", "client1", []string{"cluster1test"})
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
	testConfData := PrepareYamlConfig(size, 21, 32, maintainedBackendHost, ":80", "client1", []string{"cluster1test"})

	result, _ := ValidateConf(testConfData, false)

	assert.True(t, result, "Should be true")
}

func TestShouldValidateConfMaintainedBackendWhenEmpty(t *testing.T) {
	maintainedBackendHost := ""
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 4096
	testConfData := PrepareYamlConfig(size, 22, 33, maintainedBackendHost, ":80", "client1", []string{"cluster1test"})

	result, _ := ValidateConf(testConfData, false)

	assert.True(t, result, "Should be true")
}

func TestShouldValidateConfClientNameWithMinLenght(t *testing.T) {
	var testConf YamlConfigTest
	testConf.NewYamlConfigTest().Client.Name = "c"

	result, _ := ValidateConf(testConf.YamlConfig, false)

	assert.True(t, result, "Should be true")
}

func TestShouldNotValidateConfClientNameWhenEmpty(t *testing.T) {
	var testConf YamlConfigTest
	testConf.NewYamlConfigTest().Client.Name = ""

	result, _ := ValidateConf(testConf.YamlConfig, false)

	assert.False(t, result, "Should be false")
}

func TestShouldValidateConfClientClustersValues(t *testing.T) {
	var testConf YamlConfigTest
	testConf.NewYamlConfigTest().Client.Clusters = []string{"prod", "jprod"}

	result, _ := ValidateConf(testConf.YamlConfig, false)

	assert.True(t, result, "Should be true")
}

func TestShouldNotValidateConfClientClustersValuesWhenEmpty(t *testing.T) {
	var testConf YamlConfigTest
	testConf.NewYamlConfigTest().Client.Clusters = []string{"prod", "  "}

	result, validationErrors := ValidateConf(testConf.YamlConfig, false)

	assert.Contains(t, validationErrors, "Client.Clusters")
	assert.False(t, result, "Should be false")
}

func TestShouldNotValidateConfClientClustersValuesWhenDuplicated(t *testing.T) {
	var testConf YamlConfigTest
	testConf.NewYamlConfigTest().Client.Clusters = []string{"jprod", "jprod"}

	result, validationErrors := ValidateConf(testConf.YamlConfig, false)

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

func PrepareYamlConfig(bodyMaxSize shardingconfig.HumanSizeUnits, idleConnTimeoutInp time.Duration, responseHeaderTimeoutInp time.Duration,
	maintainedBackendHost string, listen string, clientCfgName string,
	clientClusters []string) YamlConfig {

	syncLogMethods := []shardingconfig.SyncLogMethod{{Method: "POST"}}

	url1 := url.URL{
		Scheme: "http",
		Host:   "127.0.0.1:8080",
	}
	yamlURL := []shardingconfig.YAMLUrl{{&url1}}

	maxIdleConns := 1
	maxIdleConnsPerHost := 2
	clusters := map[string]shardingconfig.ClusterConfig{"cluster1test": {
		yamlURL,
		"replicator",
		1,
		map[string]string{},
	}}

	url2 := url.URL{Scheme: "http", Host: maintainedBackendHost}
	maintainedBackends := []shardingconfig.YAMLUrl{{&url2}}

	additionalRequestHeaders := shardingconfig.AdditionalHeaders{
		"Cache-Control": "public, s-maxage=600, max-age=600",
	}

	additionalResponseHeaders := shardingconfig.AdditionalHeaders{
		"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
	}

	clientCfg := &shardingconfig.ClientConfig{
		Name:     clientCfgName,
		Clusters: clientClusters,
	}

	return YamlConfig{
		listen,
		[]shardingconfig.YAMLUrl{},
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
		logconfig.LoggingConfig{},
		metrics.Config{},
		false,
	}
}
