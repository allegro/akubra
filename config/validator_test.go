package config

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"time"

	httphandlerconfig "github.com/allegro/akubra/httphandler/config"
	"github.com/allegro/akubra/metrics"
	shardsconfig "github.com/allegro/akubra/regions/config"
	transportconfig "github.com/allegro/akubra/transport/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/validator.v1"
)

type CustomItemsTestUnique struct {
	Items []string `validate:"UniqueValuesSlice=Items"`
}

type CustomItemsTestNoEmpty struct {
	Items []string `validate:"NoEmptyValuesSlice=Items"`
}

var testTransportProperties = transportconfig.ClientTransportProperties{
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

func TestShouldValidateWhenValuesInSliceAreUnique(t *testing.T) {
	var data CustomItemsTestUnique
	data.Items = []string{"item001", "item002"}

	err := validator.SetValidationFunc("UniqueValuesSlice", UniqueValuesInSliceValidator)
	require.NoError(t, err)
	valid, _ := validator.Validate(data)

	assert.True(t, valid, "Should be true")
}

func TestShouldNotValidateWhenValuesInSliceAreDuplicated(t *testing.T) {
	var data CustomItemsTestUnique
	data.Items = []string{"not_unique", "not_unique"}

	err := validator.SetValidationFunc("UniqueValuesSlice", UniqueValuesInSliceValidator)
	require.NoError(t, err)
	valid, validationErrors := validator.Validate(data)

	assert.Contains(t, validationErrors, "Items")
	assert.False(t, valid, "Should be false")
}

func TestShouldValidateWhenValuesInSliceAreNoEmpty(t *testing.T) {
	var data CustomItemsTestNoEmpty
	data.Items = []string{"i1", "i2"}

	err := validator.SetValidationFunc("NoEmptyValuesSlice", NoEmptyValuesInSliceValidator)
	require.NoError(t, err)

	valid, _ := validator.Validate(data)

	assert.True(t, valid, "Should be true")
}

func TestShouldNotValidateWhenValuesInSliceAreEmpty(t *testing.T) {
	var data CustomItemsTestNoEmpty
	data.Items = []string{"value", "  "}

	err := validator.SetValidationFunc("NoEmptyValuesSlice", NoEmptyValuesInSliceValidator)
	require.NoError(t, err)

	valid, validationErrors := validator.Validate(data)

	assert.Contains(t, validationErrors, "Items")
	assert.False(t, valid, "Should be false")
}

func TestShouldPassListenPortsLogicalValidator(t *testing.T) {
	listen := ":8080"
	listenTechnicalEndpoint := ":8081"
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	regionConfig := shardsconfig.Policies{}
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", listen,
		listenTechnicalEndpoint,
		map[string]shardsconfig.Policies{"region": regionConfig}, nil)
	valid, validationErrors := yamlConfig.ListenPortsLogicalValidator()

	assert.Len(t, validationErrors, 0, "Should not be errors")
	assert.True(t, valid, "Should be true")
}

func TestShouldNotPassListenPortsLogicalValidatorWhenPortsAreEqual(t *testing.T) {
	listen := "127.0.0.1:8080"
	listenTechnicalEndpoint := listen
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	regionConfig := shardsconfig.Policies{}
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", listen,
		listenTechnicalEndpoint,
		map[string]shardsconfig.Policies{"region": regionConfig}, nil)
	valid, validationErrors := yamlConfig.ListenPortsLogicalValidator()

	assert.Len(t, validationErrors, 1, "Should be one error")
	assert.False(t, valid, "Should be false")
}

func TestShouldPassHeaderContentLengthValidator(t *testing.T) {
	var bodySizeLimit int64 = 128
	request := httptest.NewRequest("POST", "http://somepath", nil)
	request.Header.Set("Content-Length", "128")
	result := httphandlerconfig.RequestHeaderContentLengthValidator(*request, bodySizeLimit)
	assert.Equal(t, 0, result)
}

func TestShouldNotPassHeaderContentLengthValidatorAndReturnEntityTooLargeCode(t *testing.T) {
	var bodySizeLimit int64 = 1024
	request := httptest.NewRequest("POST", "http://somepath", nil)
	request.Header.Set("Content-Length", "1025")
	result := httphandlerconfig.RequestHeaderContentLengthValidator(*request, bodySizeLimit)
	assert.Equal(t, http.StatusRequestEntityTooLarge, result)
}

func TestShouldNotPassHeaderContentLengthValidatorAndReturnBadRequestOnUnparsableContentLengthHeader(t *testing.T) {
	var bodySizeLimit int64 = 64
	request := httptest.NewRequest("POST", "http://somepath", nil)
	request.Header.Set("Content-Length", "strange-content-header")
	result := httphandlerconfig.RequestHeaderContentLengthValidator(*request, bodySizeLimit)
	assert.Equal(t, http.StatusBadRequest, result)
}

func TestShouldPassRequestHeaderContentTypeValidator(t *testing.T) {
	requiredContentType := "application/yaml"
	request := httptest.NewRequest("POST", "http://somepath", nil)
	request.Header.Set("Content-Type", "application/yaml")
	result := RequestHeaderContentTypeValidator(*request, requiredContentType)
	assert.Equal(t, 0, result)
}

func TestShouldNotPassRequestHeaderContentTypeValidatorWhenContentTypeIsEmpty(t *testing.T) {
	requiredContentType := "application/yaml"
	request := httptest.NewRequest("POST", "http://somepath", nil)
	request.Header.Set("Content-Type", "")
	result := RequestHeaderContentTypeValidator(*request, requiredContentType)
	assert.Equal(t, http.StatusBadRequest, result)
}

func TestShouldNotPassRequestHeaderContentTypeValidatorWhenContentTypeIsUnsupported(t *testing.T) {
	requiredContentType := "application/yaml"
	request := httptest.NewRequest("POST", "http://somepath", nil)
	request.Header.Set("Content-Type", "application/json")
	result := RequestHeaderContentTypeValidator(*request, requiredContentType)
	assert.Equal(t, http.StatusUnsupportedMediaType, result)
}

func TestValidatorShouldPassWithValidRegionConfig(t *testing.T) {
	multiClusterConfig := shardsconfig.Policy{
		ShardName: "cluster1test",
		Weight:    1,
	}

	regionConfig := shardsconfig.Policies{
		Shards:  []shardsconfig.Policy{multiClusterConfig},
		Domains: []string{"domain.dc"},
	}

	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048

	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235",
		map[string]shardsconfig.Policies{"region": regionConfig}, nil)

	valid, validationErrors := yamlConfig.RegionsEntryLogicalValidator()
	assert.True(t, valid)
	assert.Empty(t, validationErrors)
}

func TestValidatorShouldFailWithMissingCluster(t *testing.T) {
	shardName := "someothercluster"
	policyName := "testregion"
	multiClusterConfig := shardsconfig.Policy{
		ShardName: shardName,
		Weight:    1,
	}

	regionConfig := shardsconfig.Policies{
		Shards:  []shardsconfig.Policy{multiClusterConfig},
		Domains: []string{"domain.dc"},
	}
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048

	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235",
		map[string]shardsconfig.Policies{policyName: regionConfig}, nil)
	valid, validationErrors := yamlConfig.RegionsEntryLogicalValidator()
	assert.False(t, valid)
	assert.Equal(
		t,
		fmt.Errorf("Shard \"%s\" in policy \"%s\" is not defined", shardName, policyName),
		validationErrors["RegionsEntryLogicalValidator"][0])
}

func TestValidatorShouldFailWithInvalidWeight(t *testing.T) {

	multiClusterConfig := shardsconfig.Policy{
		ShardName: "cluster1test",
		Weight:    199,
	}
	regionConfig := shardsconfig.Policies{
		Shards:  []shardsconfig.Policy{multiClusterConfig},
		Domains: []string{"domain.dc"},
	}
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	regions := map[string]shardsconfig.Policies{"testregion": regionConfig}
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235", regions, nil)

	valid, validationErrors := yamlConfig.RegionsEntryLogicalValidator()
	assert.False(t, valid)
	assert.Equal(
		t,
		errors.New("Weight for shard \"cluster1test\" in policy \"testregion\" is not valid"),
		validationErrors["RegionsEntryLogicalValidator"][0])
}

func TestValidatorShouldFailWithMissingClusterDomain(t *testing.T) {
	multiClusterConfig := shardsconfig.Policy{
		ShardName: "cluster1test",
		Weight:    1,
	}
	regionConfig := shardsconfig.Policies{
		Shards: []shardsconfig.Policy{multiClusterConfig},
	}
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048

	regions := map[string]shardsconfig.Policies{"testregion": regionConfig}
	yamlConfig := PrepareYamlConfig(size, 31, 45,
		"127.0.0.1:81", "127.0.0.1:1234", "127.0.0.1:1235", regions, nil)
	valid, validationErrors := yamlConfig.RegionsEntryLogicalValidator()
	assert.False(t, valid)
	assert.Equal(
		t,
		errors.New("No domain defined for policy \"testregion\""),
		validationErrors["RegionsEntryLogicalValidator"][0])
}

func TestValidatorShouldFailWithMissingClusterDefinition(t *testing.T) {
	regionConfig := shardsconfig.Policies{
		Domains: []string{"domain.dc"},
	}
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	regions := map[string]shardsconfig.Policies{"testregion": regionConfig}
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235", regions, nil)
	valid, validationErrors := yamlConfig.RegionsEntryLogicalValidator()
	assert.False(t, valid)
	assert.Equal(
		t,
		errors.New("No shards defined for policy \"testregion\""),
		validationErrors["RegionsEntryLogicalValidator"][0])
}

func TestValidatorShouldFailWithEmptyTransportsDefinition(t *testing.T) {
	transports := make(transportconfig.Transports, 0)
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235", nil, transports)
	valid, validationErrors := yamlConfig.TransportsEntryLogicalValidator()
	assert.False(t, valid)
	assert.Equal(
		t,
		errors.New("Empty transports definition"),
		validationErrors["TransportsEntryLogicalValidator"][0])
}

func TestValidatorShouldProcessTransportsWithSuccess(t *testing.T) {
	transports := transportconfig.Transports{
		transportconfig.TransportMatcherDefinition{
			Name: "TestTransport",
			Rules: transportconfig.ClientTransportRules{
				Method:     "GET",
				Path:       ".*",
				QueryParam: "",
			},
			Properties: testTransportProperties,
		},
	}
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235", nil, transports)
	valid, _ := yamlConfig.TransportsEntryLogicalValidator()
	assert.True(t, valid)
}

func TestValidatorShouldProcessTransportsWithSuccessWithNotDefinedRulesProperties(t *testing.T) {
	transports := transportconfig.Transports{
		transportconfig.TransportMatcherDefinition{
			Name: "TestTransport",
			Rules: transportconfig.ClientTransportRules{
				Method: "GET",
			},
			Properties: testTransportProperties,
		},
	}
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235", nil, transports)
	valid, _ := yamlConfig.TransportsEntryLogicalValidator()
	assert.True(t, valid)
}

func TestShouldValidateWithEmptyPropertiesInRulesDefinition(t *testing.T) {
	transports := transportconfig.Transports{
		transportconfig.TransportMatcherDefinition{
			Name: "TestTransport123",
			Rules: transportconfig.ClientTransportRules{
				Method:     "",
				Path:       "",
				QueryParam: "",
			},
			Properties: testTransportProperties,
		},
	}
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 51, 55, "127.0.0.1:82",
		"127.0.0.1:1235", "127.0.0.1:1236", nil, transports)
	valid, _ := yamlConfig.TransportsEntryLogicalValidator()
	assert.True(t, valid)
}

func TestValidatorShouldValidateTransportsWithEmptyRules(t *testing.T) {
	transports := transportconfig.Transports{
		transportconfig.TransportMatcherDefinition{
			Name: "TestTransport",
			Rules: transportconfig.ClientTransportRules{
				Method: "GET",
			},
			Properties: testTransportProperties,
		},
		transportconfig.TransportMatcherDefinition{
			Name: "DefaultTestTransport",
			Rules: transportconfig.ClientTransportRules{
				Method:     "",
				Path:       "",
				QueryParam: "",
			},
			Properties: testTransportProperties,
		},
	}
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235", nil, transports)
	valid, _ := yamlConfig.TransportsEntryLogicalValidator()
	assert.True(t, valid)
}

func TestValidatorShouldFailOnTransportWithoutProperties(t *testing.T) {
	transports := transportconfig.Transports{
		transportconfig.TransportMatcherDefinition{
			Name: "TestTransport",
			Rules: transportconfig.ClientTransportRules{
				Method: "PUT",
			},
		},
	}
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235", nil, transports)
	valid, validationErrors := yamlConfig.TransportsEntryLogicalValidator()
	assert.False(t, valid)
	assert.Equal(
		t,
		errors.New("Wrong or empty transport 'Properties' for 'Name': TestTransport"),
		validationErrors["TransportsEntryLogicalValidator"][0])
}

func TestShouldPassTransportsEntryLogicalValidatorWhenIdleConnTimeoutDurationIsZero(t *testing.T) {
	transports := prepareTransportsForEntryLogicalValidatorTest(100, 200, 0, 1)

	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235", nil, transports)
	valid, _ := yamlConfig.TransportsEntryLogicalValidator()
	assert.True(t, valid)
}

func TestShouldFailTransportsEntryLogicalValidatorWhenResponseHeaderTimeoutDurationIsZero(t *testing.T) {
	transports := prepareTransportsForEntryLogicalValidatorTest(100, 200, 1, 0)

	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235", nil, transports)
	result, _ := yamlConfig.TransportsEntryLogicalValidator()
	assert.False(t, result)
}

func TestShouldPassTransportsEntryLogicalValidatorWhenMaxIdleConnsIsZero(t *testing.T) {
	transports := prepareTransportsForEntryLogicalValidatorTest(0, 100, 3, 1)

	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235", nil, transports)
	valid, _ := yamlConfig.TransportsEntryLogicalValidator()
	assert.True(t, valid)
}

func TestShouldPassTransportsEntryLogicalValidatorWhenMaxIdleConnsPerHostIsZero(t *testing.T) {
	transports := prepareTransportsForEntryLogicalValidatorTest(20, 0, 2, 1)

	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235", nil, transports)
	valid, _ := yamlConfig.TransportsEntryLogicalValidator()
	assert.True(t, valid)
}

func prepareTransportsForEntryLogicalValidatorTest(maxIdleConns, maxIdleConnsPerHost int,
	idleConnTimeoutDuration, responseHeaderTimeoutDuration time.Duration) transportconfig.Transports {
	testTransportProps := transportconfig.ClientTransportProperties{
		MaxIdleConns:        maxIdleConns,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		IdleConnTimeout: metrics.Interval{
			Duration: idleConnTimeoutDuration,
		},
		ResponseHeaderTimeout: metrics.Interval{
			Duration: responseHeaderTimeoutDuration,
		},
		DisableKeepAlives: false,
	}

	return transportconfig.Transports{
		transportconfig.TransportMatcherDefinition{
			Name: "TestTransport",
			Rules: transportconfig.ClientTransportRules{
				Method: "PUT",
			},
			Properties: testTransportProps,
		},
	}
}
