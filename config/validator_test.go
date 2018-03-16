package config

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	httphandlerconfig "github.com/allegro/akubra/httphandler/config"
	"github.com/allegro/akubra/metrics"
	regionsconfig "github.com/allegro/akubra/regions/config"
	transportconfig "github.com/allegro/akubra/transport/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	validator "gopkg.in/validator.v1"
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
	regionConfig := regionsconfig.Region{}
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", listen,
		listenTechnicalEndpoint,
		map[string]regionsconfig.Region{"region": regionConfig}, nil)
	valid, validationErrors := yamlConfig.ListenPortsLogicalValidator()

	assert.Len(t, validationErrors, 0, "Should not be errors")
	assert.True(t, valid, "Should be true")
}

func TestShouldNotPassListenPortsLogicalValidatorWhenPortsAreEqual(t *testing.T) {
	listen := "127.0.0.1:8080"
	listenTechnicalEndpoint := listen
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	regionConfig := regionsconfig.Region{}
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", listen,
		listenTechnicalEndpoint,
		map[string]regionsconfig.Region{"region": regionConfig}, nil)
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
	multiClusterConfig := regionsconfig.RegionCluster{
		Name:   "cluster1test",
		Weight: 1,
	}

	regionConfig := regionsconfig.Region{
		Clusters: []regionsconfig.RegionCluster{multiClusterConfig},
		Domains:  []string{"domain.dc"},
	}

	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048

	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235",
		map[string]regionsconfig.Region{"region": regionConfig}, nil)

	valid, validationErrors := yamlConfig.RegionsEntryLogicalValidator()
	assert.True(t, valid)
	assert.Empty(t, validationErrors)
}

func TestValidatorShouldFailWithMissingCluster(t *testing.T) {
	multiClusterConfig := regionsconfig.RegionCluster{
		Name:   "someothercluster",
		Weight: 1,
	}

	regionConfig := regionsconfig.Region{
		Clusters: []regionsconfig.RegionCluster{multiClusterConfig},
		Domains:  []string{"domain.dc"},
	}
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235",
		map[string]regionsconfig.Region{"testregion": regionConfig}, nil)
	valid, validationErrors := yamlConfig.RegionsEntryLogicalValidator()
	assert.False(t, valid)
	assert.Equal(
		t,
		errors.New("Cluster \"testregion\" is region \"someothercluster\" is not defined"),
		validationErrors["RegionsEntryLogicalValidator"][0])
}

func TestValidatorShouldFailWithInvalidWeight(t *testing.T) {
	multiClusterConfig := regionsconfig.RegionCluster{
		Name:   "cluster1test",
		Weight: 199,
	}
	regionConfig := regionsconfig.Region{
		Clusters: []regionsconfig.RegionCluster{multiClusterConfig},
		Domains:  []string{"domain.dc"},
	}
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	regions := map[string]regionsconfig.Region{"testregion": regionConfig}
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235", regions, nil)

	valid, validationErrors := yamlConfig.RegionsEntryLogicalValidator()
	assert.False(t, valid)
	assert.Equal(
		t,
		errors.New("Weight for cluster \"cluster1test\" in region \"testregion\" is not valid"),
		validationErrors["RegionsEntryLogicalValidator"][0])
}

func TestValidatorShouldFailWithMissingClusterDomain(t *testing.T) {
	multiClusterConfig := regionsconfig.RegionCluster{
		Name:   "cluster1test",
		Weight: 1,
	}
	regionConfig := regionsconfig.Region{
		Clusters: []regionsconfig.RegionCluster{multiClusterConfig},
	}
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048

	regions := map[string]regionsconfig.Region{"testregion": regionConfig}
	yamlConfig := PrepareYamlConfig(size, 31, 45,
		"127.0.0.1:81", "127.0.0.1:1234", "127.0.0.1:1235", regions, nil)
	valid, validationErrors := yamlConfig.RegionsEntryLogicalValidator()
	assert.False(t, valid)
	assert.Equal(
		t,
		errors.New("No domain defined for region \"testregion\""),
		validationErrors["RegionsEntryLogicalValidator"][0])
}

func TestValidatorShouldFailWithMissingClusterDefinition(t *testing.T) {
	regionConfig := regionsconfig.Region{
		Domains: []string{"domain.dc"},
	}
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	regions := map[string]regionsconfig.Region{"testregion": regionConfig}
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235", regions, nil)
	valid, validationErrors := yamlConfig.RegionsEntryLogicalValidator()
	assert.False(t, valid)
	assert.Equal(
		t,
		errors.New("No clusters defined for region \"testregion\""),
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
	validTransports := transportconfig.Transports{
		transportconfig.Transport{
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
		"127.0.0.1:1234", "127.0.0.1:1235", nil, validTransports)
	valid, _ := yamlConfig.TransportsEntryLogicalValidator()
	assert.True(t, valid)
}

func TestValidatorShouldProcessTransportsWithSuccessWithNotDefinedRulesProperties(t *testing.T) {
	invalidTransports := transportconfig.Transports{
		transportconfig.Transport{
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
		"127.0.0.1:1234", "127.0.0.1:1235", nil, invalidTransports)
	valid, _ := yamlConfig.TransportsEntryLogicalValidator()
	assert.True(t, valid)
}

func TestShouldValidWithEmptyPropertiesInRulesDefinition(t *testing.T) {
	invalidTransports := transportconfig.Transports{
		transportconfig.Transport{
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
		"127.0.0.1:1235", "127.0.0.1:1236", nil, invalidTransports)
	valid, _ := yamlConfig.TransportsEntryLogicalValidator()
	assert.True(t, valid)
}

func TestValidatorShouldValidateTransportsWithEmptyRules(t *testing.T) {
	validTransports := transportconfig.Transports{
		transportconfig.Transport{
			Name: "TestTransport",
			Rules: transportconfig.ClientTransportRules{
				Method: "GET",
			},
			Properties: testTransportProperties,
		},
		transportconfig.Transport{
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
		"127.0.0.1:1234", "127.0.0.1:1235", nil, validTransports)
	valid, _ := yamlConfig.TransportsEntryLogicalValidator()
	assert.True(t, valid)
}

func TestShouldFailTransportsEntryLogicalValidatorWithoutProperties(t *testing.T) {
	invalidTransports := transportconfig.Transports{
		transportconfig.Transport{
			Name: "TestTransport",
			Rules: transportconfig.ClientTransportRules{
				Method: "PUT",
			},
		},
	}
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235", nil, invalidTransports)
	valid, validationErrors := yamlConfig.TransportsEntryLogicalValidator()
	assert.False(t, valid)
	assert.Equal(
		t,
		errors.New("Wrong or empty transport 'Properties' for 'Name': TestTransport"),
		validationErrors["TransportsEntryLogicalValidator"][0])
}

func TestShouldPassTransportsEntryLogicalValidatorWhenIdleConnTimeoutPropertyIsZero(t *testing.T) {
	testTransportProps := transportconfig.ClientTransportProperties{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout: metrics.Interval{
			Duration: 0,
		},
		ResponseHeaderTimeout: metrics.Interval{
			Duration: 1,
		},
		DisableKeepAlives: false,
	}

	invalidTransports := transportconfig.Transports{
		transportconfig.Transport{
			Name: "TestTransport",
			Rules: transportconfig.ClientTransportRules{
				Method: "PUT",
			},
			Properties: testTransportProps,
		},
	}
	var size httphandlerconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81",
		"127.0.0.1:1234", "127.0.0.1:1235", nil, invalidTransports)
	valid, _ := yamlConfig.TransportsEntryLogicalValidator()
	assert.True(t, valid)
}
