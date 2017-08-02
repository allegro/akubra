package config

import (
	"testing"

	"net/http"
	"net/http/httptest"

	shardingconfig "github.com/allegro/akubra/sharding/config"
	"github.com/go-validator/validator"

	"errors"

	"github.com/stretchr/testify/assert"
)

type CustomItemsTestUnique struct {
	Items []string `validate:"UniqueValuesSlice=Items"`
}

type CustomItemsTestNoEmpty struct {
	Items []string `validate:"NoEmptyValuesSlice=Items"`
}

func TestShouldValidateWhenValuesInSliceAreUnique(t *testing.T) {
	var data CustomItemsTestUnique
	data.Items = []string{"item001", "item002"}

	validator.SetValidationFunc("UniqueValuesSlice", UniqueValuesInSliceValidator)
	valid, _ := validator.Validate(data)

	assert.True(t, valid, "Should be true")
}

func TestShouldNotValidateWhenValuesInSliceAreDuplicated(t *testing.T) {
	var data CustomItemsTestUnique
	data.Items = []string{"not_unique", "not_unique"}

	validator.SetValidationFunc("UniqueValuesSlice", UniqueValuesInSliceValidator)
	valid, validationErrors := validator.Validate(data)

	assert.Contains(t, validationErrors, "Items")
	assert.False(t, valid, "Should be false")
}

func TestShouldValidateWhenValuesInSliceAreNoEmpty(t *testing.T) {
	var data CustomItemsTestNoEmpty
	data.Items = []string{"i1", "i2"}

	validator.SetValidationFunc("NoEmptyValuesSlice", NoEmptyValuesInSliceValidator)
	valid, _ := validator.Validate(data)

	assert.True(t, valid, "Should be true")
}

func TestShouldNotValidateWhenValuesInSliceAreEmpty(t *testing.T) {
	var data CustomItemsTestNoEmpty
	data.Items = []string{"value", "  "}

	validator.SetValidationFunc("NoEmptyValuesSlice", NoEmptyValuesInSliceValidator)
	valid, validationErrors := validator.Validate(data)

	assert.Contains(t, validationErrors, "Items")
	assert.False(t, valid, "Should be false")
}

func TestShouldPassListenPortsLogicalValidator(t *testing.T) {
	listen := ":8080"
	listenTechnicalEndpoint := ":8081"
	valid := true
	validationErrors := make(map[string][]error)
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	regionConfig := &shardingconfig.RegionConfig{}
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", listen, listenTechnicalEndpoint, map[string]shardingconfig.RegionConfig{"region": *regionConfig})
	yamlConfig.ListenPortsLogicalValidator(&valid, &validationErrors)

	assert.Len(t, validationErrors, 0, "Should not be errors")
	assert.True(t, valid, "Should be true")
}

func TestShouldNotPassListenPortsLogicalValidatorWhenPortsAreEqual(t *testing.T) {
	listen := "127.0.0.1:8080"
	listenTechnicalEndpoint := listen
	valid := true
	validationErrors := make(map[string][]error)
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	regionConfig := &shardingconfig.RegionConfig{}
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", listen, listenTechnicalEndpoint, map[string]shardingconfig.RegionConfig{"region": *regionConfig})
	yamlConfig.ListenPortsLogicalValidator(&valid, &validationErrors)

	assert.Len(t, validationErrors, 1, "Should be one error")
	assert.False(t, valid, "Should be false")
}

func TestShouldPassHeaderContentLengthValidator(t *testing.T) {
	var bodySizeLimit int64 = 128
	request := httptest.NewRequest("POST", "http://somepath", nil)
	request.Header.Set("Content-Length", "128")
	result := RequestHeaderContentLengthValidator(*request, bodySizeLimit)
	assert.Equal(t, 0, result)
}

func TestShouldNotPassHeaderContentLengthValidatorAndReturnEntityTooLargeCode(t *testing.T) {
	var bodySizeLimit int64 = 1024
	request := httptest.NewRequest("POST", "http://somepath", nil)
	request.Header.Set("Content-Length", "1025")
	result := RequestHeaderContentLengthValidator(*request, bodySizeLimit)
	assert.Equal(t, http.StatusRequestEntityTooLarge, result)
}

func TestShouldNotPassHeaderContentLengthValidatorAndReturnBadRequestOnUnparsableContentLengthHeader(t *testing.T) {
	var bodySizeLimit int64 = 64
	request := httptest.NewRequest("POST", "http://somepath", nil)
	request.Header.Set("Content-Length", "strange-content-header")
	result := RequestHeaderContentLengthValidator(*request, bodySizeLimit)
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
	multiClusterConfig := &shardingconfig.MultiClusterConfig{
		Cluster: "cluster1test",
		Weight:  1,
	}
	regionConfig := &shardingconfig.RegionConfig{
		Clusters: []shardingconfig.MultiClusterConfig{*multiClusterConfig},
		Domains:  []string{"domain.dc"},
	}
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	regions := map[string]shardingconfig.RegionConfig{"testregion": *regionConfig}
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", "127.0.0.1:1234", "127.0.0.1:1235", regions)
	valid := false
	validationErrors := make(map[string][]error)
	yamlConfig.RegionsEntryLogicalValidator(&valid, &validationErrors)
	assert.True(t, valid)
	assert.Empty(t, validationErrors)
}

func TestValidatorShouldFailWithMissingCluster(t *testing.T) {
	multiClusterConfig := &shardingconfig.MultiClusterConfig{
		Cluster: "someothercluster",
		Weight:  1,
	}
	regionConfig := &shardingconfig.RegionConfig{
		Clusters: []shardingconfig.MultiClusterConfig{*multiClusterConfig},
		Domains:  []string{"domain.dc"},
	}
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	regions := map[string]shardingconfig.RegionConfig{"testregion": *regionConfig}
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", "127.0.0.1:1234", "127.0.0.1:1235", regions)
	valid := false
	validationErrors := make(map[string][]error)
	yamlConfig.RegionsEntryLogicalValidator(&valid, &validationErrors)
	assert.False(t, false)
	assert.Equal(
		t,
		errors.New("Cluster \"testregion\" is region \"someothercluster\" is not defined"),
		validationErrors["RegionsEntryLogicalValidator"][0])
}

func TestValidatorShouldFailWithInvalidWeight(t *testing.T) {
	multiClusterConfig := &shardingconfig.MultiClusterConfig{
		Cluster: "cluster1test",
		Weight:  199,
	}
	regionConfig := &shardingconfig.RegionConfig{
		Clusters: []shardingconfig.MultiClusterConfig{*multiClusterConfig},
		Domains:  []string{"domain.dc"},
	}
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	regions := map[string]shardingconfig.RegionConfig{"testregion": *regionConfig}
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", "127.0.0.1:1234", "127.0.0.1:1235", regions)
	valid := false
	validationErrors := make(map[string][]error)
	yamlConfig.RegionsEntryLogicalValidator(&valid, &validationErrors)
	assert.False(t, false)
	assert.Equal(
		t,
		errors.New("Weight for cluster \"cluster1test\" in region \"testregion\" is not valid"),
		validationErrors["RegionsEntryLogicalValidator"][0])
}

func TestValidatorShouldFailWithMissingClusterDomain(t *testing.T) {
	multiClusterConfig := &shardingconfig.MultiClusterConfig{
		Cluster: "cluster1test",
		Weight:  1,
	}
	regionConfig := &shardingconfig.RegionConfig{
		Clusters: []shardingconfig.MultiClusterConfig{*multiClusterConfig},
	}
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	regions := map[string]shardingconfig.RegionConfig{"testregion": *regionConfig}
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", "127.0.0.1:1234", "127.0.0.1:1235", regions)
	valid := false
	validationErrors := make(map[string][]error)
	yamlConfig.RegionsEntryLogicalValidator(&valid, &validationErrors)
	assert.False(t, false)
	assert.Equal(
		t,
		errors.New("No domain defined for region \"testregion\""),
		validationErrors["RegionsEntryLogicalValidator"][0])
}

func TestValidatorShouldFailWithMissingClusterDefinition(t *testing.T) {
	regionConfig := &shardingconfig.RegionConfig{
		Domains: []string{"domain.dc"},
	}
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	regions := map[string]shardingconfig.RegionConfig{"testregion": *regionConfig}
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", "127.0.0.1:1234", "127.0.0.1:1235", regions)
	valid := false
	validationErrors := make(map[string][]error)
	yamlConfig.RegionsEntryLogicalValidator(&valid, &validationErrors)
	assert.False(t, false)
	assert.Equal(
		t,
		errors.New("No clusters defined for region \"testregion\""),
		validationErrors["RegionsEntryLogicalValidator"][0])
}
