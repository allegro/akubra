package config

import (
	"testing"

	"net/http"
	"net/http/httptest"

	shardingconfig "github.com/allegro/akubra/sharding/config"
	"github.com/go-validator/validator"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestShouldPassClientClustersEntryLogicalValidator(t *testing.T) {
	existingClusterName := "cluster1test"
	valid := true
	validationErrors := make(map[string][]error)
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", ":80", ":81", "client1", []string{existingClusterName})
	yamlConfig.ClientClustersEntryLogicalValidator(&valid, &validationErrors)

	assert.Len(t, validationErrors, 0, "Should not be errors")
	assert.True(t, valid, "Should be true")
}

func TestShouldNotPassClientClustersEntryLogicalValidatorWhenClusterDoesNotExist(t *testing.T) {
	notExistingClusterName := "notExistingClusterName"
	valid := true
	validationErrors := make(map[string][]error)
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", ":80", ":81", "client1", []string{notExistingClusterName})
	yamlConfig.ClientClustersEntryLogicalValidator(&valid, &validationErrors)

	assert.Len(t, validationErrors, 1, "Should be one error")
	assert.False(t, valid, "Should be false")
}

func TestShouldNotPassClientClustersEntryLogicalValidatorWhenEmptyClustersDefinition(t *testing.T) {
	valid := true
	validationErrors := make(map[string][]error)
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", ":80", ":81", "client1", []string{})
	yamlConfig.ClientClustersEntryLogicalValidator(&valid, &validationErrors)

	assert.Len(t, validationErrors, 1, "Should be one error")
	assert.False(t, valid, "Should be false")
}

func TestShouldPassListenPortsLogicalValidator(t *testing.T) {
	listen := ":8080"
	listenTechnicalEndpoint := ":8081"
	existingClusterName := "cluster1test"
	valid := true
	validationErrors := make(map[string][]error)
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", listen, listenTechnicalEndpoint, "client1", []string{existingClusterName})
	yamlConfig.ListenPortsLogicalValidator(&valid, &validationErrors)

	assert.Len(t, validationErrors, 0, "Should not be errors")
	assert.True(t, valid, "Should be true")
}

func TestShouldNotPassListenPortsLogicalValidatorWhenPortsAreEqual(t *testing.T) {
	listen := "127.0.0.1:8080"
	listenTechnicalEndpoint := listen
	existingClusterName := "cluster1test"
	valid := true
	validationErrors := make(map[string][]error)
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", listen, listenTechnicalEndpoint, "client1", []string{existingClusterName})
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
