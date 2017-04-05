package config

import (
	"testing"

	shardingconfig "github.com/allegro/akubra/sharding/config"
	"github.com/go-validator/validator"
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

func TestShouldPassClientClustersEntryLogicalValidator(t *testing.T) {
	existingClusterName := "cluster1test"
	valid := true
	validationErrors := make(map[string][]error)
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", ":80", "client1", []string{existingClusterName})
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
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", ":80", "client1", []string{notExistingClusterName})
	yamlConfig.ClientClustersEntryLogicalValidator(&valid, &validationErrors)

	assert.Len(t, validationErrors, 1, "Should be one error")
	assert.False(t, valid, "Should be false")
}

func TestShouldNotPassClientClustersEntryLogicalValidatorWhenEmptyClustersDefinition(t *testing.T) {
	valid := true
	validationErrors := make(map[string][]error)
	var size shardingconfig.HumanSizeUnits
	size.SizeInBytes = 2048
	yamlConfig := PrepareYamlConfig(size, 31, 45, "127.0.0.1:81", ":80", "client1", []string{})
	yamlConfig.ClientClustersEntryLogicalValidator(&valid, &validationErrors)

	assert.Len(t, validationErrors, 1, "Should be one error")
	assert.False(t, valid, "Should be false")
}
