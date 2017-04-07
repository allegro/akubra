package config

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	set "github.com/deckarep/golang-set"
)

// NoEmptyValuesInSliceValidator for strings in slice
func NoEmptyValuesInSliceValidator(v interface{}, param string) error {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Slice {
		for i := 0; i < val.Len(); i++ {
			e := val.Index(i)
			switch e.Kind() {
			case reflect.String:
				val := strings.TrimSpace(e.String())
				if len(val) == 0 {
					return fmt.Errorf("NoEmptyValuesInSliceValidator: Empty value in parameter: %q", param)
				}
			default:
				return fmt.Errorf("NoEmptyValuesInSliceValidator: Invalid Kind: %v in parameter: %q. Only kind 'String' is supported", e.Kind(), param)
			}
		}
	} else {
		return errors.New("NoEmptyValuesInSliceValidator: validates only Slice kind")
	}
	return nil
}

// UniqueValuesInSliceValidator for strings in slice
func UniqueValuesInSliceValidator(v interface{}, param string) error {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Slice {
		vals := []string{}
		uniqueVals := set.NewSet()
		for i := 0; i < val.Len(); i++ {
			e := val.Index(i)
			switch e.Kind() {
			case reflect.String:
				val := e.String()
				vals = append(vals, val)
				uniqueVals.Add(val)
			default:
				return fmt.Errorf("UniqueValuesInSliceValidator: Invalid Kind: %v in parameter: %q. Only kind 'String' is supported", e.Kind(), param)
			}
		}
		if len(vals) != uniqueVals.Cardinality() {
			return fmt.Errorf("UniqueValuesInSliceValidator: Duplicated values detected in parameter: %q", param)
		}
	} else {
		return errors.New("UniqueValuesInSliceValidator: validates only Slice kind")
	}
	return nil
}

// ClientClustersEntryLogicalValidator validate Client->Clusters entry and make sure that all required clusters are defined
func (c *YamlConfig) ClientClustersEntryLogicalValidator(valid *bool, validationErrors *map[string][]error) {
	errorsList := make(map[string][]error)

	if len(c.Client.Clusters) == 0 {
		*valid = false
		errorDetail := []error{errors.New("Empty clusters definition")}
		errorsList["ClientClustersEntryLogicalValidator"] = errorDetail
	} else {
		for _, clusterName := range c.Client.Clusters {
			_, exists := c.Clusters[clusterName]
			if !exists {
				*valid = false
				errorDetail := []error{fmt.Errorf("Undefined cluster: %q - not all required clusters are defined", clusterName)}
				errorsList["ClientClustersEntryLogicalValidator"] = errorDetail
			}
		}
	}
	*validationErrors = mergeErrors(*validationErrors, errorsList)
}

// ListenPortsLogicalValidator make sure that listen port and technical listen port are not equal
func (c *YamlConfig) ListenPortsLogicalValidator(valid *bool, validationErrors *map[string][]error) {
	errorsList := make(map[string][]error)
	listenParts := strings.Split(c.Listen, ":")
	listenTechnicalParts := strings.Split(c.TechnicalEndpointListen, ":")

	if listenParts[0] == listenTechnicalParts[0] && listenParts[1] == listenTechnicalParts[1] {
		*valid = false
		errorDetail := []error{errors.New("Listen and TechnicalEndpointListen has the same port")}
		errorsList["ListenPortsLogicalValidator"] = errorDetail
	}
	*validationErrors = mergeErrors(*validationErrors, errorsList)
}

func mergeErrors(maps ...map[string][]error) (output map[string][]error) {
	size := len(maps)
	if size == 0 {
		return output
	}
	if size == 1 {
		return maps[0]
	}
	output = make(map[string][]error)
	for _, m := range maps {
		for k, v := range m {
			output[k] = v
		}
	}
	return output
}
