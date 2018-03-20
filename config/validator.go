package config

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"net/http"

	confregions "github.com/allegro/akubra/regions/config"
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

func (c *YamlConfig) validateRegionCluster(regionName string, regionConf confregions.Region) []error {
	errList := make([]error, 0)
	if len(regionConf.Clusters) == 0 {
		errList = append(errList, fmt.Errorf("No clusters defined for region \"%s\"", regionName))
	}
	for _, regionCluster := range regionConf.Clusters {
		_, exists := c.Clusters[regionCluster.Name]
		if !exists {
			errList = append(errList, fmt.Errorf("Cluster \"%s\" is region \"%s\" is not defined", regionName, regionCluster.Name))
		}
		if regionCluster.Weight < 0 || regionCluster.Weight > 1 {
			errList = append(errList, fmt.Errorf("Weight for cluster \"%s\" in region \"%s\" is not valid", regionCluster.Name, regionName))
		}
	}
	if len(regionConf.Domains) == 0 {
		errList = append(errList, fmt.Errorf("No domain defined for region \"%s\"", regionName))
	}
	return errList
}

// RegionsEntryLogicalValidator checks the correctness of "Regions" part of configuration file
func (c *YamlConfig) RegionsEntryLogicalValidator() (valid bool, validationErrors map[string][]error) {
	errList := make([]error, 0)
	if len(c.Regions) == 0 {
		errList = append(errList, errors.New("Empty regions definition"))
	}
	for regionName, regionConf := range c.Regions {
		errList = append(errList, c.validateRegionCluster(regionName, regionConf)...)
	}
	validationErrors, valid = prepareErrors(errList, "RegionsEntryLogicalValidator")
	return
}

// TransportsEntryLogicalValidator checks the correctness of "Transports" part of configuration file
func (c *YamlConfig) TransportsEntryLogicalValidator() (valid bool, validationErrors map[string][]error) {
	errList := make([]error, 0)
	if len(c.Service.Client.Transports) == 0 {
		errList = append(errList, errors.New("Empty transports definition"))
	} else {
		for _, transportConf := range c.Service.Client.Transports {
			properties := transportConf.Properties
			if properties.MaxIdleConns < 0 || properties.MaxIdleConnsPerHost < 0 || properties.ResponseHeaderTimeout.Duration <= 0 || properties.IdleConnTimeout.Duration < 0 {
				errList = append(errList, fmt.Errorf("Wrong or empty transport 'Properties' for 'Name': %s", transportConf.Name))
				break
			}
		}
	}
	validationErrors, valid = prepareErrors(errList, "TransportsEntryLogicalValidator")
	return
}

// ListenPortsLogicalValidator make sure that listen port and technical listen port are not equal
func (c *YamlConfig) ListenPortsLogicalValidator() (valid bool, validationErrors map[string][]error) {
	errorsList := make(map[string][]error)
	listenParts := strings.Split(c.Service.Server.Listen, ":")
	listenTechnicalParts := strings.Split(c.Service.Server.TechnicalEndpointListen, ":")
	valid = true
	if listenParts[0] == listenTechnicalParts[0] && listenParts[1] == listenTechnicalParts[1] {
		valid = false
		errorDetail := []error{errors.New("Listen and TechnicalEndpointListen has the same port")}
		errorsList["ListenPortsLogicalValidator"] = errorDetail
	}
	return valid, errorsList
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

// prepareErrors
func prepareErrors(errList []error, validatorName string) (validationErrors map[string][]error, valid bool) {
	if valid = len(errList) < 1; !valid {
		errorsList := make(map[string][]error)
		errorsList[validatorName] = errList
		validationErrors = mergeErrors(validationErrors, errorsList)
	}
	return
}

// RequestHeaderContentTypeValidator for Content-Type header in request
func RequestHeaderContentTypeValidator(req http.Request, requiredContentType string) int {
	contentTypeHeader := req.Header.Get("Content-Type")
	if contentTypeHeader == "" {
		return http.StatusBadRequest
	}
	if contentTypeHeader != requiredContentType {
		return http.StatusUnsupportedMediaType
	}
	return 0
}
