package config

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"net/http"
	"strconv"

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

//RegionsEntryLogicalValidator checks the correctness of "Regions" part of configuration file
func (c *YamlConfig) RegionsEntryLogicalValidator(valid *bool, validationErrors *map[string][]error) {
	errList := make([]error, 0)
	if len(c.Regions) == 0 {
		errList = append(errList, errors.New("Empty regions definition"))
	} else {
		for regionName, clusterDef := range c.Regions {
			if len(clusterDef.Clusters) == 0 {
				errList = append(errList, fmt.Errorf("No clusters defined for region \"%s\"", regionName))
			}
			for _, singleCluster := range clusterDef.Clusters {
				_, exists := c.Clusters[singleCluster.Cluster]
				if !exists {
					errList = append(errList, fmt.Errorf("Cluster \"%s\" is region \"%s\" is not defined", regionName, singleCluster.Cluster))
				}
				if singleCluster.Weight < 0 || singleCluster.Weight > 1 {
					errList = append(errList, fmt.Errorf("Weight for cluster \"%s\" in region \"%s\" is not valid", singleCluster.Cluster, regionName))
				}
			}
			if len(clusterDef.Domains) == 0 {
				errList = append(errList, fmt.Errorf("No domain defined for region \"%s\"", regionName))
			}
		}
	}
	if len(errList) > 0 {
		*valid = false
		errorsList := make(map[string][]error)
		errorsList["RegionsEntryLogicalValidator"] = errList
		*validationErrors = mergeErrors(*validationErrors, errorsList)
	} else {
		*valid = true
	}
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
	} else {
		*valid = true
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

// RequestHeaderContentLengthValidator for Content-Length header in request
func RequestHeaderContentLengthValidator(req http.Request, bodyMaxSize int64) int {
	var contentLength int64
	contentLengthHeader := req.Header.Get("Content-Length")
	if contentLengthHeader != "" {
		var err error
		contentLength, err = strconv.ParseInt(contentLengthHeader, 10, 64)
		if err != nil {
			return http.StatusBadRequest
		}
	}
	if contentLength > bodyMaxSize || req.ContentLength > bodyMaxSize {
		return http.StatusRequestEntityTooLarge
	}
	return 0
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
