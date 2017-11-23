package config

import (
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"fmt"

	httphandler "github.com/allegro/akubra/httphandler/config"

	crdstoreconfig "github.com/allegro/akubra/crdstore/config"
	"github.com/allegro/akubra/log"
	logconfig "github.com/allegro/akubra/log/config"
	"github.com/allegro/akubra/metrics"
	confregions "github.com/allegro/akubra/regions/config"
	storages "github.com/allegro/akubra/storages/config"
	"github.com/go-validator/validator"
	yaml "gopkg.in/yaml.v2"
)

// TechnicalEndpointBodyMaxSize for /configuration/validate endpoint
const TechnicalEndpointBodyMaxSize = 8 * 1024

// TechnicalEndpointHeaderContentType for /configuration/validate endpoint
const TechnicalEndpointHeaderContentType = "application/yaml"

// YamlConfig contains configuration fields of config file
type YamlConfig struct {
	Service          httphandler.Service                `yaml:"Service,omitempty"`
	Backends         storages.BackendsMap               `yaml:"Backends,omitempty"`
	Clusters         storages.ClustersMap               `yaml:"Clusters,omitempty"`
	Regions          confregions.Regions                `yaml:"Regions,omitempty"`
	CredentialsStore crdstoreconfig.CredentialsStoreMap `yaml:"CredentialsStore,omitempty"`
	Logging          logconfig.LoggingConfig            `yaml:"Logging,omitempty"`
	Metrics          metrics.Config                     `yaml:"Metrics,omitempty"`
}

// Config contains processed YamlConfig data
type Config struct {
	YamlConfig
}

// Parse json config
func parseConf(file io.Reader) (YamlConfig, error) {

	bs, err := ioutil.ReadAll(file)
	if err != nil {
		return YamlConfig{}, err
	}
	rc := YamlConfig{}
	err = yaml.Unmarshal(bs, &rc)
	return rc, err
}

// Configure parse configuration file
func Configure(configFilePath string) (conf Config, err error) {
	confFile, err := os.Open(configFilePath)
	if err != nil {
		log.Fatalf("[ ERROR ] Problem with opening config file: '%s' - err: %v !", configFilePath, err)
		return conf, err
	}
	defer confFile.Close()
	yconf, err := parseConf(confFile)
	if err != nil {
		log.Fatalf("[ ERROR ] Problem with parsing config file: '%s' - err: %v !", configFilePath, err)
		return conf, err
	}
	conf.YamlConfig = yconf
	return conf, err
}

// ValidateConf validate configuration from YAML file
func ValidateConf(conf YamlConfig, enableLogicalValidator bool) (bool, map[string][]error) {
	validator.SetValidationFunc("NoEmptyValuesSlice", NoEmptyValuesInSliceValidator)
	validator.SetValidationFunc("UniqueValuesSlice", UniqueValuesInSliceValidator)
	valid, validationErrors := validator.Validate(conf)

	if valid && enableLogicalValidator {
		validListenPorts, portsValidationErrors := conf.ListenPortsLogicalValidator()
		validRegionsEntries, regionsValidationErrors := conf.RegionsEntryLogicalValidator()
		valid = valid && validRegionsEntries && validListenPorts
		validationErrors = mergeErrors(validationErrors, portsValidationErrors, regionsValidationErrors)
	}

	for propertyName, validatorMessage := range validationErrors {
		log.Printf("[ ERROR ] YAML config validation -> propertyName: '%s', validatorMessage: '%s'\n", propertyName, validatorMessage)
	}
	return valid, validationErrors
}

// ValidateConfigurationHTTPHandler is used in technical HTTP endpoint for config file validation
func ValidateConfigurationHTTPHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	validationResult := httphandler.RequestHeaderContentLengthValidator(*r, TechnicalEndpointBodyMaxSize)
	if validationResult > 0 {
		w.WriteHeader(validationResult)
		return
	}

	validationResult = RequestHeaderContentTypeValidator(*r, TechnicalEndpointHeaderContentType)
	if validationResult > 0 {
		w.WriteHeader(validationResult)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, fmt.Sprintf("Request Body Read Error: %s\n", err))
		return
	}
	log.Debugf("%s", body)
	var yamlConfig YamlConfig
	err = yaml.Unmarshal(body, &yamlConfig)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, fmt.Sprintf("YAML Unmarshal Error: %s", err))
		return
	}
	defer r.Body.Close()

	valid, errs := ValidateConf(yamlConfig, true)
	if !valid {
		log.Println("YAML validation - by technical endpoint - errors:", errs)
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, fmt.Sprintf("%s", errs))
		return
	}
	log.Println("Configuration checked (by technical endpoint) - OK.")
	fmt.Fprintf(w, "Configuration checked - OK.")

	w.WriteHeader(http.StatusOK)
	return
}
