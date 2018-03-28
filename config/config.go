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
	"gopkg.in/validator.v1"
	"gopkg.in/yaml.v2"
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
	defer func() {
		err = confFile.Close()
		if err != nil {
			log.Debugf("Cannot close configuration, reason: %s", err)
		}
	}()
	yconf, err := parseConf(confFile)
	if err != nil {
		log.Fatalf("[ ERROR ] Problem with parsing config file: '%s' - err: %v !", configFilePath, err)
		return conf, err
	}
	conf.YamlConfig = yconf
	return conf, err
}

func logWriteHeaderErr(err error, when string) {
	if err != nil {
		log.Printf("Error while handling %s: %q", when, err)
	}
}

// ValidateConf validate configuration from YAML file
func ValidateConf(conf YamlConfig, enableLogicalValidator bool) (bool, map[string][]error) {
	err := validator.SetValidationFunc("NoEmptyValuesSlice", NoEmptyValuesInSliceValidator)
	if err != nil {
		return false, map[string][]error{"SetValidationFuncError": []error{err}}
	}
	err = validator.SetValidationFunc("UniqueValuesSlice", UniqueValuesInSliceValidator)
	if err != nil {
		return false, map[string][]error{"SetValidationFuncError": []error{err}}
	}

	valid, validationErrors := validator.Validate(conf)

	if valid && enableLogicalValidator {
		validListenPorts, portsValidationErrors := conf.ListenPortsLogicalValidator()
		validRegionsEntries, regionsValidationErrors := conf.RegionsEntryLogicalValidator()
		validTransportsEntries, transportsValidationErrors := conf.TransportsEntryLogicalValidator()
		valid = valid && validListenPorts && validRegionsEntries && validTransportsEntries
		validationErrors = mergeErrors(validationErrors, portsValidationErrors, regionsValidationErrors, transportsValidationErrors)
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
		_, ioerr := io.WriteString(w, fmt.Sprintf("Request Body Read Error: %s\n", err))
		logWriteHeaderErr(ioerr, "internal server error")
		return
	}

	var yamlConfig YamlConfig
	err = yaml.Unmarshal(body, &yamlConfig)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, ioerr := io.WriteString(w, fmt.Sprintf("YAML Unmarshal Error: %s", err))
		logWriteHeaderErr(ioerr, "bad request")
		return
	}

	defer func() {
		if err := r.Body.Close(); err != nil {
			log.Printf("Cannot close request body: %q\n", err)
		}
	}()

	valid, errs := ValidateConf(yamlConfig, true)
	if !valid {
		log.Println("YAML validation - by technical endpoint - errors:", errs)
		w.WriteHeader(http.StatusBadRequest)
		_, ioerr := io.WriteString(w, fmt.Sprintf("%s", errs))
		logWriteHeaderErr(ioerr, "validation bad request")
		return
	}
	log.Println("Configuration checked (by technical endpoint) - OK.")
	fmt.Fprintf(w, "Configuration checked - OK.")

	w.WriteHeader(http.StatusOK)
}
