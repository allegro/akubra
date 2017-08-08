package config

import (
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"fmt"

	"github.com/allegro/akubra/log"
	logconfig "github.com/allegro/akubra/log/config"
	"github.com/allegro/akubra/metrics"
	shardingconfig "github.com/allegro/akubra/sharding/config"
	set "github.com/deckarep/golang-set"
	"github.com/go-validator/validator"
	yaml "gopkg.in/yaml.v2"
)

// TechnicalEndpointBodyMaxSize for /configuration/validate endpoint
const TechnicalEndpointBodyMaxSize = 8 * 1024

// TechnicalEndpointHeaderContentType for /configuration/validate endpoint
const TechnicalEndpointHeaderContentType = "application/yaml"

// YamlConfig contains configuration fields of config file
type YamlConfig struct {
	// Listen interface and port e.g. "0.0.0.0:8000", "127.0.0.1:9090", ":80"
	Listen                  string `yaml:"Listen,omitempty" validate:"regexp=^(([0-9]+[.][0-9]+[.][0-9]+[.][0-9]+)?[:][0-9]+)$"`
	TechnicalEndpointListen string `yaml:"TechnicalEndpointListen,omitempty" validate:"regexp=^(([0-9]+[.][0-9]+[.][0-9]+[.][0-9]+)?[:][0-9]+)$"`
	HealthCheckEndpoint     string `yaml:"HealthCheckEndpoint,omitempty" validate:"regexp=^([/a-z0-9]+)$"`
	// List of backend URI's e.g. "http://s3.mydatacenter.org"
	Backends []shardingconfig.YAMLUrl `yaml:"Backends,omitempty,flow"`
	// Maximum accepted body size
	BodyMaxSize shardingconfig.HumanSizeUnits `yaml:"BodyMaxSize,omitempty"`
	// MaxIdleConns see: https://golang.org/pkg/net/http/#Transport
	// Default 0 (no limit)
	MaxIdleConns int `yaml:"MaxIdleConns" validate:"min=0"`
	// MaxIdleConnsPerHost see: https://golang.org/pkg/net/http/#Transport
	// Default 100
	MaxIdleConnsPerHost int `yaml:"MaxIdleConnsPerHost" validate:"min=0"`
	// IdleConnTimeout see: https://golang.org/pkg/net/http/#Transport
	// Default 0 (no limit)
	IdleConnTimeout metrics.Interval `yaml:"IdleConnTimeout"`
	// ResponseHeaderTimeout see: https://golang.org/pkg/net/http/#Transport
	// Default 5s (no limit)
	ResponseHeaderTimeout metrics.Interval `yaml:"ResponseHeaderTimeout"`
	// Max number of incoming requests to process in parallel
	MaxConcurrentRequests int32 `yaml:"MaxConcurrentRequests" validate:"min=1"`

	Clusters map[string]shardingconfig.ClusterConfig `yaml:"Clusters,omitempty"`
	Regions  map[string]shardingconfig.RegionConfig  `yaml:"Regions,omitempty"`
	// Additional not amazon specific headers proxy will add to original request
	AdditionalRequestHeaders shardingconfig.AdditionalHeaders `yaml:"AdditionalRequestHeaders,omitempty"`
	// Additional headers added to backend response
	AdditionalResponseHeaders shardingconfig.AdditionalHeaders `yaml:"AdditionalResponseHeaders,omitempty"`
	// Read timeout on outgoing connections

	// Backend in maintenance mode. Akubra will not send data there
	MaintainedBackends []shardingconfig.YAMLUrl `yaml:"MaintainedBackends,omitempty"`

	// List request methods to be logged in synclog in case of backend failure
	SyncLogMethods []shardingconfig.SyncLogMethod `yaml:"SyncLogMethods,omitempty"`
	Logging        logconfig.LoggingConfig        `yaml:"Logging,omitempty"`
	Metrics        metrics.Config                 `yaml:"Metrics,omitempty"`
	// Should we keep alive connections with backend servers
	DisableKeepAlives bool `yaml:"DisableKeepAlives"`
}

// Config contains processed YamlConfig data
type Config struct {
	YamlConfig
	SyncLogMethodsSet set.Set
	Synclog           log.Logger
	Accesslog         log.Logger
	Mainlog           log.Logger
	ClusterSyncLog    log.Logger
}

// Parse json config
func parseConf(file io.Reader) (YamlConfig, error) {
	rc := YamlConfig{}
	bs, err := ioutil.ReadAll(file)
	if err != nil {
		return rc, err
	}
	err = yaml.Unmarshal(bs, &rc)
	return rc, err
}

func setupLoggers(conf *Config) (err error) {
	emptyLoggerConfig := log.LoggerConfig{}

	if conf.Logging.Accesslog == emptyLoggerConfig {
		conf.Logging.Accesslog = log.LoggerConfig{Syslog: "LOG_LOCAL0"}
	}

	conf.Accesslog, err = log.NewLogger(conf.Logging.Accesslog)

	if err != nil {
		return err
	}

	if conf.Logging.Synclog == emptyLoggerConfig {
		conf.Logging.Synclog = log.LoggerConfig{
			Syslog:    "LOG_LOCAL1",
			PlainText: true,
		}

	}
	conf.Synclog, err = log.NewLogger(conf.Logging.Synclog)

	if err != nil {
		return err
	}

	if conf.Logging.Mainlog == emptyLoggerConfig {
		conf.Logging.Mainlog = log.LoggerConfig{Syslog: "LOG_LOCAL2"}
	}

	conf.Mainlog, err = log.NewLogger(conf.Logging.Mainlog)
	log.DefaultLogger = conf.Mainlog
	if err != nil {
		return err
	}

	if conf.Logging.ClusterSyncLog == emptyLoggerConfig {
		conf.Logging.ClusterSyncLog = log.LoggerConfig{
			Syslog:    "LOG_LOCAL3",
			PlainText: true,
		}
	}

	conf.ClusterSyncLog, err = log.NewLogger(conf.Logging.ClusterSyncLog)

	return err
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

	setupSyncLogThread(&conf, []interface{}{"PUT", "GET", "HEAD", "DELETE", "OPTIONS"})

	err = setupLoggers(&conf)
	return conf, err
}

func setupSyncLogThread(conf *Config, methods []interface{}) {
	if len(conf.SyncLogMethods) > 0 {
		conf.SyncLogMethodsSet = set.NewThreadUnsafeSet()
		for _, v := range conf.SyncLogMethods {
			conf.SyncLogMethodsSet.Add(v.Method)
		}
	} else {
		conf.SyncLogMethodsSet = set.NewThreadUnsafeSetFromSlice(methods)
	}
}

// ValidateConf validate configuration from YAML file
func ValidateConf(conf YamlConfig, enableLogicalValidator bool) (bool, map[string][]error) {
	validator.SetValidationFunc("NoEmptyValuesSlice", NoEmptyValuesInSliceValidator)
	validator.SetValidationFunc("UniqueValuesSlice", UniqueValuesInSliceValidator)
	valid, validationErrors := validator.Validate(conf)
	if valid && enableLogicalValidator {
		var validListenPorts bool
		conf.RegionsEntryLogicalValidator(&valid, &validationErrors)
		conf.ListenPortsLogicalValidator(&validListenPorts, &validationErrors)
		valid = valid && validListenPorts
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

	validationResult := RequestHeaderContentLengthValidator(*r, TechnicalEndpointBodyMaxSize)
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
