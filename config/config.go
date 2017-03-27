package config

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"

	"strings"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/metrics"
	set "github.com/deckarep/golang-set"
	"github.com/go-validator/validator"
	yaml "gopkg.in/yaml.v2"
)

// ClusterConfig defines cluster configuration
type ClusterConfig struct {
	// Backends should contain s3 backend urls
	Backends []YAMLUrl `yaml:"Backends,omitempty"`
	// Type, currently replicator is only option
	Type string `yaml:"Type,omitempty"`
	// Points how much load cluster should handle
	Weight int `yaml:"Weight,omitempty"`
	// Cluster type specific options
	Options map[string]string `yaml:"Options,omitempty"`
}

// ClientConfig keeps information about client setup
type ClientConfig struct {
	// Client name
	Name string `yaml:"Name,omitempty" validate:"regexp=^([a-zA-Z0-9_-]+)$"`
	// List of clusters name
	Clusters []string `yaml:"Clusters,omitempty" validate:"NoEmptyValuesSlice=Clusters,UniqueValuesSlice=Clusters"`
}

// YAMLUrl type fields in yaml configuration will parse urls
type YAMLUrl struct {
	*url.URL
}

// SyncLogMethod type fields in yaml configuration will parse list of HTTP methods
type SyncLogMethod struct {
	method string
}

// AdditionalHeaders type fields in yaml configuration will parse list of special headers
type AdditionalHeaders map[string]string

// YamlConfig contains configuration fields of config file
type YamlConfig struct {
	// Listen interface and port e.g. "0.0.0.0:8000", "127.0.0.1:9090", ":80"
	Listen string `yaml:"Listen,omitempty" validate:"regexp=^(([0-9]+[.][0-9]+[.][0-9]+[.][0-9]+)?[:][0-9]+)$"`
	// List of backend URI's e.g. "http://s3.mydatacenter.org"
	Backends []YAMLUrl `yaml:"Backends,omitempty,flow"`
	// Maximum accepted body size
	BodyMaxSize string `yaml:"BodyMaxSize,omitempty" validate:"regexp=^([1-9][0-9]+[kMG][B])$"`
	// MaxIdleConns see: https://golang.org/pkg/net/http/#Transport
	// Default 0 (no limit)
	MaxIdleConns int `yaml:"MaxIdleConns" validate:"min=0"`
	// MaxIdleConnsPerHost see: https://golang.org/pkg/net/http/#Transport
	// Default 100
	MaxIdleConnsPerHost int `yaml:"MaxIdleConnsPerHost" validate:"min=1"`
	// IdleConnTimeout see: https://golang.org/pkg/net/http/#Transport
	// Default 0 (no limit)
	IdleConnTimeout metrics.Interval `yaml:"IdleConnTimeout" validate:"regexp=^([1-9][0-9]*s)$"`
	// ResponseHeaderTimeout see: https://golang.org/pkg/net/http/#Transport
	// Default 5s (no limit)
	ResponseHeaderTimeout metrics.Interval `yaml:"ResponseHeaderTimeout" validate:"regexp=^([1-9][0-9]*s)$"`

	Clusters map[string]ClusterConfig `yaml:"Clusters,omitempty"`
	// Additional not amazon specific headers proxy will add to original request
	AdditionalRequestHeaders AdditionalHeaders `yaml:"AdditionalRequestHeaders,omitempty"`
	// Additional headers added to backend response
	AdditionalResponseHeaders AdditionalHeaders `yaml:"AdditionalResponseHeaders,omitempty"`
	// Read timeout on outgoing connections

	// Backend in maintenance mode. Akubra will not send data there
	MaintainedBackends []YAMLUrl `yaml:"MaintainedBackends,omitempty"`

	// List request methods to be logged in synclog in case of backend failure
	SyncLogMethods []SyncLogMethod `yaml:"SyncLogMethods,omitempty"`
	Client         *ClientConfig   `yaml:"Client,omitempty"`
	Logging        LoggingConfig   `yaml:"Logging,omitempty"`
	Metrics        metrics.Config  `yaml:"Metrics,omitempty"`
	// Should we keep alive connections with backend servers
	DisableKeepAlives bool `yaml:"DisableKeepAlives"`
}

// LoggingConfig contains Loggers configuration
type LoggingConfig struct {
	Accesslog      log.LoggerConfig `yaml:"Accesslog,omitempty"`
	Synclog        log.LoggerConfig `yaml:"Synclog,omitempty"`
	Mainlog        log.LoggerConfig `yaml:"Mainlog,omitempty"`
	ClusterSyncLog log.LoggerConfig `yaml:"ClusterSynclog,omitempty"`
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

// UnmarshalYAML for YAMLUrl
func (j *YAMLUrl) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	url, err := url.Parse(s)
	if url.Host == "" {
		return fmt.Errorf("url should match proto://host[:port]/path scheme - got %q", s)
	}
	j.URL = url
	return err
}

// UnmarshalYAML for SyncLogMethod
func (j *SyncLogMethod) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	method := fmt.Sprintf("%v", s)
	switch method {
	case "GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS":
		break
	default:
		return fmt.Errorf("Sync log method should be one from [GET, POST, DELETE, HEAD, OPTIONS] - got %q", s)
	}
	j.method = method
	return nil
}

// UnmarshalYAML for AdditionalHeaders
func (j *AdditionalHeaders) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var headers map[string]string
	if err := unmarshal(&headers); err != nil {
		return err
	}

	for key, value := range headers {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("Empty additional header key: %q with value: %q", key, value)
		}
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("Empty additional header value: %q with key: %q", value, key)
		}
	}
	return nil
}

// Parse json config
func parseConf(file io.Reader) (YamlConfig, error) {
	rc := YamlConfig{}
	bs, err := ioutil.ReadAll(file)
	if err != nil {
		return rc, err
	}
	err = yaml.Unmarshal(bs, &rc)
	if err != nil {
		return rc, err
	}
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
			conf.SyncLogMethodsSet.Add(v)
		}
	} else {
		conf.SyncLogMethodsSet = set.NewThreadUnsafeSetFromSlice(methods)
	}
}

// ValidateConf validate configuration from YAML file
func ValidateConf(conf YamlConfig) (bool, map[string][]error) {
	validator.SetValidationFunc("NoEmptyValuesSlice", NoEmptyValuesInSliceValidator)
	validator.SetValidationFunc("UniqueValuesSlice", UniqueValuesInSliceValidator)
	valid, validationErrors := validator.Validate(conf)
	for propertyName, validatorMessage := range validationErrors {
		log.Printf("[ ERROR ] YAML config validation -> propertyName: '%s', validatorMessage: '%s'\n", propertyName, validatorMessage)
	}
	return valid, validationErrors
}
