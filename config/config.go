package config

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"

	"github.com/allegro/akubra/log"
	set "github.com/deckarep/golang-set"
	"github.com/go-yaml/yaml"
)

// YamlConfig contains configuration fields of config file
type YamlConfig struct {
	// Listen interface and port e.g. "0:8000", "localhost:9090", ":80"
	Listen string `yaml:"Listen,omitempty"`
	// List of backend uri's e.g. "http:// s3.mydaracenter.org"
	Backends []YAMLURL `yaml:"Backends,omitempty,flow"`
	// Limit of outgoing connections. When limit is reached, akubra will omit external backend
	// with greatest number of stalled connections
	ConnLimit int64 `yaml:"ConnLimit,omitempty"`
	// Additional not amazon specific headers proxy will add to original request
	AdditionalRequestHeaders map[string]string `yaml:"AdditionalRequestHeaders,omitempty"`
	// Additional headers added to backend response
	AdditionalResponseHeaders map[string]string `yaml:"AdditionalResponseHeaders,omitempty"`
	// Read timeout on outgoing connections
	ConnectionTimeout string `yaml:"ConnectionTimeout,omitempty"`
	// Dial timeout on outgoing connections
	ConnectionDialTimeout string `yaml:"ConnectionDialTimeout,omitempty"`
	// Backend in maintenance mode. Akubra will not send data there
	MaintainedBackends []YAMLURL `yaml:"MaintainedBackends,omitempty"`
	// List request methods to be logged in synclog in case of backend failure
	SyncLogMethods []string `yaml:"SyncLogMethods,omitempty"`
	// Should we keep alive connections with backend servers
	KeepAlive bool                     `yaml:"KeepAlive"`
	Clusters  map[string]ClusterConfig `yaml:"Clusters,omitempty"`
	Client    *ClientConfig            `yaml:"Client,omitempty"`
	Logging   *LoggingConfig           `yaml:"Logging,omitempty"`
}

// LoggingConfig contains Loggers configuration
type LoggingConfig struct {
	Accesslog      log.LoggerConfig `yaml:"Logging,omitempty"`
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

// YAMLURL type fields in yaml configuration will parse urls
type YAMLURL struct {
	*url.URL
}

// UnmarshalYAML parses strings to url.URL
func (j *YAMLURL) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	url, err := url.Parse(s)
	if url.Host == "" {
		return fmt.Errorf("url should match proto://host[:port]/path scheme, got %q", s)
	}
	j.URL = url
	return err
}

// ClusterConfig defines cluster configuration
type ClusterConfig struct {
	// Backends should contain s3 backend urls
	Backends []YAMLURL `yaml:"Backends,omitempty"`
	// Type, currently replicator is only option
	Type string `yaml:"Type,omitempty"`
	// Points how much load cluster should handle
	Weight uint64 `yaml:"Weight,omitempty"`
	// Cluster type specific options
	Options map[string]string `yaml:"Options,omitempty"`
}

// ClientConfig keeps information about client setup
type ClientConfig struct {
	// Client name
	Name string `yaml:"Name,omitempty"`
	// List of clusters name
	Clusters []string `yaml:"Clusters,omitempty"`
	// Total number of shards
	ShardsCount uint64 `yaml:"ShardsCount,omitempty"`
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
		return
	}

	yconf, err := parseConf(confFile)
	if err != nil {
		return
	}
	conf.YamlConfig = yconf

	if len(conf.SyncLogMethods) > 0 {
		conf.SyncLogMethodsSet = set.NewThreadUnsafeSet()
		for _, v := range conf.SyncLogMethods {
			conf.SyncLogMethodsSet.Add(v)
		}
	} else {
		conf.SyncLogMethodsSet = set.NewThreadUnsafeSetFromSlice(
			[]interface{}{"PUT", "GET", "HEAD", "DELETE", "OPTIONS"})
	}

	err = setupLoggers(&conf)
	return
}
