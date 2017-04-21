package config

import "github.com/allegro/akubra/log"

// LoggingConfig contains Loggers configuration
type LoggingConfig struct {
	Accesslog      log.LoggerConfig `yaml:"Accesslog,omitempty"`
	Synclog        log.LoggerConfig `yaml:"Synclog,omitempty"`
	Mainlog        log.LoggerConfig `yaml:"Mainlog,omitempty"`
	ClusterSyncLog log.LoggerConfig `yaml:"ClusterSynclog,omitempty"`
}
