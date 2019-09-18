package config

import "github.com/allegro/akubra/log"

// LoggingConfig contains Loggers configuration
type LoggingConfig struct {
	Accesslog log.LoggerConfig `yaml:"Accesslog,omitempty"`
	Mainlog   log.LoggerConfig `yaml:"Mainlog,omitempty"`
}
