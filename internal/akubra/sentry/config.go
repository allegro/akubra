package sentry

import "github.com/allegro/akubra/internal/akubra/metrics"

type Config struct {
	Dsn     string           `yaml:"Dsn,omitempty"`
	Timeout metrics.Interval `yaml:"Timeout,omitempty"`
}
