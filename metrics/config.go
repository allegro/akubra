package metrics

import (
	"fmt"
	"time"
)

// Interval is time.Duration wrapper
type Interval struct {
	time.Duration
}

// UnmarshalYAML implements
func (interval *Interval) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	duration, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("duration should match https://golang.org/pkg/time/#ParseDuration scheme, got %q", s)
	}
	interval.Duration = duration
	return err
}

// Config defines metrics publication details
type Config struct {
	// Target, possible values: "graphite", "expvar", "stdout"
	Target string `yaml:"Target,omitempty"`
	// Interval determines how often metrics should be released, applicable for "graphite" and "stdout"
	Interval Interval `yaml:"Interval,omitempty"`
	// Addr points graphite collector address
	Addr string `yaml:"Addr,omitempty"`
	// ExpAddr is expvar server adress
	ExpAddr string `yaml:"ExpAddr,omitempty"`
	// Prefix graphite metrics
	Prefix string `yaml:"Prefix,omitempty"`
	// AppendDefaults adds "<hostname>.<process>"  suffix
	AppendDefaults bool `yaml:"AppendDefaults,omitempty"`
	// Percentiles customizes metrics sent to graphite default: 0.75, 0.95, 0.99, 0.999
	Percentiles []float64 `yaml:"Percentiles"`
	// Debug includes runtime.MemStats metrics
	Debug bool `yaml:"Debug"`
}
