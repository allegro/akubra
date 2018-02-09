package config

import (
	"github.com/allegro/akubra/metrics"
	"regexp"
	"errors"
)

// ClientTransportDetail properties
type ClientTransportDetail struct {
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
	// DisableKeepAlives see: https://golang.org/pkg/net/http/#Transport
	// Default false
	DisableKeepAlives bool `yaml:"DisableKeepAlives"`
}

// TriggersCompiledRules compiled rules
type TriggersCompiledRules struct {
	Method *regexp.Regexp
	Path       *regexp.Regexp
	QueryParam *regexp.Regexp
}

// ClientTransportTriggers properties
type ClientTransportTriggers struct {
	Method string `yaml:"Method" validate:"min=1"`
	Path       string `yaml:"Path" validate:"min=0"`
	QueryParam string `yaml:"QueryParam" validate:"min=0"`
	TriggersCompiledRules
}

// Transport properties
type Transport struct {
	Triggers        ClientTransportTriggers `yaml:"Triggers"`
	MergingStrategy string                  `yaml:"MergingStrategy" validate:"min=1"`
	Details         ClientTransportDetail   `yaml:"Details"`
}

// Transports map with Transport
type Transports map[byte]Transport

// Validate trigger
func (t *Transport) Validate() error {
	if len(t.Triggers.Method) == 0 {
		return errors.New("Method in Client->Transport->Trigger config is empty")
	}
	if len(t.Triggers.Path) == 0 {
		return errors.New("Path in Client->Transport->Trigger config is empty")
	}
	if len(t.Triggers.QueryParam) == 0 {
		return errors.New("QueryParam in Client->Transport->Trigger config is empty")
	}
	return nil
}

// compileRules preparing precompiled regular expressions for rules
func (t *Transport) compileRules() error {
	var err error
	t.Triggers.TriggersCompiledRules.Method, err = regexp.Compile(t.Triggers.Method)
	if err != nil {
		return err
	}
	t.Triggers.TriggersCompiledRules.Path, err = regexp.Compile(t.Triggers.Path)
	if err != nil {
		return err
	}
	t.Triggers.TriggersCompiledRules.QueryParam, err = regexp.Compile(t.Triggers.QueryParam)
	if err != nil {
		return err
	}
	return nil
}

// DetailsMatched verifying if all details matching with rules
func (t *Transport) DetailsMatched(method, path, queryParam string) bool {
	if t.Triggers.TriggersCompiledRules.Method == nil {
		t.compileRules()
	}
	methodMatched := t.Triggers.TriggersCompiledRules.Method.MatchString(method)
	pathMatched := t.Triggers.TriggersCompiledRules.Path.MatchString(path)
	queryMatched := t.Triggers.TriggersCompiledRules.QueryParam.MatchString(queryParam)
	return methodMatched && pathMatched && queryMatched
}
