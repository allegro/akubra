package config

import (
	"errors"
	"fmt"
	"github.com/allegro/akubra/metrics"
	"regexp"
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

// ClientTransportTriggers properties
type ClientTransportTriggers struct {
	Method     string `yaml:"Method" validate:"max=64"`
	Path       string `yaml:"Path" validate:"max=64"`
	QueryParam string `yaml:"QueryParam" validate:"max=64"`
}

// TriggersCompiledRules properties
type TriggersCompiledRules struct {
	MethodRegexp     *regexp.Regexp
	PathRegexp       *regexp.Regexp
	QueryParamRegexp *regexp.Regexp
	IsCompiled       bool
}

// Transport properties
type Transport struct {
	Triggers              ClientTransportTriggers `yaml:"Triggers"`
	TriggersCompiledRules TriggersCompiledRules
	MergingStrategy       string                `yaml:"MergingStrategy"`
	Details               ClientTransportDetail `yaml:"Details"`
}

// Transports map with Transport
type Transports map[string]Transport

// compileRule
func (t *Transport) compileRule(regexpRule string) (compiledRule *regexp.Regexp, err error) {
	if len(regexpRule) > 0 {
		compiledRule, err = regexp.Compile(regexpRule)
	}
	return
}

// compileRules preparing precompiled regular expressions for rules
func (t *Transport) compileRules() error {
	if !t.TriggersCompiledRules.IsCompiled {
		if len(t.Triggers.Method) > 0 {
			var err error
			t.TriggersCompiledRules.MethodRegexp, err = t.compileRule(t.Triggers.Method)
			if err != nil {
				return errors.New(fmt.Sprintf("compileRule for Client->Transport->Trigger->Method error: %q", err))
			}
		}
		if len(t.Triggers.Path) > 0 {
			var err error
			t.TriggersCompiledRules.PathRegexp, err = t.compileRule(t.Triggers.Path)
			if err != nil {
				return errors.New(fmt.Sprintf("compileRule for Client->Transport->Trigger->Path error: %q", err))
			}
		}
		if len(t.Triggers.QueryParam) > 0 {
			var err error
			t.TriggersCompiledRules.QueryParamRegexp, err = t.compileRule(t.Triggers.QueryParam)
			if err != nil {
				return errors.New(fmt.Sprintf("compileRule for Client->Transport->Trigger->QueryParam error: %q", err))
			}
		}
		t.TriggersCompiledRules.IsCompiled = true
	}
	return nil
}

// GetMatchedTransport return first details matching with rules from Triggers by arguments: method, path, queryParam
func (t *Transports) GetMatchedTransport(method, path, queryParam string) (Transport, string, bool) {
	var defaultTransport Transport
	var defaultTransportName string

	for transportName, transport := range *t {
		transport.compileRules()
		methodMatched, pathMatched, queryParamMatched := false, false, false
		methodEmpty, pathEmpty, queryParamEmpty := false, false, false
		methodIsDeclared, pathIsDeclared, queryIsDeclared :=
			len(transport.Triggers.Method) > 0, len(transport.Triggers.Path) > 0, len(transport.Triggers.QueryParam) > 0

		if methodIsDeclared {
			methodMatched = transport.TriggersCompiledRules.MethodRegexp.MatchString(method)
		} else {
			methodEmpty = true
			methodMatched = true
		}
		if pathIsDeclared {
			pathMatched = transport.TriggersCompiledRules.PathRegexp.MatchString(path)
		} else {
			pathEmpty = true
			pathMatched = true
		}
		if queryIsDeclared {
			queryParamMatched = transport.TriggersCompiledRules.QueryParamRegexp.MatchString(queryParam)
		} else {
			queryParamEmpty = true
			queryParamMatched = true
		}

		if methodMatched && pathMatched && queryParamMatched {
			return transport, transportName, true
		}
		if methodEmpty && pathEmpty && queryParamEmpty && len(defaultTransportName) == 0 {
			defaultTransport = transport
			defaultTransportName = transportName
		}
	}
	if defaultTransport.TriggersCompiledRules.IsCompiled && len(defaultTransportName) > 0 {
		return defaultTransport, defaultTransportName, true
	}

	return Transport{}, "", false
}
