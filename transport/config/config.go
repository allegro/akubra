package config

import (
	"fmt"
	"regexp"

	"github.com/allegro/akubra/metrics"
)

// ClientTransportProperties details
type ClientTransportProperties struct {
	// MaxIdleConns see: https://golang.org/pkg/net/http/#Transport
	// Zero means no limit.
	MaxIdleConns int `yaml:"MaxIdleConns" validate:"min=0"`
	// MaxIdleConnsPerHost see: https://golang.org/pkg/net/http/#Transport
	// If zero, DefaultMaxIdleConnsPerHost is used.
	MaxIdleConnsPerHost int `yaml:"MaxIdleConnsPerHost" validate:"min=0"`
	// IdleConnTimeout see: https://golang.org/pkg/net/http/#Transport
	// Zero means no limit.
	IdleConnTimeout metrics.Interval `yaml:"IdleConnTimeout"`
	// ResponseHeaderTimeout see: https://golang.org/pkg/net/http/#Transport
	ResponseHeaderTimeout metrics.Interval `yaml:"ResponseHeaderTimeout"`
	// DisableKeepAlives see: https://golang.org/pkg/net/http/#Transport
	// Default false
	DisableKeepAlives bool `yaml:"DisableKeepAlives"`
}

// ClientTransportRules properties
type ClientTransportRules struct {
	Method     string `yaml:"Method" validate:"max=64"`
	Path       string `yaml:"Path" validate:"max=64"`
	QueryParam string `yaml:"QueryParam" validate:"max=64"`
}

// CompiledRules properties
type CompiledRules struct {
	MethodRegexp     *regexp.Regexp
	PathRegexp       *regexp.Regexp
	QueryParamRegexp *regexp.Regexp
	IsCompiled       bool
}

// TransportMatcherDefinition properties
type TransportMatcherDefinition struct {
	Name          string               `yaml:"Name"`
	Rules         ClientTransportRules `yaml:"Rules"`
	CompiledRules CompiledRules
	Properties    ClientTransportProperties `yaml:"Properties"`
}

// Transports map with TransportMatcherDefinition
type Transports []TransportMatcherDefinition

// compileRule
func (t *TransportMatcherDefinition) compileRule(regexpRule string) (compiledRule *regexp.Regexp, err error) {
	if len(regexpRule) > 0 {
		compiledRule, err = regexp.Compile(regexpRule)
	}
	return
}

// ruleFlags for internal matching func
type ruleFlags struct {
	declared bool
	matched  bool
	empty    bool
}

// compileRules prepares precompiled regular expressions for rules
func (t *TransportMatcherDefinition) compileRules() error {
	if t.CompiledRules.IsCompiled {
		return nil
	}
	var err error
	if len(t.Rules.Method) > 0 {
		t.CompiledRules.MethodRegexp, err = t.compileRule(t.Rules.Method)
		if err != nil {
			return fmt.Errorf("compileRule for Client->TransportMatcherDefinition->Trigger->Method error: %q", err)
		}
	}
	if len(t.Rules.Path) > 0 {
		t.CompiledRules.PathRegexp, err = t.compileRule(t.Rules.Path)
		if err != nil {
			return fmt.Errorf("compileRule for Client->TransportMatcherDefinition->Trigger->Path error: %q", err)
		}
	}
	if len(t.Rules.QueryParam) > 0 {
		t.CompiledRules.QueryParamRegexp, err = t.compileRule(t.Rules.QueryParam)
		if err != nil {
			return fmt.Errorf("compileRule for Client->TransportMatcherDefinition->Trigger->QueryParam error: %q", err)
		}
	}
	t.CompiledRules.IsCompiled = true

	return nil
}

// matchTransportFlags matches method, path and query for TransportMatcherDefinition
func matchTransportFlags(transport TransportMatcherDefinition, method, path, queryParam string) (ruleFlags, ruleFlags, ruleFlags) {
	var methodFlag, pathFlag, queryParamFlag ruleFlags

	methodFlag.declared = len(transport.Rules.Method) > 0
	pathFlag.declared = len(transport.Rules.Path) > 0
	queryParamFlag.declared = len(transport.Rules.QueryParam) > 0

	if methodFlag.declared {
		methodFlag.matched = transport.CompiledRules.MethodRegexp.MatchString(method)
	} else {
		methodFlag.empty = true
		methodFlag.matched = true
	}
	if pathFlag.declared {
		pathFlag.matched = transport.CompiledRules.PathRegexp.MatchString(path)
	} else {
		pathFlag.empty = true
		pathFlag.matched = true
	}
	if queryParamFlag.declared {
		queryParamFlag.matched = transport.CompiledRules.QueryParamRegexp.MatchString(queryParam)
	} else {
		queryParamFlag.empty = true
		queryParamFlag.matched = true
	}
	return methodFlag, pathFlag, queryParamFlag
}

// GetMatchedTransportDefinition returns first details matching with rules from Rules by arguments: method, path, queryParam
func (t *Transports) GetMatchedTransportDefinition(method, path, queryParam string) (matchedTransport TransportMatcherDefinition, ok bool) {
	var matchedTransportName string
	for _, transport := range *t {
		err := transport.compileRules()
		if err != nil {
			return matchedTransport, false
		}
		methodFlag, pathFlag, queryParamFlag := matchTransportFlags(transport, method, path, queryParam)

		if methodFlag.matched && pathFlag.matched && queryParamFlag.matched {
			return transport, true
		}
		if methodFlag.empty && pathFlag.empty && queryParamFlag.empty && len(matchedTransportName) == 0 {
			matchedTransport = transport
		}
		matchedTransportName = transport.Name
	}
	return
}
