package config

import (
	"fmt"
	"regexp"

	"github.com/allegro/akubra/metrics"
)

// ClientTransportProperties details
type ClientTransportProperties struct {
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

// Transport properties
type Transport struct {
	Name          string               `yaml:"Name"`
	Rules         ClientTransportRules `yaml:"Rules"`
	CompiledRules CompiledRules
	Properties    ClientTransportProperties `yaml:"Properties"`
}

// Transports map with Transport
type Transports []Transport

// compileRule
func (t *Transport) compileRule(regexpRule string) (compiledRule *regexp.Regexp, err error) {
	if len(regexpRule) > 0 {
		compiledRule, err = regexp.Compile(regexpRule)
	}
	return
}

// transportFlags for internal matching func
type transportFlags struct {
	declared bool
	matched  bool
	empty    bool
}

// compileRules prepares precompiled regular expressions for rules
func (t *Transport) compileRules() error {
	if t.CompiledRules.IsCompiled {
		return nil
	}
	var err error
	if len(t.Rules.Method) > 0 {
		t.CompiledRules.MethodRegexp, err = t.compileRule(t.Rules.Method)
		if err != nil {
			return fmt.Errorf("compileRule for Client->Transport->Trigger->Method error: %q", err)
		}
	}
	if len(t.Rules.Path) > 0 {
		t.CompiledRules.PathRegexp, err = t.compileRule(t.Rules.Path)
		if err != nil {
			return fmt.Errorf("compileRule for Client->Transport->Trigger->Path error: %q", err)
		}
	}
	if len(t.Rules.QueryParam) > 0 {
		t.CompiledRules.QueryParamRegexp, err = t.compileRule(t.Rules.QueryParam)
		if err != nil {
			return fmt.Errorf("compileRule for Client->Transport->Trigger->QueryParam error: %q", err)
		}
	}
	t.CompiledRules.IsCompiled = true

	return nil
}

// GetMatchedTransport returns first details matching with rules from Rules by arguments: method, path, queryParam
func (t *Transports) GetMatchedTransport(method, path, queryParam string) (matchedTransport Transport, ok bool) {
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

// matchTransportFlags matches method, path and query for Transport
func matchTransportFlags(transport Transport, method, path, queryParam string) (transportFlags, transportFlags, transportFlags) {
	var methodFlag, pathFlag, queryParamFlag transportFlags

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