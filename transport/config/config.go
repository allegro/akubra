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

// ClientTransportApplyRule properties
type ClientTransportApplyRule struct {
	Method     string `yaml:"Method" validate:"max=64"`
	Path       string `yaml:"Path" validate:"max=64"`
	QueryParam string `yaml:"QueryParam" validate:"max=64"`
}

// ApplyRuleCompiledRules properties
type ApplyRuleCompiledRules struct {
	MethodRegexp     *regexp.Regexp
	PathRegexp       *regexp.Regexp
	QueryParamRegexp *regexp.Regexp
	IsCompiled       bool
}

// Transport properties
type Transport struct {
	Name                   string                   `yaml:"Name"`
	ApplyRule              ClientTransportApplyRule `yaml:"ApplyRule"`
	ApplyRuleCompiledRules ApplyRuleCompiledRules
	Properties             ClientTransportProperties `yaml:"Properties"`
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
	if !t.ApplyRuleCompiledRules.IsCompiled {
		var err error
		if len(t.ApplyRule.Method) > 0 {
			t.ApplyRuleCompiledRules.MethodRegexp, err = t.compileRule(t.ApplyRule.Method)
			if err != nil {
				return fmt.Errorf("compileRule for Client->Transport->Trigger->Method error: %q", err)
			}
		}
		if len(t.ApplyRule.Path) > 0 {
			t.ApplyRuleCompiledRules.PathRegexp, err = t.compileRule(t.ApplyRule.Path)
			if err != nil {
				return fmt.Errorf("compileRule for Client->Transport->Trigger->Path error: %q", err)
			}
		}
		if len(t.ApplyRule.QueryParam) > 0 {
			t.ApplyRuleCompiledRules.QueryParamRegexp, err = t.compileRule(t.ApplyRule.QueryParam)
			if err != nil {
				return fmt.Errorf("compileRule for Client->Transport->Trigger->QueryParam error: %q", err)
			}
		}
		t.ApplyRuleCompiledRules.IsCompiled = true
	}
	return nil
}

// GetMatchedTransport returns first details matching with rules from ApplyRule by arguments: method, path, queryParam
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

	methodFlag.declared = len(transport.ApplyRule.Method) > 0
	pathFlag.declared = len(transport.ApplyRule.Path) > 0
	queryParamFlag.declared = len(transport.ApplyRule.QueryParam) > 0

	if methodFlag.declared {
		methodFlag.matched = transport.ApplyRuleCompiledRules.MethodRegexp.MatchString(method)
	} else {
		methodFlag.empty = true
		methodFlag.matched = true
	}
	if pathFlag.declared {
		pathFlag.matched = transport.ApplyRuleCompiledRules.PathRegexp.MatchString(path)
	} else {
		pathFlag.empty = true
		pathFlag.matched = true
	}
	if queryParamFlag.declared {
		queryParamFlag.matched = transport.ApplyRuleCompiledRules.QueryParamRegexp.MatchString(queryParam)
	} else {
		queryParamFlag.empty = true
		queryParamFlag.matched = true
	}
	return methodFlag, pathFlag, queryParamFlag
}
