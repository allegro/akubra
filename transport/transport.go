package transport

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	httphandlerConfig "github.com/allegro/akubra/httphandler/config"
	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/transport/config"
)

const (
	defaultMaxIdleConnsPerHost = 100
	defaultDialTimeout         = time.Second
)

// Matcher mapping initialized Transports with http.RoundTripper by transport name
type Matcher struct {
	RoundTrippers    map[string]http.RoundTripper
	TransportsConfig config.Transports
}

// SelectTransportDefinition returns transport instance by method, path and queryParams
func (m *Matcher) SelectTransportDefinition(method, path, queryParams string, log log.Logger) (matchedTransport config.TransportMatcherDefinition, err error) {
	matchedTransport, ok := m.TransportsConfig.GetMatchedTransportDefinition(method, path, queryParams)
	if !ok {
		errMsg := fmt.Sprintf("Transport not matched with args. method: %s, path: %s, queryParams: %s", method, path, queryParams)
		err = &DefinitionError{errors.New(errMsg)}
		log.Print(errMsg)
	}
	return
}

// RoundTrip for transport matching
func (m *Matcher) RoundTrip(request *http.Request) (response *http.Response, err error) {
	selectedRoundTriper, err := m.SelectTransportRoundTripper(request)
	if err == nil {
		return selectedRoundTriper.RoundTrip(request)
	}
	return
}

// SelectTransportRoundTripper for selecting RoundTripper by request object from transports matcher
func (m *Matcher) SelectTransportRoundTripper(request *http.Request) (selectedRoundTripper http.RoundTripper, err error) {
	selectedTransport, err := m.SelectTransportDefinition(request.Method, request.URL.Path, request.URL.RawQuery, log.DefaultLogger)
	if len(selectedTransport.Name) > 0 {
		reqID := request.Context().Value(log.ContextreqIDKey)
		log.Debugf("Request %s - selected transport name: %s (by method: %s, path: %s, queryParams: %s)",
			reqID, selectedTransport.Name, request.Method, request.URL.Path, request.URL.RawQuery)
		selectedRoundTripper = m.RoundTrippers[selectedTransport.Name]
	}

	return
}

// ConfigureHTTPTransports returns RoundTrippers mapped by transport name from configuration
func ConfigureHTTPTransports(clientConf httphandlerConfig.Client) (http.RoundTripper, error) {
	roundTrippers := make(map[string]http.RoundTripper)
	transportMatcher := &Matcher{TransportsConfig: clientConf.Transports}
	maxIdleConnsPerHost := defaultMaxIdleConnsPerHost
	if len(clientConf.Transports) > 0 {
		for _, transport := range clientConf.Transports {
			roundTrippers[transport.Name] = perepareTransport(transport.Properties, clientConf, maxIdleConnsPerHost)
		}
		transportMatcher.RoundTrippers = roundTrippers
	} else {
		return nil, errors.New("Service->Server->Client->Transports config is empty")
	}

	return transportMatcher, nil
}

// DefinitionError properties for Transports
type DefinitionError struct {
	error
}

// perepareTransport with properties
func perepareTransport(properties config.ClientTransportProperties, clientConf httphandlerConfig.Client, maxIdleConnsPerHost int) http.RoundTripper {
	if properties.MaxIdleConnsPerHost != 0 {
		maxIdleConnsPerHost = properties.MaxIdleConnsPerHost
	}

	timeout := defaultDialTimeout
	if clientConf.DialTimeout.Duration > 0 {
		timeout = clientConf.DialTimeout.Duration
	}

	httpTransport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: timeout,
		}).DialContext,
		MaxIdleConns:          properties.MaxIdleConns,
		MaxIdleConnsPerHost:   maxIdleConnsPerHost,
		IdleConnTimeout:       properties.IdleConnTimeout.Duration,
		ResponseHeaderTimeout: properties.ResponseHeaderTimeout.Duration,
		DisableKeepAlives:     properties.DisableKeepAlives,
	}
	return httpTransport
}
