package config

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/allegro/akubra/metrics"
	units "github.com/docker/go-units"
)

// Server struct handles basic http server parameters
type Server struct {
	// Maximum accepted body size
	BodyMaxSize HumanSizeUnits `yaml:"BodyMaxSize,omitempty"`
	// Max number of incoming requests to process in parallel
	MaxConcurrentRequests int32 `yaml:"MaxConcurrentRequests" validate:"min=1"`
}

// AdditionalHeaders type fields in yaml configuration will parse list of special headers
type AdditionalHeaders map[string]string

// UnmarshalYAML for AdditionalHeaders
func (ah *AdditionalHeaders) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var headers map[string]string
	if err := unmarshal(&headers); err != nil {
		return err
	}
	for key, value := range headers {

		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("Empty additional header with value: %q", value)
		}
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("Empty additional header with key: %q", key)
		}

	}
	*ah = AdditionalHeaders(headers)
	return nil

}

// Client keeps backend client configuration
type Client struct {
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
	// Additional not AWS S3 specific headers proxy will add to original request
	AdditionalRequestHeaders AdditionalHeaders `yaml:"AdditionalRequestHeaders,omitempty"`
	// Additional headers inseted to client response
	AdditionalResponseHeaders AdditionalHeaders `yaml:"AdditionalRequestHeaders,omitempty"`
}

// HumanSizeUnits type for max. payload body size in bytes
type HumanSizeUnits struct {
	SizeInBytes int64
}

// UnmarshalYAML for HumanSizeUnits
func (hsu *HumanSizeUnits) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var size string
	if err := unmarshal(&size); err != nil {
		return err
	}
	value, err := units.FromHumanSize(size)
	if err != nil {
		return fmt.Errorf("Unable to parse BodyMaxSize: %s" + err.Error())
	}
	if value < 1 {
		return errors.New("BodyMaxSize must be greater than zero")
	}
	hsu.SizeInBytes = value
	return nil
}

// RequestHeaderContentLengthValidator for Content-Length header in request
func RequestHeaderContentLengthValidator(req http.Request, bodyMaxSize int64) int {
	var contentLength int64
	contentLengthHeader := req.Header.Get("Content-Length")
	if contentLengthHeader != "" {
		var err error
		contentLength, err = strconv.ParseInt(contentLengthHeader, 10, 64)
		if err != nil {
			return http.StatusBadRequest
		}
	}
	if contentLength > bodyMaxSize || req.ContentLength > bodyMaxSize {
		return http.StatusRequestEntityTooLarge
	}
	return 0
}
