package config

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/allegro/akubra/metrics"
	transport "github.com/allegro/akubra/transport/config"
	units "github.com/docker/go-units"
)

// Server struct handles basic http server parameters
type Server struct {
	// Maximum accepted body size
	BodyMaxSize HumanSizeUnits `yaml:"BodyMaxSize,omitempty"`
	// Max number of incoming requests to process in parallel
	MaxConcurrentRequests   int32  `yaml:"MaxConcurrentRequests" validate:"min=1"`
	Listen                  string `yaml:"Listen,omitempty" validate:"regexp=^(([0-9]+[.][0-9]+[.][0-9]+[.][0-9]+)?[:][0-9]+)$"`
	TechnicalEndpointListen string `yaml:"TechnicalEndpointListen,omitempty" validate:"regexp=^(([0-9]+[.][0-9]+[.][0-9]+[.][0-9]+)?[:][0-9]+)$"`
	HealthCheckEndpoint     string `yaml:"HealthCheckEndpoint,omitempty" validate:"regexp=^([/a-z0-9]+)$"`
	// ReadTimeout is client request max duration
	ReadTimeout metrics.Interval `yaml:"ReadTimeout" validate:"nonzero"`
	// WriteTimeout is server request max processing time
	WriteTimeout metrics.Interval `yaml:"WriteTimeout" validate:"nonzero"`
	// ShutdownTimeout is gracefull shoutdown duration limit
	ShutdownTimeout metrics.Interval `yaml:"ShutdownTimeout" validate:"nonzero"`
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
	// Additional not AWS S3 specific headers proxy will add to original request
	AdditionalRequestHeaders AdditionalHeaders `yaml:"AdditionalRequestHeaders,omitempty"`
	// Additional headers inseted to client response
	AdditionalResponseHeaders AdditionalHeaders `yaml:"AdditionalResponseHeaders,omitempty"`
	// Transports configuration
	Transports transport.Transports `yaml:"Transports,omitempty"`
	// DialTimeout limits wait period for connection dial
	DialTimeout metrics.Interval `yaml:"DialTimeout"`
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
	value, err := units.RAMInBytes(size)
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

// Service section
type Service struct {
	Server Server `yaml:"Server,omitempty"`
	Client Client `yaml:"Client,omitempty"`
}
