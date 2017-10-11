package config

import (
	"fmt"
	"strings"

	shardingconfig "github.com/allegro/akubra/sharding/config"
)

// Config is set of properties used in httpHandler
type Config struct {
	// Maximum accepted body size
	BodyMaxSize shardingconfig.HumanSizeUnits `yaml:"BodyMaxSize,omitempty"`
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
