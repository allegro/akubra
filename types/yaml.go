package types

import (
	"errors"
	"fmt"
	"net/url"

	units "github.com/docker/go-units"
)

// YAMLUrl type fields in yaml configuration will parse urls
type YAMLUrl struct {
	*url.URL
}

// UnmarshalYAML for YAMLUrl
func (yurl *YAMLUrl) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	url, err := url.Parse(s)
	if url.Host == "" {
		return fmt.Errorf("url should match proto://host[:port]/path scheme - got %q", s)
	}
	yurl.URL = url
	return err
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
