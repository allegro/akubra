package types

import (
	"testing"

	"net/url"

	"fmt"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

// TestYaml type for tests
type TestYaml struct {
	URL YAMLUrl `yaml:"URL"`
}

// unmareshal func for tests
var unmareshal = func(interface{}) error {
	var err error
	return err
}

func TestYAMLUrlParsingFailure(t *testing.T) {
	testyaml := TestYaml{URL: YAMLUrl{&url.URL{}}}
	err := testyaml.URL.UnmarshalYAML(unmareshal)
	assert.Equal(t, err.Error(), "url should match proto://host[:port]/path scheme - got \"\"")
}

func TestYAMLUrlParsingSuccessful(t *testing.T) {
	expectedURL := "http://golang.org:80/pkg/net"
	correct := []byte(`URL: ` + expectedURL)
	testyaml := TestYaml{URL: YAMLUrl{}}
	err := yaml.Unmarshal(correct, &testyaml)
	assert.Nil(t, err)
	assert.Equal(t, expectedURL, fmt.Sprintf("%s", testyaml.URL))
}
