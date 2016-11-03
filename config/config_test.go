package config

import (
	"testing"

	"github.com/go-yaml/yaml"
)

type TestYaml struct {
	Field YAMLURL
}

func TestYAMLURLParsingSuccessful(t *testing.T) {
	correct := []byte(`field: http://golang.org:80/pkg/net`)
	testyaml := TestYaml{}
	err := yaml.Unmarshal(correct, &testyaml)
	if err != nil {
		t.Error(err.Error())
	}
}

func TestYAMLURLParsingFailure(t *testing.T) {
	incorrect := []byte(`field: golang.org:80/pkg/net`)
	testyaml := TestYaml{}
	err := yaml.Unmarshal(incorrect, &testyaml)
	if err == nil {
		t.Errorf("Missing protocol should return error")
	}
}

func TestYAMLURLParsingEmpty(t *testing.T) {
	incorrect := []byte(`field:`)
	testyaml := TestYaml{}
	err := yaml.Unmarshal(incorrect, &testyaml)
	if err != nil {
		t.Errorf("Should not even try to parse")
	}
	if testyaml.Field.URL != nil {
		t.Errorf("Should be nil")
	}
}
