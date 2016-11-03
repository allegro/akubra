package config

import (
	"testing"

	"github.com/go-yaml/yaml"
)

type TestYaml struct {
	Field YAMLURL
}

func TestYAMLURLParsing(t *testing.T) {
	correct := []byte(`field: http://golang.org:80/pkg/net`)
	testyaml := TestYaml{}
	err := yaml.Unmarshal(correct, &testyaml)
	if err != nil {
		t.Error(err.Error())
	}
	incorrect := []byte(`field: golang.org:80/pkg/net`)
	testyaml2 := TestYaml{}
	err = yaml.Unmarshal(incorrect, &testyaml2)
	if err == nil {
		t.Errorf("Missing protocol should return error")
	}
}
