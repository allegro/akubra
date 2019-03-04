package crdstore

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)


func TestShouldFailWhenAnyOfTheRequriedPropertiesAreMissing(t *testing.T) {
	factory := vaultCredsBackendFactory{}
	for _, missingProp := range requiredVaultProps {
		props := make(map[string]string)
		for _, prop := range requiredVaultProps {
			if prop == missingProp {
				continue
			}
			props[prop] = "test"
		}
		_, err := factory.create("test-vault", props)
		assert.Equal(t, err, fmt.Errorf("property '%s' is requried to instantiate vault client", missingProp))
	}
}

func TestShouldFailWhenVaultTokenIsNotProvidedNeitherAsPropertyNorAsEnvVariable(t *testing.T) {
	factory := vaultCredsBackendFactory{}
	props := make(map[string]string)
	for _, prop := range requiredVaultProps {
		props[prop] = "test"
	}
	oldEnv := os.Getenv("CREDS_BACKEND_VAULT_test_token")
	os.Setenv("CREDS_BACKEND_VAULT_test_token", "")

	_, err := factory.create("test",props)
	assert.Equal(t, err.Error(), "no vault token provided")
	os.Setenv("CREDS_BACKEND_VAULT_test_token", oldEnv)
}

func TestShouldUseTokenFromPropertiesIfDefined(t *testing.T) {
	factory := vaultCredsBackendFactory{}
	props := make(map[string]string)
	props["Token"] = "123"
	props["Timeout"] = "1000ms"
	props["MaxRetries"] = "3"
	props["Endpoint"] = "http://localhost"
	props["PathPrefix"] = "/secret"

	credsBackend, err := factory.create("test-vault", props)
	assert.NotNil(t, credsBackend)
	assert.Nil(t, err)
}

func TestShouldUseTokenFromEnvVariableIfItIsMissingInProps(t *testing.T) {
	factory := vaultCredsBackendFactory{}
	props := make(map[string]string)
	props["Timeout"] = "1000ms"
	props["MaxRetries"] = "3"
	props["Endpoint"] = "http://localhost"
	props["PathPrefix"] = "/secret"

	oldEnv := os.Getenv("CREDS_BACKEND_VAULT_test-valut_token")
	os.Setenv("CREDS_BACKEND_VAULT_test-valut_token", "env-token-123")

	credsBackend, err := factory.create("test-vault", props)
	assert.NotNil(t, credsBackend)
	assert.Nil(t, err)

	os.Setenv("CREDS_BACKEND_VAULT_test-valut_token", oldEnv)
}
