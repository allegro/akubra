package crdstore

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/go-cleanhttp"
	"github.com/hashicorp/vault/api"
	"github.com/stretchr/testify/assert"
)

type testHandler struct {
	expectedPath string
	statusToReturn int
	body []byte
}

func (handler *testHandler) ServeHTTP(respWriter http.ResponseWriter, request *http.Request) {
	if request.URL.Path == handler.expectedPath {
		respWriter.WriteHeader(handler.statusToReturn)
		respWriter.Write(handler.body)
		return
	}
	respWriter.WriteHeader(http.StatusBadRequest)
	respWriter.Write([]byte{})
}

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

	_, err := factory.create("test", props)
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
	os.Setenv("CREDS_BACKEND_VAULT_test-vault_token", "env-token-123")

	credsBackend, err := factory.create("test-vault", props)
	assert.NotNil(t, credsBackend)
	assert.Nil(t, err)

	os.Setenv("CREDS_BACKEND_VAULT_test-vault_token", oldEnv)
}

func TestShouldReturnErrorWhenVaultResponseIsInvalid(t *testing.T) {
	expectedAccess := "accessKeyTestStorage"
	expectedSecret := "secretKeyTestStorage"
	server := httptest.NewServer(&testHandler{
		statusToReturn: http.StatusOK,
		body: []byte(fmt.Sprintf(vaultResponseFormat, expectedAccess, expectedSecret)),
		expectedPath: "/v1/secret/data/akubra/testAccess/testStorage"})
	defer server.Close()

	vaultClient, err := api.NewClient(&api.Config{
		Address:    server.URL,
		Timeout:    time.Second * 1,
		MaxRetries: 1,
		HttpClient: &http.Client{Transport: cleanhttp.DefaultTransport(), Timeout: time.Second * 2},
	})
	assert.Nil(t, err)

	vault := vaultCredsBackend{
		vaultClient: vaultClient,
		pathPrefix:  "secret/data",
	}

	creds, err := vault.FetchCredentials("testAccess", "testStorage")
	assert.Nil(t, creds)
	assert.Contains(t, err.Error(), "Error making API request")
}

func TestShouldReturnErrorWhenResponseIsInvalid(t *testing.T) {
	server := httptest.NewServer(&testHandler{
		statusToReturn: http.StatusOK,
		body: []byte(`{ "text": "Some invalid json" }`),
		expectedPath: "/v1/secret/data/testAccess/testStorage"})
	defer server.Close()

	vaultClient, err := api.NewClient(&api.Config{
		Address:    server.URL,
		Timeout:    time.Second * 1,
		MaxRetries: 1,
		HttpClient: &http.Client{Transport: cleanhttp.DefaultTransport(), Timeout: time.Second * 2},
	})
	assert.Nil(t, err)

	vault := vaultCredsBackend{
		vaultClient: vaultClient,
		pathPrefix:  "secret/data",
	}

	creds, err := vault.FetchCredentials("testAccess", "testStorage")
	assert.Nil(t, creds)
	assert.Equal(t, err.Error(), fmt.Sprintf("invlid response for testAccess/testStorage"))
}

const vaultResponseFormat = `
{
  "request_id": "c107f9f8-940e-aa5e-8209-19626c4f1032",
  "lease_id": "",
  "renewable": false,
  "lease_duration": 0,
  "data": {
    "data": {
      "access": "%s",
      "secret": "%s"
    },
    "metadata": {
      "created_time": "2019-03-05T07:18:34.324038747Z",
      "deletion_time": "",
      "destroyed": false,
      "version": 1
    }
  },
  "wrap_info": null,
  "warnings": null,
  "auth": null
}
`