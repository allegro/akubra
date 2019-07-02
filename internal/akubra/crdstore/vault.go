package crdstore

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/allegro/akubra/internal/akubra/metrics"
	"github.com/allegro/akubra/internal/akubra/config/vault"
	cleanhttp "github.com/hashicorp/go-cleanhttp"
	"github.com/hashicorp/vault/api"
	"github.com/pkg/errors"
)

const vaultTokenEnvVarFormat = "CREDS_BACKEND_VAULT_%s_token"
const vaultCredsFormat = "%s/%s/%s"

var requiredVaultProps = []string{"Endpoint", "Timeout", "MaxRetries", "PathPrefix"}

type vaultCredsBackendFactory struct {
	credentialsBackendFactory
}

type vaultCredsBackend struct {
	CredentialsBackend
	vaultClient *api.Client
	pathPrefix  string
	name        string
}

func (vaultFactory *vaultCredsBackendFactory) create(crdStoreName string, props map[string]string) (CredentialsBackend, error) {

	for _, requiredProp := range requiredVaultProps {
		if _, propPresent := props[requiredProp]; !propPresent {
			return nil, fmt.Errorf("property '%s' is requried to instantiate vault client", requiredProp)
		}
	}

	vaultToken := ""
	var isTokenProvided bool
	if vaultToken, isTokenProvided = props["Token"]; !isTokenProvided || vaultToken == "" {
		vaultToken, isTokenProvided = os.LookupEnv(fmt.Sprintf(vaultTokenEnvVarFormat, crdStoreName))
		if vaultToken == "" || !isTokenProvided {
			if vault.PrimaryToken != "" {
				return nil, errors.New("no vault token provided")
			}
			vaultToken = vault.PrimaryToken
		}
	}

	timeout, err := time.ParseDuration(props["Timeout"])
	if err != nil {
		return nil, fmt.Errorf("timeout is not parsable: %s", err)
	}

	maxRetries, err := strconv.ParseInt(props["MaxRetries"], 10, 8)
	if err != nil {
		return nil, fmt.Errorf("MaxRetries is not parsable: %s", err)
	}

	transport := cleanhttp.DefaultPooledTransport()
	transport.ResponseHeaderTimeout = time.Second * 3
	transport.TLSHandshakeTimeout = time.Second * 3
	vaultClient, err := api.NewClient(&api.Config{
		Address:    props["Endpoint"],
		Timeout:    timeout,
		MaxRetries: int(maxRetries),
		HttpClient: &http.Client{Transport: transport, Timeout: time.Second * 2},
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create Vault client: %s", err)
	}

	vaultClient.SetToken(vaultToken)
	return &vaultCredsBackend{
		vaultClient: vaultClient,
		pathPrefix:  props["PathPrefix"],
		name:        crdStoreName,
	}, nil
}

func (vault *vaultCredsBackend) FetchCredentials(accessKey string, storageName string) (*CredentialsStoreData, error) {
	fetchStartTime := time.Now()
	vaultResponse, err := vault.
		vaultClient.
		Logical().
		Read(fmt.Sprintf(vaultCredsFormat, vault.pathPrefix, accessKey, storageName))

	metrics.UpdateSince(fmt.Sprintf("credsStore.%s.read", vault.name), fetchStartTime)
	if err != nil {
		metrics.UpdateSince(fmt.Sprintf("credsStore.%s.err", vault.name), fetchStartTime)
		return nil, err
	}
	access, secret, err := parseVaultResponse(vaultResponse)
	if err != nil {
		metrics.UpdateSince(fmt.Sprintf("credsStore.%s.invalid", vault.name), fetchStartTime)
		return nil, err
	}
	return &CredentialsStoreData{
		AccessKey: access,
		SecretKey: secret,
	}, nil
}

func parseVaultResponse(vaultResponse *api.Secret) (string, string, error) {
	if vaultResponse == nil || vaultResponse.Data == nil || vaultResponse.Data["data"] == nil {
		return "", "", errors.New("empty response")
	}
	responseData, castOK := vaultResponse.Data["data"].([]interface{})
	if !castOK || len(responseData) == 0 {
		return "", "", errors.New("invalid response format")
	}
	keys, castOK := responseData[0].(map[string]interface{})
	if !castOK || len(responseData) == 0 {
		return "", "", errors.New("invalid response format")
	}
	if _, accessPresent := keys["access_key"]; !accessPresent {
		return "", "", errors.New("access key is missing")
	}
	if _, secretPresent := keys["secret_key"]; !secretPresent {
		return "", "", errors.New("secret key is missing")
	}
	return keys["access_key"].(string), keys["secret_key"].(string), nil
}
