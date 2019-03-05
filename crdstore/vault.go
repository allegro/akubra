package crdstore

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/hashicorp/go-cleanhttp"
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
}

func (vaultFactory *vaultCredsBackendFactory) create(crdStoreName string, props map[string]string) (CredentialsBackend, error) {

	for _, requiredProp := range requiredVaultProps {
		if _, propPresent := props[requiredProp]; !propPresent {
			return nil, fmt.Errorf("property '%s' is requried to instantiate vault client", requiredProp)
		}
	}

	vaultToken := ""
	isTokenProvided := false
	if vaultToken, isTokenProvided = props["Token"]; !isTokenProvided || vaultToken == "" {
		vaultToken, isTokenProvided = os.LookupEnv(fmt.Sprintf(vaultTokenEnvVarFormat, crdStoreName))
		if vaultToken == "" || !isTokenProvided {
			return nil, errors.New("no vault token provided")
		}
	}

	timeout, err := time.ParseDuration(props["Timeout"])
	if err != nil {
		return nil, fmt.Errorf("Timeout is not parsable: %s", err)
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
	}, nil
}

func (vault *vaultCredsBackend) FetchCredentials(accessKey string, storageName string) (*CredentialsStoreData, error) {
	vaultResponse, err := vault.
		vaultClient.
		Logical().
		Read(fmt.Sprintf(vaultCredsFormat, vault.pathPrefix, accessKey, storageName))
	if err != nil {
		return nil, err
	}
	if vaultResponse == nil || vaultResponse.Data == nil || vaultResponse.Data["data"] == nil {
		return nil, fmt.Errorf("invlid response for %s/%s", accessKey, storageName)
	}
	responseData := vaultResponse.Data["data"].(map[string]interface{})
	if _, accessPresent := responseData["access"]; !accessPresent {
		return nil, fmt.Errorf("access key is missing for %s/%s", accessPresent, storageName)
	}
	if _, secretPresent := responseData["secret"]; !secretPresent {
		return nil, fmt.Errorf("access key is missing for %s/%s", accessKey, storageName)
	}
	return &CredentialsStoreData{
		AccessKey: responseData["access"].(string),
		SecretKey: responseData["secret"].(string),
	}, nil
}
