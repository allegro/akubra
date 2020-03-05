package vault

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/akubra/metrics"

	cleanhttp "github.com/hashicorp/go-cleanhttp"
	"github.com/hashicorp/vault/api"

	yaml "gopkg.in/yaml.v2"
)

const (
	vaultConfigVarName = "VAULT_CONFIG_VAR_NAME"
)

// Client returns key-value store client
type Client interface {
	Read(path string) (map[string]interface{}, error)
	Write(path string, data map[string]interface{}) error
}

// DefaultClient is initialized with environment variables:
// VAULT_CONFIG_VAR_NAME points to env var with raw yaml map with
// following keys:
// : Address
// : Token
// : Timeout
// : MaxRetries
// : Prefix
var DefaultClient Client

// PrimaryToken is exposed default token obtained by environment variables
var PrimaryToken string

func init() {
	configVarName := os.Getenv(vaultConfigVarName)
	if configVarName == "" {
		log.Println("No config var name")
		return
	}
	vaultConfigRaw := os.Getenv(configVarName)
	if vaultConfigRaw == "" {
		log.Printf("No config in %s\n", configVarName)
		return
	}
	settings := Settings{}
	yaml.Unmarshal([]byte(vaultConfigRaw), &settings)
	if settings.Token != "" {
		PrimaryToken = settings.Token
	}
	DefaultClient = newVault(settings)
}

func newVault(settings Settings) Client {
	httpClient := newHTTPClient()
	client, err := api.NewClient(&api.Config{
		Address:    settings.Address,
		Timeout:    settings.Timeout.Duration,
		MaxRetries: settings.MaxRetries,
		HttpClient: httpClient,
	})
	if err != nil {
		return nil
	}
	client.SetToken(settings.Token)
	return &vaultClient{Client: client, prefix: settings.Prefix}
}

func newHTTPClient() *http.Client {
	transport := cleanhttp.DefaultPooledTransport()
	transport.ResponseHeaderTimeout = time.Second * 3
	transport.TLSHandshakeTimeout = time.Second * 3
	return &http.Client{
		Transport: transport,
		Timeout:   time.Second * 2,
	}
}

// Settings keeps vault client settings
type Settings struct {
	Address    string           `yaml:"Address"`
	Timeout    metrics.Interval `yaml:"Timeout"`
	MaxRetries int              `yaml:"MaxRetries"`
	Token      string           `yaml:"Token"`
	Prefix     string           `yaml:"Prefix"`
}

type vaultClient struct {
	*api.Client
	prefix string
}

func (client *vaultClient) Read(path string) (map[string]interface{}, error) {
	secretPath := fmt.Sprintf("%s/%s", client.prefix, path)
	secret, err := client.Client.
		Logical().
		Read(secretPath)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, nil
	}
	return secret.Data, nil
}

func (client *vaultClient) Write(path string, data map[string]interface{}) error {
	secretPath := fmt.Sprintf("%s/%s", client.prefix, path)
	v, err := client.Client.
		Logical().Write(secretPath, data)
	fmt.Printf("%v %v", v, err)
	return err
}
