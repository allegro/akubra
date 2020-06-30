package crdstore

import (
	"fmt"
	"github.com/allegro/akubra/internal/akubra/balancing"
	"github.com/allegro/akubra/internal/akubra/log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/allegro/akubra/internal/akubra/config/vault"
	"github.com/allegro/akubra/internal/akubra/metrics"
	cleanhttp "github.com/hashicorp/go-cleanhttp"
	"github.com/hashicorp/vault/api"
	"github.com/pkg/errors"
)

const vaultTokenEnvVarFormat = "CREDS_BACKEND_VAULT_%s_token"
const vaultCredsFormat = "%s/%s/%s"

var requiredVaultProps = []string{"Endpoint", "Timeout", "MaxRetries", "PathPrefix"}

var (
	errNoCredentialsFound       = errors.New("no credentials found in crdstore")
	errInvalidCredentialsFormat = errors.New("invalid credentials response format")
	errAccessKeyMissing         = errors.New("access key missing")
	errSecretKeyMissing         = errors.New("secret key missing")
)

type vaultCredsBackendFactory struct{}

type vaultCredsBackend struct {
	vaultClient *api.Client
	pathPrefix  string
	name        string
}

type balancedVaultClientFactory struct{}

func (bvcf *balancedVaultClientFactory) create(crdStoreName string, props map[string]string) (CredentialsBackend, error) {
	endpoints := make([]string, 0)
	for k, v := range props {
		print(k)
		if strings.HasPrefix(k,"Endpoint") {
			endpoints = append(endpoints, v)
		}
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("improperly configured balanced vault credentials store")
	}

	nodes := make([]balancing.Node, 0)
	factory := &vaultCredsBackendFactory{}
	bprops, err := extractBreakerProps(props)
	if err != nil {
		return nil, fmt.Errorf("balanced vault credstore missconfigured %w", err)
	}
	for _, endpoint := range endpoints {
		endpointProps := make(map[string]string)
		for k, v := range props {
			if k != "Endpoints" {
				endpointProps[k] = v
			}
		}
		endpointProps["Endpoint"] = endpoint
		backend, err := factory.create(crdStoreName, endpointProps)
		if err != nil {
			return nil, err
		}
		meter := balancing.NewCallMeter(bprops.retention, bprops.resolution)
		breaker := balancing.NewBreaker(bprops.breakerProbeSize, bprops.callTimeLimit,
			bprops.timeLimitPercentile, bprops.errorRate, bprops.closeDelay,bprops.maxDelay)
		backendNode := MeasuredVaultClient{Node:meter, Breaker:breaker, client:backend}
		nodes = append(nodes, backendNode)
	}
	return balancedVaultClients(nodes), nil
}

func extractBreakerProps(props map[string]string) (*breakerProps, error) {
	breakerProbeSize, ok := props["BreakerProbeSize"]
	if !ok {
		return nil, fmt.Errorf("no breakerProbeSize")
	}

	breakerProbeSizeInt, err := strconv.ParseInt(breakerProbeSize, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("broken configuration breakerProbeSize %w", err)
	}

	breakerErrorRate, ok := props["BreakerErrorRate"]
	if !ok {
		return nil, fmt.Errorf("no breakerProbeSize")
	}

	breakerErrorRateFloat, err := strconv.ParseFloat(breakerErrorRate, 64)
	if err != nil {
		return nil, fmt.Errorf("broken configuration breakerProbeSize %w", err)
	}

	breakerCallTimeLimitPercentile, ok := props["BreakerCallTimeLimitPercentile"]
	if !ok {
		return nil, fmt.Errorf("no breakerCallTimeLimitPercentile")
	}

	breakerCallTimeLimitPercentileFloat, err := strconv.ParseFloat(breakerCallTimeLimitPercentile, 64)
	if err != nil {
		return nil, fmt.Errorf("broken configuration breakerCallTimeLimitPercentile %w", err)
	}

	breakerCallTimeLimit, ok := props["BreakerCallTimeLimit"]
	if !ok {
		return nil, fmt.Errorf("no breakerCallTimeLimit")
	}

	breakerCallTimeLimitDuration, err := time.ParseDuration(breakerCallTimeLimit)
	if err != nil {
		return nil, fmt.Errorf("broken configuration breakerCallTimeLimit %w", err)
	}

	breakerBasicCutOutDuration, ok := props["BreakerBasicCutOutDuration"]
	if !ok {
		return nil, fmt.Errorf("no breakeBasicCutOutDurationt")
	}

	breakerBasicCutOutDurationDuration, err := time.ParseDuration(breakerBasicCutOutDuration)
	if err != nil {
		return nil, fmt.Errorf("broken configuration breakerBasicCutOutDuration %w", err)
	}

	breakerMaxCutOutDuration, ok := props["BreakerMaxCutOutDuration"]
	if !ok {
		return nil, fmt.Errorf("no breakerMaxCutOutDuration")
	}

	breakerMaxCutOutDurationDuration, err := time.ParseDuration(breakerMaxCutOutDuration)
	if err != nil {
		return nil, fmt.Errorf("broken configuration breakerMaxCutOutDuration %w", err)
	}

	meterResolution, ok := props["MeterResolution"]
	if !ok {
		return nil, fmt.Errorf("no meterResolution")
	}

	meterResolutionDuration, err := time.ParseDuration(meterResolution)
	if err != nil {
		return nil, fmt.Errorf("broken configuration meterResolution %w", err)
	}

	meterRetention, ok := props["MeterRetention"]
	if !ok {
		return nil, fmt.Errorf("no meterRetention")
	}

	meterRetentionDuration, err := time.ParseDuration(meterRetention)
	if err != nil {
		return nil, fmt.Errorf("broken configuration meterRetention %w", err)
	}

	return &breakerProps{
		retention:           meterResolutionDuration,
		resolution:          meterRetentionDuration,
		breakerProbeSize:    int(breakerProbeSizeInt),
		callTimeLimit:       breakerCallTimeLimitDuration,
		timeLimitPercentile: breakerCallTimeLimitPercentileFloat,
		errorRate:           breakerErrorRateFloat,
		closeDelay:          breakerBasicCutOutDurationDuration,
		maxDelay:            breakerMaxCutOutDurationDuration,
	}, nil
}

type breakerProps struct {
	retention                      time.Duration
	resolution                     time.Duration
	breakerProbeSize               int
	callTimeLimit                  time.Duration
	timeLimitPercentile, errorRate float64
	closeDelay, maxDelay           time.Duration
}

type BalancedVaultClient struct {
	balancing.ResponseTimeBalancer
}

func (bvc *BalancedVaultClient) FetchCredentials(accessKey, storageName string) (*CredentialsStoreData, error) {
	electedNodes := make([]balancing.Node, 0)
	for {
		node, err := bvc.ResponseTimeBalancer.Elect(electedNodes...)
		if err == balancing.ErrNoActiveNodes {
			return nil, fmt.Errorf("no creds storages available")
		}
		mvc := node.(*MeasuredVaultClient)
		value, err := mvc.FetchCredentials(accessKey, storageName)
		if err != nil {
			electedNodes = append(electedNodes, node)
			continue
		}
		return value, err
	}
	return nil, fmt.Errorf("no creds storages available")
}

func balancedVaultClients(nodes []balancing.Node) *BalancedVaultClient {
	return &BalancedVaultClient{balancing.ResponseTimeBalancer{nodes}}
}

type MeasuredVaultClient struct {
	balancing.Node
	balancing.Breaker
	Name   string
	client CredentialsBackend
}

func (mvc *MeasuredVaultClient) FetchCredentials(accessKey, storageName string) (*CredentialsStoreData, error) {
	start := time.Now()
	secret, err := mvc.client.FetchCredentials(accessKey, storageName)
	duration := time.Since(start)
	open := mvc.Breaker.Record(duration, err == nil)
	mvc.Node.UpdateTimeSpent(duration)
	mvc.Node.SetActive(!open)
	return secret, err
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
			if vault.PrimaryToken == "" {
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
	log.Debugf("Request in FetchCredentials %s", accessKey)
	defer log.Debugf("Request out FetchCredentials %s", accessKey)
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
		return "", "", errNoCredentialsFound
	}

	responseData, castOK := vaultResponse.Data["data"].([]interface{})
	if !castOK || len(responseData) == 0 {
		return "", "", errInvalidCredentialsFormat
	}

	keys, castOK := responseData[0].(map[string]interface{})
	if !castOK || len(responseData) == 0 {
		return "", "", errInvalidCredentialsFormat
	}

	if _, accessPresent := keys["access_key"]; !accessPresent {
		return "", "", errAccessKeyMissing
	}

	if _, secretPresent := keys["secret_key"]; !secretPresent {
		return "", "", errSecretKeyMissing
	}

	return keys["access_key"].(string), keys["secret_key"].(string), nil
}
