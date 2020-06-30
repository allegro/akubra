package storages

import (
	"fmt"
	"github.com/allegro/akubra/internal/akubra/crdstore"
	"github.com/allegro/akubra/internal/akubra/httphandler"
	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/akubra/storages/auth"
	"github.com/allegro/akubra/internal/akubra/utils"
	"net/http"
)

//ShardAuthenticator is a delegating NamedSharedClient that checks the requests authorization
type ShardAuthenticator struct {
	shardClient                 NamedShardClient
	ignoredCanonicalizedHeaders map[string]bool
}

//NewShardAuthenticator creates an instance of ShardAuthenticator
func NewShardAuthenticator(shardClient NamedShardClient, ignoredCanonicalizedHeaders map[string]bool) NamedShardClient {
	return &ShardAuthenticator{
		shardClient:                 shardClient,
		ignoredCanonicalizedHeaders: ignoredCanonicalizedHeaders}
}

//Name returns then of the shard
func (shardAuth *ShardAuthenticator) Name() string {
	return shardAuth.shardClient.Name()
}

//Backends returns the backends of a shard
func (shardAuth *ShardAuthenticator) Backends() []*StorageClient {
	return shardAuth.shardClient.Backends()
}

// RoundTrip first ensures that client is authorized to access the shard and the delegates
// the request to shard client
func (shardAuth *ShardAuthenticator) RoundTrip(req *http.Request) (*http.Response, error) {
	authHeaderVal := req.Context().Value(httphandler.AuthHeader)
	if authHeaderVal == nil {
		return shardAuth.shardClient.RoundTrip(req)
	}

	authHeader := authHeaderVal.(*utils.ParsedAuthorizationHeader)
	backends := shardAuth.shardClient.Backends()

	var backendsCredentials []auth.Keys
	for idx := range backends {
		switch backends[idx].Type {
		case auth.Passthrough:
			continue
		case auth.S3FixedKey:
			backendsCredentials = append(backendsCredentials, extractKeysFrom(backends[idx].Properties))
		case auth.S3AuthService:
			keys, err := fetchKeysFor(authHeader.AccessKey, backends[idx])
			if err != nil {
				utils.SetRequestProcessingMetadata(
					req,
					"auth",
					fmt.Sprintf("no keys for %s on backend %s", authHeader.AccessKey, backends[idx].Name))
				return nil, err
			}
			log.Debugf("Backend %s creds added", backends[idx].Name)
			backendsCredentials = append(backendsCredentials, keys)
		}
	}

	for idx := range backendsCredentials {

		if auth.ErrNone != auth.DoesSignMatch(req, backendsCredentials[idx], shardAuth.ignoredCanonicalizedHeaders) {
			log.Debugf("authorization check failed for req %s, signature mismatch on storage '%s' using access '%s'",
				req.Context().Value(log.ContextreqIDKey).(string), backends[idx].Name, backendsCredentials[idx].AccessKeyID)

			return utils.ResponseForbidden(req), nil
		}
	}

	return shardAuth.shardClient.RoundTrip(req)
}

func fetchKeysFor(clientAccessKey string, backend *StorageClient) (auth.Keys, error) {
	credentialsStoreName, ok := backend.Properties["CredentialsStore"]
	if !ok {
		credentialsStoreName = crdstore.DefaultCredentialsStoreName
	}
	credentialsStore, err := crdstore.GetInstance(credentialsStoreName)
	if err != nil {
		return auth.Keys{}, err
	}
	crdStoreResp, err := credentialsStore.Get(clientAccessKey, "akubra")
	if err != nil {
		return auth.Keys{}, err
	}
	return auth.Keys{
		AccessKeyID:     crdStoreResp.AccessKey,
		SecretAccessKey: crdStoreResp.SecretKey,
	}, err
}

func extractKeysFrom(backendProps map[string]string) auth.Keys {
	accessKey := backendProps["AccessKey"]
	secretKey := backendProps["Secret"]
	return auth.Keys{
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
	}
}
