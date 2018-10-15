package auth

import (
	"fmt"
	"net/http"

	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/storages/config"
)

const (
	// Passthrough is basic type, does nothing to the request
	Passthrough = "passthrough"
	// S3FixedKey will sign requests with single key
	S3FixedKey = "S3FixedKey"
	// S3AuthService will sign requests using key from external source
	S3AuthService = "S3AuthService"
)

// Decorators maps Backend type with httphadler decorators factory
var Decorators = map[string]func(string, config.Storage) (httphandler.Decorator, error){
	Passthrough: func(string, config.Storage) (httphandler.Decorator, error) {
		return func(rt http.RoundTripper) http.RoundTripper {
			return rt
		}, nil
	},
	S3FixedKey: func(backend string, backendConf config.Storage) (httphandler.Decorator, error) {
		accessKey, ok := backendConf.Properties["AccessKey"]
		if !ok {
			return nil, fmt.Errorf("no AccessKey defined for backend type %q", S3FixedKey)
		}

		secret, ok := backendConf.Properties["Secret"]
		if !ok {
			return nil, fmt.Errorf("no Secret defined for backend type %q", S3FixedKey)
		}

		keys := Keys{
			AccessKeyID:     accessKey,
			SecretAccessKey: secret,
		}
		methods := backendConf.Properties["Methods"]
		return ForceSignDecorator(keys, backendConf.Backend.Host, methods), nil
	},
	S3AuthService: func(backend string, backendConf config.Storage) (httphandler.Decorator, error) {
		endpoint, ok := backendConf.Properties["AuthServiceEndpoint"]
		if !ok {
			endpoint = "default"
		}

		return SignAuthServiceDecorator(backend, endpoint, backendConf.Backend.Host), nil
	},
}
