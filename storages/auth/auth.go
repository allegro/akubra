package auth

import (
	"fmt"

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
var Decorators = map[string]func(string, config.Backend) (httphandler.Decorator, error){
	Passthrough: func(backend string, backendConfig config.Backend) (httphandler.Decorator, error) {
		return RequestFormatDecorator(backendConfig.Endpoint.URL, backendConfig.ForcePathStyle), nil
	},
	S3FixedKey: func(backend string, backendConf config.Backend) (httphandler.Decorator, error) {
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
		return SignDecorator(keys, backendConf.Region, backendConf.Endpoint.URL, backendConf.ForcePathStyle), nil
	},
	S3AuthService: func(backend string, backendConf config.Backend) (httphandler.Decorator, error) {
		endpoint, ok := backendConf.Properties["AuthServiceEndpoint"]
		if !ok {
			endpoint = "default"
		}

		return SignAuthServiceDecorator(backend, backendConf.Region, endpoint, backendConf.Endpoint.URL, backendConf.ForcePathStyle), nil
	},
}
