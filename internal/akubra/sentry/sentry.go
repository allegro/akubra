package sentry

import (
	"fmt"
	"github.com/getsentry/sentry-go"
	sentryhttp "github.com/getsentry/sentry-go/http"
)

func CreateSentryHandler(config *Config) (*sentryhttp.Handler, error) {
	if config == nil {
		return nil, fmt.Errorf("received nil SentryConfig")
	}

	if config.Dsn == "" {
		return nil, fmt.Errorf("no sentry DSN specified")
	}

	err := sentry.Init(sentry.ClientOptions{
		Dsn: config.Dsn,
	})
	if err != nil {
		return nil, err
	}

	sentryHandler := sentryhttp.New(sentryhttp.Options{
		Repanic:         true,
		WaitForDelivery: false,
		Timeout:         config.Timeout.Duration,
	})
	return sentryHandler, nil
}
