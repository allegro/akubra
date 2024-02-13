package sentry

import (
	"github.com/allegro/akubra/internal/akubra/metrics"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreateSentryHandlerError(t *testing.T) {
	sentryConfigs := []*Config{
		nil,
		{},
		{Dsn: "https://malformed_dsn"},
		{Timeout: metrics.Interval{Duration: time.Second}},
	}

	for _, config := range sentryConfigs {
		// given
		_, err := CreateSentryHandler(config)
		// expect
		assert.Error(t, err)
	}
}

func TestCreateSentryHandler(t *testing.T) {
	sentryConfigs := []*Config{
		{
			Dsn:     "https://key@host/path/42",
			Timeout: metrics.Interval{Duration: time.Second},
		},
		{
			Dsn: "https://key@host/path/42",
		},
	}

	for _, config := range sentryConfigs {
		// given
		sentryHandler, err := CreateSentryHandler(config)
		// expect
		assert.NoError(t, err)
		assert.NotNil(t, sentryHandler)
	}
}
