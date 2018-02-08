package config

import (
	"github.com/allegro/akubra/metrics"
)

// ClientTransportDetail properties
type ClientTransportDetail struct {
	// MaxIdleConns see: https://golang.org/pkg/net/http/#Transport
	// Default 0 (no limit)
	MaxIdleConns int `yaml:"MaxIdleConns" validate:"min=0"`
	// MaxIdleConnsPerHost see: https://golang.org/pkg/net/http/#Transport
	// Default 100
	MaxIdleConnsPerHost int `yaml:"MaxIdleConnsPerHost" validate:"min=0"`
	// IdleConnTimeout see: https://golang.org/pkg/net/http/#Transport
	// Default 0 (no limit)
	IdleConnTimeout metrics.Interval `yaml:"IdleConnTimeout"`
	// ResponseHeaderTimeout see: https://golang.org/pkg/net/http/#Transport
	// Default 5s (no limit)
	ResponseHeaderTimeout metrics.Interval `yaml:"ResponseHeaderTimeout"`
	// DisableKeepAlives see: https://golang.org/pkg/net/http/#Transport
	// Default false
	DisableKeepAlives bool `yaml:"DisableKeepAlives"`
}

// ClientTransportTriggers properties
type ClientTransportTriggers struct {
	Method 		string	`yaml:"Method" validate:"min=1"`
	Header 		string	`yaml:"Header" validate:"min=1"`
	Path 		string	`yaml:"Path" validate:"min=1"`
	QueryParam 	string	`yaml:"QueryParam" validate:"min=1"`
}

type Transport struct {
	Triggers			ClientTransportTriggers	`yaml:"Triggers"`
	MergingStrategy		string					`yaml:"MergingStrategy" validate:"min=1"`
	Details 			ClientTransportDetail	`yaml:"Details"`
}

type Transports map[byte]Transport
