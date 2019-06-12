package types

import "io"

// Resetter interface
type Resetter interface {
	Reset() io.ReadCloser
}
