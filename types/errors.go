package types

import (
	"fmt"

	"github.com/allegro/akubra/transport"
)

// BackendError is returned in Backend.RoundTrip if any
type BackendError struct {
	HostName string
	OrigErr  error
}

// Backend method returns host name
func (be *BackendError) Backend() string {
	return be.HostName
}

// Err returns original error instance
func (be *BackendError) Err() error {
	return be.OrigErr
}

// Error method complies with built in error interface
func (be *BackendError) Error() (errMsg string) {
	errMsg = fmt.Sprintf("backend %s responded with error %s", be.HostName, be.OrigErr)
	if _, ok := be.OrigErr.(*transport.DefinitionError); !ok {
		errMsg = fmt.Sprintf("backend %s responded with error %s", be.HostName, be.OrigErr)
	}
	return
}

// ErrorBackendMaintenance signals maintenance mode error
var ErrorBackendMaintenance = fmt.Errorf("Host in maintenance mode")
