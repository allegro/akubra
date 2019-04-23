package util

import (
	"context"
	"sync"
)

// ContextWithError is mutex protected context
type ContextWithError struct {
	context.Context
	error      error
	mutex      sync.Mutex
	cancelFunc context.CancelFunc
}

// CancelContext cancels context
func (context *ContextWithError) CancelContext(err error) {
	context.mutex.Lock()
	defer context.mutex.Unlock()
	if context.error == nil {
		context.error = err
	}
	context.cancelFunc()
}

// GetError returns error
func (context *ContextWithError) GetError() error {
	context.mutex.Lock()
	defer context.mutex.Unlock()
	return context.error
}

// NewContextWithError creates ContextWithError instance
func NewContextWithError() *ContextWithError {
	ctx, cancelFunc := context.WithCancel(context.Background())
	contextWithError := &ContextWithError{
		Context:    ctx,
		error:      nil,
		mutex:      sync.Mutex{},
		cancelFunc: cancelFunc}

	return contextWithError
}
