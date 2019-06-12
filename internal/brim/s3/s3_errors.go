package s3

import "errors"

// TextErr is an error that also implements the TextMarshaller interface for
// serializing out to various plain text encodings. Packages creating their
// own custom errors should use TextErr if they're intending to use serializing
// formats like json, msgpack etc.
type TextErr struct {
	Err error
}

// Error implements the error interface.
func (t TextErr) Error() string {
	return t.Err.Error()
}

// MarshalText implements the TextMarshaller
func (t TextErr) MarshalText() ([]byte, error) {
	return []byte(t.Err.Error()), nil
}

var (
	// ErrZeroContentLenthValue is the error returned when object in SRC has no content
	ErrZeroContentLenthValue = TextErr{errors.New("Content has zero length or hasn't size header")}
	// ErrContentLengthMaxValue is the error returned when object in SRC has no content
	ErrContentLengthMaxValue = TextErr{errors.New("Object size is to big")}
	// ErrEmptyContentType is the error returned when object hasn't Content-Type header
	ErrEmptyContentType = TextErr{errors.New("No Content-Type header")}
	// ErrDatabaseIntegrity is the error returned for all abnormal database cases
	ErrDatabaseIntegrity = TextErr{errors.New("Database integrity error")}
)
