package http

import (
	"net/http"
)

//Client is an interface for a http client
type Client interface {
	Do(request *http.Request) (*http.Response, error)
}
