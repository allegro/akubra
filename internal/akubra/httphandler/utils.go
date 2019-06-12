package httphandler

import (
	"net/http"
)

//IsHealthCheck checks if the request is a health check
func IsHealthCheck(request *http.Request) bool {
	return (request.URL.Path == "/" || request.URL.Path == "/status/ping") && request.URL.RawQuery == ""
}
