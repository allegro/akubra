package httphandler

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// AccessMessageData holds all important informations
// about http roundtrip
type AccessMessageData struct {
	Method     string  `json:"message"`
	Host       string  `json:"host"`
	Path       string  `json:"path"`
	UserAgent  string  `json:"useragent"`
	StatusCode int     `json:"status"`
	Duration   float64 `json:"duration"`
	RespErr    string  `json:"error"`
}

//String produces data in csv format with fields in following order:
//Method, Host, Path, UserAgent, StatusCode, Duration, RespErr)
func (amd AccessMessageData) String() string {
	return fmt.Sprintf("%q, %q, %q, %q, %d, %f, %q",
		amd.Method, amd.Host, amd.Path, amd.UserAgent,
		amd.StatusCode, amd.Duration, amd.RespErr)
}

//JSON produces data in json format with fields in following order
func (amd AccessMessageData) JSON() ([]byte, error) {
	return json.Marshal(amd)
}

//NewAccessLogMessage creates new AccessMessageData
func NewAccessLogMessage(req http.Request,
	statusCode int, duration float64, respErr string) *AccessMessageData {
	return &AccessMessageData{
		req.Method,
		req.Host,
		req.URL.Path,
		req.Header.Get("User-Agent"),
		statusCode, duration, respErr}
}

//ScanCSVAccessLogMessage will scan csv string and return AccessMessageData.
//Returns fmt.SScanf error if matching failed
func ScanCSVAccessLogMessage(csvstr string) (AccessMessageData, error) {
	amd := AccessMessageData{}
	_, err := fmt.Sscanf(csvstr, "%q, %q, %q, %q, %d, %f, %q", &amd.Method, &amd.Host,
		&amd.Path, &amd.UserAgent, &amd.StatusCode, &amd.Duration,
		&amd.RespErr)
	return amd, err
}
