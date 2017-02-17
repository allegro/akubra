package httphandler

import (
	"fmt"
	"net/http"
	"time"

	"github.com/allegro/akubra/log"
)

// AccessMessageData holds all important informations
// about http roundtrip
type AccessMessageData struct {
	Method     string  `json:"method"`
	Host       string  `json:"host"`
	Path       string  `json:"path"`
	UserAgent  string  `json:"useragent"`
	StatusCode int     `json:"status"`
	Duration   float64 `json:"duration"`
	RespErr    string  `json:"error"`
	ReqID      string  `json:"reqID"`
	Time       string  `json:"ts"`
}

// String produces data in csv format with fields in following order:
// Method, Host, Path, UserAgent, StatusCode, Duration, RespErr)
func (amd AccessMessageData) String() string {
	return fmt.Sprintf("%q, %q, %q, %q, %d, %f, %q",
		amd.Method, amd.Host, amd.Path, amd.UserAgent,
		amd.StatusCode, amd.Duration, amd.RespErr)
}

// NewAccessLogMessage creates new AccessMessageData
func NewAccessLogMessage(req http.Request,
	statusCode int, duration float64, respErr string) *AccessMessageData {
	ts := time.Now().Format(time.RFC3339Nano)
	reqID, _ := req.Context().Value(log.ContextreqIDKey).(string)
	return &AccessMessageData{
		req.Method,
		req.Host,
		req.URL.Path,
		req.Header.Get("User-Agent"),
		statusCode, duration * 1000, respErr,
		reqID, ts}
}

// ScanCSVAccessLogMessage will scan csv string and return AccessMessageData.
// Returns fmt.SScanf error if matching failed
func ScanCSVAccessLogMessage(csvstr string) (AccessMessageData, error) {
	amd := AccessMessageData{}
	_, err := fmt.Sscanf(csvstr, "%q, %q, %q, %q, %d, %f, %q", &amd.Method, &amd.Host,
		&amd.Path, &amd.UserAgent, &amd.StatusCode, &amd.Duration,
		&amd.RespErr, &amd.Time)
	return amd, err
}

// SyncLogMessageData holds all important informations
// about replication errors
type SyncLogMessageData struct {
	Method      string `json:"method"`
	FailedHost  string `json:"failedhost"`
	Path        string `json:"path"`
	SuccessHost string `json:"successhost"`
	UserAgent   string `json:"useragent"`
	// ContentLength if negative means no content length header provided
	ContentLength int64  `json:"content-length"`
	ErrorMsg      string `json:"error"`
	ReqID         string `json:"reqID"`
	Time          string `json:"ts"`
}

// String produces data in csv format with fields in following order:
// Method, Host, Path, UserAgent, StatusCode, Duration, RespErr)
func (slmd SyncLogMessageData) String() string {
	return fmt.Sprintf("%q, %q, %q, %q, %q, %d, %q",
		slmd.Method,
		slmd.FailedHost,
		slmd.Path,
		slmd.SuccessHost,
		slmd.UserAgent,
		slmd.ContentLength,
		slmd.ErrorMsg)
}

// NewSyncLogMessageData creates new SyncLogMessageData
func NewSyncLogMessageData(method, failedHost, path, successHost, userAgent,
	reqID, errorMsg string, contentLength int64) *SyncLogMessageData {
	ts := time.Now().Format(time.RFC3339Nano)
	return &SyncLogMessageData{
		method, failedHost, path, successHost, userAgent,
		contentLength, errorMsg, reqID, ts}
}
