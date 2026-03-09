package httphandler

import (
	"fmt"
	"github.com/allegro/akubra/internal/akubra/utils"
	"net/http"
	"time"

	"github.com/allegro/akubra/internal/akubra/log"
)

// AccessMessageData holds all important informations
// about http roundtrip
type AccessMessageData struct {
	Method           string  `json:"req_method"`
	Host             string  `json:"req_host"`
	Path             string  `json:"req_path"`
	UserAgent        string  `json:"req_useragent"`
	StatusCode       int     `json:"resp_status_code"`
	Duration         float64 `json:"duration_ms"`
	RespErr          string  `json:"resp_err_msg"`
	ReqID            string  `json:"req_id"`
	Timestamp        string  `json:"@timestamp"`
	AccessKey        string  `json:"access_key"`
	BackendResponses string  `json:"backend_responses"`
	Message          string  `json:"message"`
	Level            string  `json:"level"`
}

// String produces data in csv format with fields in following order:
// Method, Host, Path, UserAgent, StatusCode, Duration, RespErr)
func (amd AccessMessageData) String() string {
	return fmt.Sprintf("%q, %q, %q, %q, %d, %f, %q",
		amd.Method, amd.Host, amd.Path, amd.UserAgent,
		amd.StatusCode, amd.Duration, amd.RespErr)
}

// NewAccessLogMessage creates new AccessMessageData
func NewAccessLogMessage(req *http.Request,
	statusCode int, duration float64, respErr string) *AccessMessageData {
	ts := time.Now().UTC().Format("2006-01-02T15:04:05.000") + "Z"
	reqID, _ := req.Context().Value(log.ContextreqIDKey).(string)
	backendResponses := utils.GetRequestProcessingMetadata(req, "backendResponse")
	level := "INFO"
	if respErr != "" {
		level = "ERROR"
	}
	return &AccessMessageData{
		Method:           req.Method,
		Host:             req.Host,
		Path:             req.URL.Path,
		UserAgent:        req.Header.Get("User-Agent"),
		StatusCode:       statusCode,
		Duration:         duration,
		RespErr:          respErr,
		ReqID:            reqID,
		Timestamp:        ts,
		AccessKey:        utils.ExtractAccessKey(req),
		BackendResponses: backendResponses,
		Message:          "access log",
		Level:            level,
	}
}

// ScanCSVAccessLogMessage will scan csv string and return AccessMessageData.
// Returns fmt.SScanf error if matching failed
func ScanCSVAccessLogMessage(csvstr string) (AccessMessageData, error) {
	amd := AccessMessageData{}
	_, err := fmt.Sscanf(csvstr, "%q, %q, %q, %q, %d, %f, %q", &amd.Method, &amd.Host,
		&amd.Path, &amd.UserAgent, &amd.StatusCode, &amd.Duration,
		&amd.RespErr, &amd.Timestamp)
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
	AccessKey     string `json:"access-key"`
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
