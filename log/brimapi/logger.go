package brimapi

import (
	"github.com/sirupsen/logrus"
	"fmt"
)

// Credentials stores brim api credentials
type Credentials struct {
	User string `json:"User"`
	Pass string `json:"Pass"`
}

// LogHook collects and sends sync events to brim api
type LogHook struct {
	Creds Credentials `json:"Credentials"`
	Host  string      `json:"Host"`
}

// Levels for logrus.Hook interface complience
func (lh *LogHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Fire for logrus.Hook interface compliance
func (lh *LogHook) Fire(entry *logrus.Entry) (err error) {
	endpoint, err := doRequest(lh, httpClient, entry.Message)
	if err != nil {
		return fmt.Errorf("problem with sync task by endpoint: '%s' with payload: '%s' - err: '%s'",
			endpoint, entry.Message, err)
	}
	return
}
