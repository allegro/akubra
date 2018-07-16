package brimapi

import (
	"fmt"

	"github.com/allegro/akubra/discovery"
	"github.com/sirupsen/logrus"
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
	Path  string      `json:"Path"`
}

// Levels for logrus.Hook interface complience
func (lh *LogHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Fire for logrus.Hook interface compliance
func (lh *LogHook) Fire(entry *logrus.Entry) (err error) {
	endpoint, err := lh.doRequest(entry.Message)
	if err != nil {
		return fmt.Errorf("problem with sync task by endpoint: '%s' with payload: '%s' - err: '%s'",
			endpoint, entry.Message, err)
	}
	fmt.Printf("put sync task by endpoint: '%s' with payload: '%s'\n", endpoint, entry.Message)
	return
}

func (lh *LogHook) doRequest(payload string) (endpoint string, err error) {
	resp, endpoint, err := discovery.DoRequestWithDiscoveryService(
		discovery.GetHTTPClient(), lh.Host, lh.Path, lh.Creds.User, lh.Creds.Pass, payload)
	if err != nil {
		return
	}
	return endpoint, discovery.DiscardBody(resp)
}
