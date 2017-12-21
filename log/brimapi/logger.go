package brimapi

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/sirupsen/logrus"
)

// Credentials stores brim api credentials
type Credentials struct {
	User string `json:"User"`
	Pass string `json:"Pass"`
}

const uploadSynctasksURI = "/v1/processes/uploadsynctasks"

func discardBody(resp *http.Response) error {
	if resp != nil && resp.Body != nil {
		_, err := io.Copy(ioutil.Discard, resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			return err
		}
	}
	return nil
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

// Fire for logrus.Hook interface complience
func (lh *LogHook) Fire(entry *logrus.Entry) error {
	bodyBytes := []byte(entry.Message)
	req, err := http.NewRequest(
		http.MethodPut,
		lh.Host+uploadSynctasksURI,
		bytes.NewBuffer(bodyBytes))

	if err != nil {
		return err
	}
	req.SetBasicAuth(lh.Creds.User, lh.Creds.Pass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	return discardBody(resp)
}
