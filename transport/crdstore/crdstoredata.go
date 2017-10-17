package crdstore

import (
	"encoding/json"
	"fmt"

	"github.com/levigross/grequests"
)

const UrlPattern = "%s/%s/%s"

type CredentialsStoreData struct {
	AccessKey string `json:"access"`
	SecretKey string `json:"secret"`
}

func (csd *CredentialsStoreData) Marshal(AccessKey, SecretKey string) (creds []byte, err error) {
	csd.AccessKey = AccessKey
	csd.SecretKey = SecretKey
	creds, err = json.Marshal(csd)
	return
}

func (csd *CredentialsStoreData) Unmarshal(credentials string) error {
	return json.Unmarshal([]byte(credentials), &csd)
}

func (csdata *CredentialsStoreData) GetFromService(endpoint, accessKey, storageType string) (csd *CredentialsStoreData, err error) {
	ro := &grequests.RequestOptions{
		DialTimeout:    RequestOptionsDialTimeout,
		RequestTimeout: RequestOptionsRequestTimeout,
		RedirectLimit:  1,
		IsAjax:         false,
	}
	resp, err := grequests.Get(fmt.Sprintf(UrlPattern, endpoint, accessKey, storageType), ro)
	if err != nil {
		return csd, fmt.Errorf("unable to make request to credentials store service - err: %s", err)
	}
	if resp.StatusCode != 200 {
		return csd, fmt.Errorf("unable to get credentials from store service - StatusCode: %d", resp.StatusCode)
	}

	credentials := resp.String()
	if len(credentials) == 0 {
		return csd, fmt.Errorf("got empty credentials from store service%s", "")
	}

	csd.Unmarshal(credentials)

	return
}
