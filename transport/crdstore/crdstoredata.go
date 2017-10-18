package crdstore

import (
	"encoding/json"
	"time"
)

const urlPattern = "%s/%s/%s"

// CredentialsStoreData - stores single access-secret key pair with EOL(TTL)
type CredentialsStoreData struct {
	AccessKey string    `json:"access"`
	SecretKey string    `json:"secret"`
	EOL       time.Time `json:"-"`
}

//Marshal - Marshal CredentialsStoreData from json
func (csd *CredentialsStoreData) Marshal(AccessKey, SecretKey string) (creds []byte, err error) {
	csd.AccessKey = AccessKey
	csd.SecretKey = SecretKey
	creds, err = json.Marshal(csd)
	return
}

//Unmarshal - Unmarshal CredentialsStoreData to json
func (csd *CredentialsStoreData) Unmarshal(credentials string) error {
	return json.Unmarshal([]byte(credentials), &csd)
}
