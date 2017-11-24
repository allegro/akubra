package crdstore

import (
	"encoding/json"
	"time"
)

const urlPattern = "%s/%s/%s"

// CredentialsStoreData - stores single access-secret key pair with EOL(TTL)
type CredentialsStoreData struct {
	AccessKey string `json:"access"`
	SecretKey string `json:"secret"`
	err       error
	EOL       time.Time `json:"-"`
}

// Unmarshal - Unmarshal CredentialsStoreData to json
func (csd *CredentialsStoreData) Unmarshal(credentials []byte) error {
	return json.Unmarshal(credentials, &csd)
}
