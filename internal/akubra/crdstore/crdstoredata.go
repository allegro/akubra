package crdstore

import (
	"encoding/json"
	"time"
)

// CredentialsStoreData - stores single access-secret key pair with EOL(TTL)
type CredentialsStoreData struct {
	AccessKey string    `json:"access"`
	SecretKey string    `json:"secret"`
	EOL       time.Time `json:"-"`
	err       error
}

// Unmarshal - Unmarshal CredentialsStoreData to json
func (csd *CredentialsStoreData) Unmarshal(credentials []byte) error {
	return json.Unmarshal(credentials, &csd)
}
