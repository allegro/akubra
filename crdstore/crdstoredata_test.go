package crdstore

import (
	"testing"

	"encoding/json"

	"github.com/stretchr/testify/require"
)

func TestShouldUnmarshalCredentialsStoreData(t *testing.T) {
	expectedAccessKey := "access222"
	expectedSecretKey := "secret222"
	credentials := []byte(`{"access":"access222","secret":"secret222"}`)

	var cds CredentialsStoreData
	json.Unmarshal(credentials, &cds)

	require.Equal(t, expectedAccessKey, cds.AccessKey, "access keys must be equals")
	require.Equal(t, expectedSecretKey, cds.SecretKey, "secret keys must be equals")
}
