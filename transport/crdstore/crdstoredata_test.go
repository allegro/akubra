package crdstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldUnmarshalCredentialsStoreData(t *testing.T) {
	expectedAccessKey := "access222"
	expectedSecretKey := "secret222"
	credentials := string(`{"access":"access222","secret":"secret222"}`)

	var cds CredentialsStoreData
	cds.Unmarshal(credentials)

	require.Equal(t, expectedAccessKey, cds.AccessKey, "access keys must be equals")
	require.Equal(t, expectedSecretKey, cds.SecretKey, "secret keys must be equals")
}

func TestShouldMarshalCredentialsStoreData(t *testing.T) {
	expectedCredentials := []byte(`{"access":"access333","secret":"secret333"}`)
	accessKey := "access333"
	secretKey := "secret333"

	var cds CredentialsStoreData
	credentials, err := cds.Marshal(accessKey, secretKey)

	require.Nil(t, err)
	require.Equal(t, expectedCredentials, credentials, "credentials must be equals")
}
