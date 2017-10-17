package crdstore

import (
	"testing"
	"time"

	"fmt"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type CdrStoreServiceAPIMock struct {
	mock.Mock
}

func (csm *CdrStoreServiceAPIMock) GetFromService(endpoint, accessKey, storageType string) (csd *CredentialsStoreData, err error) {
	args := csm.Called(endpoint, accessKey, storageType)
	csd = args.Get(0).(*CredentialsStoreData)
	err = args.Error(1)
	return csd, err
}

func TestShouldGetZeroKeysFromEmptyCache(t *testing.T) {
	cs := NewCredentialsStore("http://localhost:8090", time.Second)
	keysCount := cs.cache.Count()
	require.Equal(t, 0, keysCount, "keys count need to be zero")
}

func TestShouldPrepareInternalKeyBasedOnAccessAndStorageType(t *testing.T) {
	expectedKey := "access_____storage_type"
	cs := NewCredentialsStore("http://localhost:8090", time.Second)
	key := cs.prepareKey("access", "storage_type")
	require.Equal(t, expectedKey, key, "keys must be equals")
}

func TestShouldSetCredentialsFromExternalServiceEndpoint(t *testing.T) {
	secretKey := "secret123"
	accessKey := "DFKJDHKJDFKJDHFDF"
	endpoint := "http://localhost:8090"
	storageType := "akubra2"
	expectedCredentials := &CredentialsStoreData{AccessKey: accessKey, SecretKey: secretKey}

	m := &CdrStoreServiceAPIMock{}
	m.On("GetFromService", endpoint, accessKey, storageType).Return(expectedCredentials, nil)

	cs := NewCredentialsStore(endpoint, 10*time.Second)
	cs.defaultService = m
	csd, result := cs.Get(accessKey, storageType)

	require.True(t, result, "should be nil")
	require.Equal(t, expectedCredentials, csd, "keys must be equals")
}

func TestShouldNotGetCredentialsFromBackupWhenExternalServiceFailed(t *testing.T) {
	cacheTtl := 3 * time.Second
	secretKey := "secret333"
	accessKey := "SDFKJDFHIUEHREFIDKFB"
	endpoint := "http://localhost:8091"
	storageType := "akubra3"
	expectedCredentials := &CredentialsStoreData{AccessKey: accessKey, SecretKey: secretKey}
	err := fmt.Errorf("")

	m := &CdrStoreServiceAPIMock{}
	m.On("GetFromService", endpoint, accessKey, storageType).Return(expectedCredentials, err)

	cs := NewCredentialsStore(endpoint, cacheTtl)
	cs.defaultService = m
	_, result := cs.Get(accessKey, storageType)

	time.Sleep(cacheTtl + 2*time.Second)
	require.False(t, result, "should be nil")
}
