package crdstore

import (
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/syncmap"

)

const (
	existingAccess  = "access_exists"
	existingStorage = "storage_exists"
	errorAccess     = "access_error"
	errorStorage    = "storage_error"
)

var existingCredentials = CredentialsStoreData{AccessKey: "access_exists", SecretKey: "secret_exists"}

type credentialsBackendMock struct {
	mock.Mock
}

func (credsBackendMock *credentialsBackendMock) FetchCredentials(accessKey string, storageName string) (*CredentialsStoreData, error) {
	args := credsBackendMock.Called(accessKey, storageName)
	creds := args.Get(0)
	if creds == nil {
		return nil, args.Error(1)
	}
	return creds.(*CredentialsStoreData), args.Error(1)
}

func TestShouldPrepareInternalKeyBasedOnAccessAndStorageType(t *testing.T) {
	expectedKey := "access_____storage_type"
	cs := CredentialsStore{}
	key := cs.prepareKey("access", "storage_type")
	require.Equal(t, expectedKey, key, "keys must be equal")
}

func TestShouldSetCredentialsFromExternalServiceEndpoint(t *testing.T) {
	cs := prepareCredentialsStore(existingAccess, existingStorage, &existingCredentials, nil)
	csd, err := cs.Get(existingCredentials.AccessKey, existingStorage)
	require.NoError(t, err)
	require.Equal(t, existingCredentials.AccessKey, csd.AccessKey, "key must be equal")
	require.Equal(t, existingCredentials.SecretKey, csd.SecretKey, "key must be equal")
}

func TestShouldNotCacheCredentialOnErrorFromExternalService(t *testing.T) {
	cs := prepareCredentialsStore("access_error", "storage_error", nil, errors.New("network error"))

	_, err := cs.Get("access_error", "storage_error")
	require.Error(t, err)
}

func TestShouldGetCredentialFromCacheIfExternalServiceFails(t *testing.T) {
	expectedCredentials := &CredentialsStoreData{AccessKey: errorAccess, SecretKey: "secret_1"}
	cs := prepareCredentialsStore(errorAccess, errorStorage, nil, errors.New("network error"))

	cs.cache.Store(cs.prepareKey(errorAccess, errorStorage), expectedCredentials)

	crd, err := cs.Get(errorAccess, errorStorage)

	require.NoError(t, err)
	require.Equal(t, expectedCredentials.SecretKey, crd.SecretKey)
}

func TestShouldGetCredentialFromCacheIfTTLIsNotExpired(t *testing.T) {
	expectedCredentials := &CredentialsStoreData{AccessKey: existingAccess, SecretKey: "secret_1", EOL: time.Now().Add(10 * time.Second)}

	cs := prepareCredentialsStore("", "", nil, nil)
	cs.cache.Store(cs.prepareKey(existingAccess, existingStorage), expectedCredentials)
	crd, err := cs.Get(existingAccess, existingStorage)

	require.NoError(t, err)
	require.Equal(t, expectedCredentials.SecretKey, crd.SecretKey)
}

func TestShouldUpdateCredentialsIfTTLIsExpired(t *testing.T) {
	oldCredentials := &CredentialsStoreData{AccessKey: existingAccess, SecretKey: "secret_1", EOL: time.Now().Add(-20 * time.Second)}
	cs := prepareCredentialsStore(existingAccess, existingStorage, &existingCredentials, nil)

	cs.cache.Store(cs.prepareKey(existingAccess, existingStorage), oldCredentials)
	crd, err := cs.Get(existingAccess, existingStorage)

	require.NoError(t, err)
	require.Equal(t, existingCredentials.SecretKey, crd.SecretKey)
}

func TestShouldDeleteCachedCredentialsOnErrCredentialsNotFound(t *testing.T) {
	accessKey := "no_access"
	backend := "no_storage"
	oldCredentials := &CredentialsStoreData{AccessKey: accessKey, SecretKey: "secret_1", EOL: time.Now().Add(-10 * time.Second)}

	cs := prepareCredentialsStore(accessKey, backend, nil, ErrCredentialsNotFound)

	cs.cache.Store(cs.prepareKey("not_existing", backend), oldCredentials)
	crd, err := cs.Get(accessKey, backend)

	require.Equal(t, ErrCredentialsNotFound, err)
	require.Nil(t, crd)
}

func TestShouldGetCredentialsFromCacheIfUpdateIsLocked(t *testing.T) {
	expectedCredentials := &CredentialsStoreData{AccessKey: existingAccess, SecretKey: "secret_1", EOL: time.Now().Add(-11 * time.Second)}
	cs := prepareCredentialsStore(existingAccess, existingStorage, expectedCredentials, nil)

	cs.cache.Store(cs.prepareKey(existingAccess, existingStorage), expectedCredentials)
	cs.lock.Lock()
	crd, err := cs.Get(existingAccess, existingStorage)
	cs.lock.Unlock()

	require.NoError(t, err)
	require.Equal(t, expectedCredentials.SecretKey, crd.SecretKey)
}

func TestShouldUpdateCacheInBackground(t *testing.T) {
	cachedCredentials := &CredentialsStoreData{AccessKey: existingAccess, SecretKey: "secret_1", EOL: time.Now().Add(1 * time.Second)}
	cs := prepareCredentialsStore(existingAccess, existingStorage, &existingCredentials, nil)

	cs.cache.Store(cs.prepareKey(existingAccess, existingStorage), cachedCredentials)
	_, err := cs.Get(existingAccess, existingStorage)
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	cs.lock.Lock()
	crd, err := cs.Get(existingAccess, existingStorage)
	cs.lock.Unlock()

	require.NoError(t, err)
	require.Equal(t, existingCredentials.SecretKey, crd.SecretKey)
}

func prepareCredentialsStore(accessKey, storage string, expectedCreds *CredentialsStoreData, err error) *CredentialsStore {
	credsBackendMock := &credentialsBackendMock{mock.Mock{}}
	cs := CredentialsStore{
		credentialsBackend: credsBackendMock,
		cache:              &syncmap.Map{},
		lock:               sync.Mutex{},
		TTL:                time.Second * 10}

	credsBackendMock.On("FetchCredentials", accessKey, storage).Return(expectedCreds, err)
	return &cs
}
