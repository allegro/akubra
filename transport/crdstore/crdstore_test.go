package crdstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

const (
	httpListen      = "127.0.0.1:8091"
	httpEndpoint    = "http://127.0.0.1:8091"
	emptyAccess     = "access_empty"
	emptyStorage    = "storage_empty"
	invalidAccess   = "access_invalid"
	invalidStorage  = "storage_invalid"
	existingAccess  = "access_exists"
	existingStorage = "storage_exists"
	errorAccess     = "access_error"
	errorStorage    = "storage_error"
)

var existingCredentials = CredentialsStoreData{AccessKey: "access_exists", SecretKey: "secret_exists"}

func httpHandler(w http.ResponseWriter, r *http.Request) {
	switch r.URL.String() {
	case fmt.Sprintf("/%s/%s", existingAccess, existingStorage):
		w.WriteHeader(http.StatusOK)
		resp, _ := json.Marshal(existingCredentials)
		w.Write(resp)
	case fmt.Sprintf("/%s/%s", errorAccess, errorStorage):
		w.WriteHeader(http.StatusBadRequest)
		resp, _ := json.Marshal(existingCredentials)
		w.Write(resp)
	case fmt.Sprintf("/%s/%s", invalidAccess, invalidStorage):
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{'invalid'json//}"))
	case fmt.Sprintf("/%s/%s", emptyAccess, emptyStorage):
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func TestMain(m *testing.M) {
	http.HandleFunc("/", httpHandler)
	go func() {
		err := http.ListenAndServe(httpListen, nil)
		if err != nil {
			log.Println(err)
		}
	}()
	code := m.Run()
	os.Exit(code)

}
func TestShouldPrepareInternalKeyBasedOnAccessAndStorageType(t *testing.T) {
	expectedKey := "access_____storage_type"
	cs := NewCredentialsStore(httpEndpoint, time.Second)
	key := cs.prepareKey("access", "storage_type")
	require.Equal(t, expectedKey, key, "keys must be equals")
}

func TestShouldSetCredentialsFromExternalServiceEndpoint(t *testing.T) {
	cs := NewCredentialsStore(httpEndpoint, 10*time.Second)

	csd, err := cs.Get(existingCredentials.AccessKey, existingStorage)
	require.NoError(t, err)
	require.Equal(t, existingCredentials.AccessKey, csd.AccessKey, "key must be equal")
	require.Equal(t, existingCredentials.SecretKey, csd.SecretKey, "key must be equal")
}

func TestShouldNotCacheCredentialOnErrorFromExternalService(t *testing.T) {
	cs := NewCredentialsStore(httpEndpoint, 10*time.Second)
	_, err := cs.Get("access_error", "storage_error")

	require.Error(t, err)
}

func TestShouldGetCredentialFromCacheIfExternalServiceFails(t *testing.T) {
	expectedCredentials := &CredentialsStoreData{AccessKey: errorAccess, SecretKey: "secret_1"}

	cs := NewCredentialsStore(httpEndpoint, 10*time.Second)
	cs.cache.Store(cs.prepareKey(errorAccess, errorStorage), expectedCredentials)
	crd, err := cs.Get(errorAccess, errorStorage)

	require.NoError(t, err)
	require.Equal(t, expectedCredentials.SecretKey, crd.SecretKey)
}

func TestShouldGetCredentialFromCacheIfConnectionRefused(t *testing.T) {
	expectedCredentials := &CredentialsStoreData{AccessKey: errorAccess, SecretKey: "secret_1"}

	cs := NewCredentialsStore("http://127.255.255.255:8091", 10*time.Second)
	cs.cache.Store(cs.prepareKey(errorAccess, errorStorage), expectedCredentials)
	crd, err := cs.Get(errorAccess, errorStorage)

	require.NoError(t, err)
	require.Equal(t, expectedCredentials.SecretKey, crd.SecretKey)
}

func TestShouldGetCredentialFromCacheIfTTLIsNotExpired(t *testing.T) {
	expectedCredentials := &CredentialsStoreData{AccessKey: existingAccess, SecretKey: "secret_1", EOL: time.Now().Add(10 * time.Second)}
	cs := NewCredentialsStore(httpEndpoint, 10*time.Second)

	cs.cache.Store(cs.prepareKey(existingAccess, existingStorage), expectedCredentials)
	crd, err := cs.Get(existingAccess, existingStorage)

	require.NoError(t, err)
	require.Equal(t, expectedCredentials.SecretKey, crd.SecretKey)
}

func TestShouldUpdateCredentialsIfTTLIsExpired(t *testing.T) {
	oldCredentials := &CredentialsStoreData{AccessKey: existingAccess, SecretKey: "secret_1", EOL: time.Now().Add(-20 * time.Second)}

	cs := NewCredentialsStore(httpEndpoint, 10*time.Second)
	cs.cache.Store(cs.prepareKey(existingAccess, existingStorage), oldCredentials)
	crd, err := cs.Get(existingAccess, existingStorage)

	require.NoError(t, err)
	require.Equal(t, existingCredentials.SecretKey, crd.SecretKey)
}

func TestShouldDeleteCachedCredentialsOnErrCredentialsNotFound(t *testing.T) {
	accessKey := "no_access"
	storageType := "no_storage"
	oldCredentials := &CredentialsStoreData{AccessKey: accessKey, SecretKey: "secret_1", EOL: time.Now().Add(-10 * time.Second)}

	cs := NewCredentialsStore(httpEndpoint, 10*time.Second)
	cs.cache.Store(cs.prepareKey("not_existing", storageType), oldCredentials)
	crd, err := cs.Get(accessKey, storageType)

	require.Equal(t, ErrCredentialsNotFound, err)
	require.Nil(t, crd)
}

func TestShouldGetCredentialsFromCacheIfUpdateIsLocked(t *testing.T) {
	expectedCredentials := &CredentialsStoreData{AccessKey: existingAccess, SecretKey: "secret_1", EOL: time.Now().Add(-11 * time.Second)}
	cs := NewCredentialsStore(httpEndpoint, 10*time.Second)

	cs.cache.Store(cs.prepareKey(existingAccess, existingStorage), expectedCredentials)
	cs.lock.Lock()
	crd, err := cs.Get(existingAccess, existingStorage)

	require.NoError(t, err)
	require.Equal(t, expectedCredentials.SecretKey, crd.SecretKey)
}

func TestShouldGetAnErrorOnInvalidJSON(t *testing.T) {
	cs := NewCredentialsStore(httpEndpoint, 10*time.Second)

	crd, err := cs.Get(invalidAccess, invalidStorage)

	require.Error(t, err)
	require.Nil(t, crd)
}

func TestShouldGetAnErrorOnEmptyString(t *testing.T) {
	cs := NewCredentialsStore(httpEndpoint, 10*time.Second)

	crd, err := cs.Get(emptyAccess, emptyStorage)

	require.Error(t, err)
	require.Nil(t, crd)
}
