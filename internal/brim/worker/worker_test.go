package worker

import (
	"fmt"
	"github.com/allegro/akubra/internal/brim/model"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	"github.com/allegro/akubra/internal/akubra/watchdog"
	"github.com/stretchr/testify/assert"
)

func TestShouldPerformNoMigrationIfThereAreNoDestClientsDefined(t *testing.T) {
	taskChannel := make(chan *model.WALTask, 3)

	tasksWG := sync.WaitGroup{}
	tasksWG.Add(2)
	recordProcessedFunc := func(_ *watchdog.ConsistencyRecord, err error) error {
		tasksWG.Done()
		assert.NoError(t, err)
		return nil
	}

	noopPut := &model.WALTask{
		DestinationsClients: []*s3.S3{},
		WALEntry: &model.WALEntry{
			Record:              &watchdog.ConsistencyRecord{Method: watchdog.PUT},
			RecordProcessedHook: recordProcessedFunc}}

	noopDelete := &model.WALTask{
		DestinationsClients: []*s3.S3{},
		WALEntry: &model.WALEntry{
			Record:              &watchdog.ConsistencyRecord{Method: watchdog.DELETE},
			RecordProcessedHook: recordProcessedFunc}}

	taskChannel <- noopPut
	taskChannel <- noopDelete

	worker := NewTaskMigratorWALWorker(2)
	worker.Process(taskChannel)

	tasksWG.Wait()
}

func TestMigrations(t *testing.T) {

	for _, migrationScenario := range []struct {
		testName                    string
		desiredVersion              int
		method                      string
		numberOfRequests            int
		shouldAtLeastDstStorageFail bool
		expectMultipart             bool
	}{
		{testName: "Successful PUT", desiredVersion: 987654321, method: "PUT", numberOfRequests: 2},
		{testName: "Failed PUT", desiredVersion: 987654321, method: "PUT", numberOfRequests: 2, shouldAtLeastDstStorageFail: true},
		{testName: "Successful DELETE", desiredVersion: 987654321, method: "DELETE", numberOfRequests: 2},
		{testName: "Failed DELETE", desiredVersion: 987654321, method: "DELETE", numberOfRequests: 2, shouldAtLeastDstStorageFail: true},
		{testName: "Success Multipart", desiredVersion: 987654321, method: "PUT", numberOfRequests: 2, expectMultipart: true},
	} {
		fmt.Printf("Running '%s' test case\n", migrationScenario.testName)
		taskChannel := make(chan *model.WALTask)

		tasksWG := sync.WaitGroup{}
		tasksWG.Add(1)
		numberOfReq := 0
		mutex := &sync.Mutex{}

		var srcCli *s3.S3
		var srcStorage *httptest.Server

		objSize := 4
		if migrationScenario.expectMultipart {
			objSize = 20
		}

		if migrationScenario.method == "PUT" {
			srcStorage = prepareSrcServer(migrationScenario.desiredVersion, objSize, t)
			srcCli = s3.New(aws.Auth{AccessKey: "123", SecretKey: "321"},
				aws.Region{Name: "generic", S3Endpoint: srcStorage.URL})
		}

		var dstClients []*s3.S3
		var dstStorages []*httptest.Server
		for i := 0; i < migrationScenario.numberOfRequests; i++ {
			dstStorage := prepareDstServer(migrationScenario.method, objSize, migrationScenario.expectMultipart, migrationScenario.desiredVersion, &numberOfReq, mutex, t)
			dstStorages = append(dstStorages, dstStorage)

			dstClient := s3.New(aws.Auth{AccessKey: "123", SecretKey: "321"},
				aws.Region{Name: "generic", S3Endpoint: dstStorage.URL})
			dstClients = append(dstClients, dstClient)
		}

		if migrationScenario.shouldAtLeastDstStorageFail {
			dstStorages[len(dstStorages)/2].Close()
		}

		var migrationError error
		migration := &model.WALTask{
			SourceClient:        srcCli,
			DestinationsClients: dstClients,
			WALEntry: &model.WALEntry{
				Record: &watchdog.ConsistencyRecord{
					Method:        watchdog.Method(migrationScenario.method),
					AccessKey:     "123",
					ObjectID:      "bucket/key",
					ObjectVersion: migrationScenario.desiredVersion},
				RecordProcessedHook: func(_ *watchdog.ConsistencyRecord, err error) error {
					defer tasksWG.Done()
					if migrationError == nil {
						migrationError = err
					}
					return nil
				}}}

		worker := NewTaskMigratorWALWorker(1)
		worker.SetMultiPartThresholdInBytes(10)
		worker.Process(taskChannel)

		taskChannel <- migration
		tasksWG.Wait()

		if migrationScenario.shouldAtLeastDstStorageFail {
			assert.Error(t, migrationError)
		} else {
			assert.Equal(t, numberOfReq, migrationScenario.numberOfRequests)
			assert.NoError(t, migrationError)
		}
	}
}

func prepareDstServer(expectedMethod string, objSize int, expectMultiPart bool, expectedVersion int,
	numberOfReq *int, mutex *sync.Mutex, t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		body, err := ioutil.ReadAll(req.Body)
		assert.NoError(t, err)

		if expectedMethod == "PUT" {
			assert.Equal(t, body, []byte(strings.Repeat("X", objSize)))
			if !expectMultiPart {
				assert.Equal(t, req.Method, expectedMethod)
				assert.Equal(t, req.URL.Path, "/bucket/key")
				assert.Equal(t, req.Header.Get("x-amz-meta-obj-version"), string(expectedVersion))

			}
		}

		if expectMultiPart {
			assert.True(t, (req.Method == http.MethodPost && req.Header.Get("x-amz-meta-obj-version") == string(expectedVersion)) ||
				req.Method == http.MethodPut ||
				req.Method == http.MethodHead)

		}

		assert.True(t, strings.HasPrefix(req.Header.Get("Authorization"), "AWS 123:"))

		if http.MethodHead == req.Method {
			_, _ = rw.Write([]byte(""))
		} else {
			_, _ = rw.Write([]byte("OK"))
		}

		mutex.Lock()
		defer mutex.Unlock()
		*numberOfReq++
	}))
}
func prepareSrcServer(objVersion int, objSize int, t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		assert.True(t, http.MethodGet == req.Method || http.MethodHead == req.Method)
		assert.True(t, strings.HasPrefix(req.URL.Path, "/bucket/key"))
		assert.True(t, strings.HasPrefix(req.Header.Get("Authorization"), "AWS 123:"))
		if req.URL.RawQuery == "acl=" {
			_, _ = rw.Write([]byte(bucketACLResponse))
		} else {
			rw.Header().Set("x-amz-meta-obj-version", string(objVersion))
			rw.WriteHeader(200)
			if http.MethodGet == req.Method {
				rw.Header().Set("Content-Length", fmt.Sprintf("%d", objSize))
				_, _ = rw.Write([]byte(strings.Repeat("X", objSize)))
			} else {
				_, _ = rw.Write([]byte(""))

			}
		}
	}))
}

const (
	bucketACLResponse = `
<?xml version="1.0" encoding="UTF-8"?>
<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>*** Owner-Canonical-User-ID ***</ID>
    <DisplayName>owner-display-name</DisplayName>
  </Owner>
  <AccessControlList>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xsi:type="Canonical User">
        <ID>*** Owner-Canonical-User-ID ***</ID>
        <DisplayName>display-name</DisplayName>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
  </AccessControlList>
</AccessControlPolicy>
`
)
