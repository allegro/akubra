package storages

import (
	"bytes"
	"context"
	"fmt"
	"github.com/allegro/akubra/internal/akubra/regions/config"
	"github.com/allegro/akubra/internal/akubra/watchdog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/tools/go/ssa/interp/testdata/src/errors"
	"io/ioutil"
	"net/http"
	"testing"
)

type WatchdogMock struct {
	*mock.Mock
}

type ShardClientMock struct {
	*mock.Mock
}

func TestInsertingRecordsBasedOnTheRequest(t *testing.T) {
	versionHeaderName := "x-watchdog-version"
	for _, testCase := range []struct {
		method             string
		url                string
		consistencyLevel   config.ConsistencyLevel
		shouldInsertRecord bool
		isMultiPart        bool
	}{
		{method: http.MethodPut, url: "http://localhost/newBucket", consistencyLevel: config.Strong, shouldInsertRecord: false},
		{method: http.MethodPut, url: "http://localhost/newBucket", consistencyLevel: config.Weak, shouldInsertRecord: false},
		{method: http.MethodPut, url: "http://localhost/newBucket", consistencyLevel: config.None, shouldInsertRecord: false},
		{method: http.MethodPut, url: "http://localhost/newBucket/objectg", consistencyLevel: config.Strong, shouldInsertRecord: true},
		{method: http.MethodPut, url: "http://localhost/newBucket/objectg", consistencyLevel: config.Weak, shouldInsertRecord: true},
		{method: http.MethodPut, url: "http://localhost/newBucket/objectg", consistencyLevel: config.None, shouldInsertRecord: false},
		{method: http.MethodGet, url: "http://localhost/newBucket/objectg", consistencyLevel: config.Strong, shouldInsertRecord: false},
		{method: http.MethodGet, url: "http://localhost/newBucket/objectg?acl", consistencyLevel: config.Strong, shouldInsertRecord: false},
		{method: http.MethodPut, url: "http://localhost/newBucket/objectg?acl", consistencyLevel: config.Strong, shouldInsertRecord: true},
		{method: http.MethodPost, url: "http://localhost/newBucket/objectg?uploads", consistencyLevel: config.Strong, shouldInsertRecord: true, isMultiPart: true},
		{method: http.MethodPost, url: "http://localhost/newBucket/objectg?partNumber=1", consistencyLevel: config.Strong, shouldInsertRecord: false, isMultiPart: true},
	} {
		shardMock := &ShardClientMock{&mock.Mock{}}
		factoryMock := &ConsistencyRecordFactoryMock{&mock.Mock{}}
		watchdogMock := &WatchdogMock{&mock.Mock{}}

		consistentShard := ConsistencyShardClient{
			watchdog:          watchdogMock,
			shard:             shardMock,
			recordFactory:     factoryMock,
			versionHeaderName: versionHeaderName,
		}

		request, err := http.NewRequest(testCase.method, testCase.url, nil)
		request = request.WithContext(context.WithValue(request.Context(), watchdog.ConsistencyLevel, testCase.consistencyLevel))
		request = request.WithContext(context.WithValue(request.Context(), watchdog.ReadRepair, false))
		assert.NotNil(t, request)
		assert.Nil(t, err)

		response := &http.Response{Request: request, StatusCode: http.StatusOK}
		shardMock.On("RoundTrip", request).Return(response, nil)

		consistencyRecord := &watchdog.ConsistencyRecord{}
		factoryMock.On("CreateRecordFor", request).Return(consistencyRecord, nil)

		watchdogMock.On("Insert", consistencyRecord).Return(nil, nil)

		if testCase.isMultiPart && testCase.shouldInsertRecord {
			response.Body = ioutil.NopCloser(bytes.NewBuffer([]byte(initiateMultiPartResponse)))
			watchdogMock.On("SupplyRecordWithVersion", consistencyRecord).Return(nil)
		}

		resp, err := consistentShard.RoundTrip(request)
		assert.Nil(t, err)
		assert.NotNil(t, resp)

		shardMock.AssertCalled(t, "RoundTrip", request)
		if testCase.shouldInsertRecord {
			if testCase.isMultiPart {
				watchdogMock.AssertCalled(t, "SupplyRecordWithVersion", consistencyRecord)
			}
			factoryMock.AssertCalled(t, "CreateRecordFor", request)
			watchdogMock.AssertCalled(t, "Insert", consistencyRecord)
			assert.NotEmpty(t, request.Header.Get(versionHeaderName))
		} else {
			factoryMock.AssertNotCalled(t, "CreateRecordFor", request)
			watchdogMock.AssertNotCalled(t, "Insert", consistencyRecord)
			assert.Empty(t, request.Header.Get(versionHeaderName))
		}
	}
}

func TestRecordCompaction(t *testing.T) {
	versionHeaderName := "x-watchdog-version"
	for _, noErrorsOccurredDuringRequestProcessing := range []bool{true, false} {
		shardMock := &ShardClientMock{&mock.Mock{}}
		factoryMock := &ConsistencyRecordFactoryMock{&mock.Mock{}}
		watchdogMock := &WatchdogMock{&mock.Mock{}}

		consistentShard := ConsistencyShardClient{
			watchdog:          watchdogMock,
			shard:             shardMock,
			recordFactory:     factoryMock,
			versionHeaderName: versionHeaderName,
		}

		deleteMarker := &watchdog.DeleteMarker{}
		request, err := http.NewRequest(http.MethodPut, "http:/localhost:8080/bukcet/obj", nil)
		request = request.WithContext(context.WithValue(request.Context(), watchdog.NoErrorsDuringRequest, &noErrorsOccurredDuringRequestProcessing))
		ctx, cancel := context.WithCancel(request.Context())
		request = request.WithContext(ctx)
		cancel()

		watchdogMock.On("Delete", deleteMarker).Return(nil)
		consistencyRequest := &consistencyRequest{DeleteMarker: deleteMarker, Request: request}

		assert.NotNil(t, request)
		assert.Nil(t, err)

		consistentShard.awaitCompletion(consistencyRequest)

		if noErrorsOccurredDuringRequestProcessing {
			watchdogMock.AssertCalled(t, "Delete", deleteMarker)
		} else {
			watchdogMock.AssertNotCalled(t, "Delete", deleteMarker)
		}
	}
}

func TestReadRepair(t *testing.T) {
	versionHeaderName := "x-watchdog-version"
	for _, objectVersionToPerformReadRepairOn := range []int{-1, 123} {
		shardMock := &ShardClientMock{&mock.Mock{}}
		factoryMock := &ConsistencyRecordFactoryMock{&mock.Mock{}}
		watchdogMock := &WatchdogMock{&mock.Mock{}}

		consistentShard := ConsistencyShardClient{
			watchdog:          watchdogMock,
			shard:             shardMock,
			recordFactory:     factoryMock,
			versionHeaderName: versionHeaderName,
		}

		request, err := http.NewRequest(http.MethodGet, "http:/localhost:8080/bukcet/obj", nil)
		assert.NotNil(t, request)
		assert.Nil(t, err)

		objVersion := ""
		if objectVersionToPerformReadRepairOn > 0 {
			objVersion = fmt.Sprintf("%d", objectVersionToPerformReadRepairOn)
		}
		request = request.WithContext(context.WithValue(request.Context(), watchdog.ReadRepairObjectVersion, &objVersion))
		ctx, cancel := context.WithCancel(request.Context())
		request = request.WithContext(ctx)
		cancel()

		consistencyRequest := &consistencyRequest{Request: request}

		consistencyRecord := &watchdog.ConsistencyRecord{}
		readRepairRecord := consistencyRecord
		readRepairRecord.ObjectVersion = objectVersionToPerformReadRepairOn

		factoryMock.On("CreateRecordFor", request).Return(consistencyRecord, nil)
		watchdogMock.On("Insert", readRepairRecord).Return(nil, nil)

		consistentShard.awaitCompletion(consistencyRequest)

		if objectVersionToPerformReadRepairOn == -1 {
			factoryMock.AssertNotCalled(t, "CreateRecordFor", request)
			watchdogMock.AssertNotCalled(t, "Insert", readRepairRecord)
		} else {
			factoryMock.AssertCalled(t, "CreateRecordFor", request)
			watchdogMock.AssertCalled(t, "Insert", readRepairRecord)
		}
	}
}

func TestConsistencyLevels(t *testing.T) {
	versionHeaderName := "x-watchdog-version"
	for _, testCase := range []struct {
		consistencyLevel  config.ConsistencyLevel
		shouldInsertFail  bool
		shouldRequestFail bool
	}{
		{consistencyLevel: config.Strong, shouldInsertFail: true, shouldRequestFail: true},
		{consistencyLevel: config.Strong, shouldInsertFail: false, shouldRequestFail: false},
		{consistencyLevel: config.Weak, shouldInsertFail: true, shouldRequestFail: false},
		{consistencyLevel: config.Weak, shouldInsertFail: true, shouldRequestFail: false},
	} {
		shardMock := &ShardClientMock{&mock.Mock{}}
		factoryMock := &ConsistencyRecordFactoryMock{&mock.Mock{}}
		watchdogMock := &WatchdogMock{&mock.Mock{}}

		consistentShard := ConsistencyShardClient{
			watchdog:          watchdogMock,
			shard:             shardMock,
			recordFactory:     factoryMock,
			versionHeaderName: versionHeaderName,
		}

		request, err := http.NewRequest(http.MethodPut, "http://localhost:8080/bucket/object", nil)
		request = request.WithContext(context.WithValue(request.Context(), watchdog.ConsistencyLevel, testCase.consistencyLevel))
		request = request.WithContext(context.WithValue(request.Context(), watchdog.ReadRepair, false))
		assert.NotNil(t, request)
		assert.Nil(t, err)

		response := &http.Response{Request: request, StatusCode: http.StatusOK}
		shardMock.On("RoundTrip", request).Return(response, nil)

		consistencyRecord := &watchdog.ConsistencyRecord{}
		factoryMock.On("CreateRecordFor", request).Return(consistencyRecord, nil)

		if testCase.shouldInsertFail {
			watchdogMock.On("Insert", consistencyRecord).Return(nil, errors.New("error"))
		} else {
			watchdogMock.On("Insert", consistencyRecord).Return(nil, nil)
		}

		_, err = consistentShard.RoundTrip(request)

		factoryMock.AssertCalled(t, "CreateRecordFor", request)
		watchdogMock.AssertCalled(t, "Insert", consistencyRecord)

		if testCase.shouldRequestFail {
			assert.Equal(t, err.Error(), "error")
		} else {
			shardMock.AssertCalled(t, "RoundTrip", request)
			assert.Nil(t, err)
		}
	}
}

func (shardMock *ShardClientMock) RoundTrip(req *http.Request) (resp *http.Response, rerr error) {
	args := shardMock.Called(req)
	r := args.Get(0)
	if r != nil {
		return r.(*http.Response), args.Error(1)
	}
	return nil, args.Error(1)
}

func (shardMock *ShardClientMock) Backends() []*StorageClient {
	backends := shardMock.Called().Get(0)
	if backends != nil {
		return backends.([]*StorageClient)
	}
	return nil
}

func (shardMock *ShardClientMock) Name() string {
	return shardMock.Called().Get(0).(string)
}

func (wm *WatchdogMock) Insert(record *watchdog.ConsistencyRecord) (*watchdog.DeleteMarker, error) {
	args := wm.Called(record)
	arg0 := args.Get(0)
	var deleteMarker *watchdog.DeleteMarker
	if arg0 != nil {
		deleteMarker = arg0.(*watchdog.DeleteMarker)
	}
	err := args.Error(1)
	return deleteMarker, err
}

func (wm *WatchdogMock) Delete(marker *watchdog.DeleteMarker) error {
	args := wm.Called(marker)
	return args.Error(0)
}

func (wm *WatchdogMock) UpdateExecutionDelay(delta *watchdog.ExecutionDelay) error {
	args := wm.Called(delta)
	return args.Error(0)
}

func (wm *WatchdogMock) SupplyRecordWithVersion(record *watchdog.ConsistencyRecord) error {
	args := wm.Called(record)
	return args.Error(0)
}

type ConsistencyRecordFactoryMock struct {
	*mock.Mock
}

func (fm *ConsistencyRecordFactoryMock) CreateRecordFor(request *http.Request) (*watchdog.ConsistencyRecord, error) {
	args := fm.Called(request)
	record := args.Get(0).(*watchdog.ConsistencyRecord)
	err := args.Error(1)
	return record, err
}


var initiateMultiPartResponse = `
<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult
xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Bucket>example-bucket</Bucket>
   <Key>example-object</Key>
   <UploadId>EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-</UploadId>
</InitiateMultipartUploadResult>`