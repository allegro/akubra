package sharding

import (
	"errors"
	"fmt"
	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/akubra/regions/config"
	"github.com/allegro/akubra/internal/akubra/storages"
	"github.com/allegro/akubra/internal/akubra/utils"
	"github.com/allegro/akubra/internal/akubra/watchdog"
	"net/http"
	"time"
)

//ConsistentShardsRing is a shard ring that guarantees consistency based on the defined provided consistency level
type ConsistentShardsRing struct {
	watchdog          watchdog.ConsistencyWatchdog
	versionHeaderName string
	recordFactory     watchdog.ConsistencyRecordFactory
	shardsRing        ShardsRingAPI
}

type consistencyRequest struct {
	*http.Request
	*watchdog.ConsistencyRecord
	*watchdog.DeleteMarker
	isMultiPartUploadRequest         bool
	isInitiateMultipartUploadRequest bool
	consistencyLevel                 config.ConsistencyLevel
	isReadRepairOn                   bool
}

//GetRingProps returns props of the shard
func (consistentShardRing *ConsistentShardsRing) GetRingProps() *RingProps {
	return consistentShardRing.shardsRing.GetRingProps()
}

//Pick pcik shard for key
func (consistentShardRing *ConsistentShardsRing) Pick(key string) (storages.NamedShardClient, error) {
	return consistentShardRing.shardsRing.Pick(key)
}

//DoRequest performs the request and also records the request if the consistency level requires so
func (consistentShardRing *ConsistentShardsRing) DoRequest(req *http.Request) (*http.Response, error) {
	consistencyLevel, isReadRepairOn, err := extractRegionPropsFrom(req)
	if err != nil {
		return nil, err
	}
	consistencyRequest := &consistencyRequest{
		Request:                          req,
		isReadRepairOn:                   isReadRepairOn,
		consistencyLevel:                 consistencyLevel,
		isMultiPartUploadRequest:         utils.IsMultiPartUploadRequest(req),
		isInitiateMultipartUploadRequest: utils.IsInitiateMultiPartUploadRequest(req),
	}
	consistencyRequest, err = consistentShardRing.ensureConsistency(consistencyRequest)
	if err != nil {
		return nil, err
	}

	resp, err := consistentShardRing.shardsRing.DoRequest(consistencyRequest.Request)
	if err != nil {
		return nil, err
	}
	go consistentShardRing.awaitCompletion(consistencyRequest)

	if consistencyRequest.isInitiateMultipartUploadRequest {
		return consistentShardRing.logIfInitMultiPart(consistencyRequest, resp)
	}
	return resp, err
}

func extractRegionPropsFrom(request *http.Request) (config.ConsistencyLevel, bool, error) {
	consistencyLevelProp := request.Context().Value(watchdog.ConsistencyLevel)
	if consistencyLevelProp == nil {
		return "", false, errors.New("'ConsistencyLevel' not present in request's context")
	}
	consistencyLevel, castOk := consistencyLevelProp.(config.ConsistencyLevel)
	if !castOk {
		return "", false, errors.New("couldn't determine consistency level of region")

	}
	readRepairProp := request.Context().Value(watchdog.ReadRepair)
	if readRepairProp == nil {
		return "", false, errors.New("'ReadRepair' not present in request's context")
	}
	readRepair, castOk := readRepairProp.(bool)
	if !castOk {
		return "", false, errors.New("couldn't if read reapair should be used")

	}
	return consistencyLevel, readRepair, nil
}
func (consistentShardRing *ConsistentShardsRing) shouldLogRequest(consistencyRequest *consistencyRequest) bool {
	if consistentShardRing.watchdog == nil {
		return false
	}
	if consistencyRequest.consistencyLevel == config.None {
		return false
	}
	isObjectPath := utils.IsObjectPath(consistencyRequest.URL.Path)
	if http.MethodDelete == consistencyRequest.Request.Method && isObjectPath {
		return true
	}
	isPutOrInitMultiPart := (http.MethodPut == consistencyRequest.Request.Method && !consistencyRequest.isMultiPartUploadRequest) ||
		(http.MethodPost == consistencyRequest.Request.Method && consistencyRequest.isInitiateMultipartUploadRequest)

	return isPutOrInitMultiPart && isObjectPath
}

func (consistentShardRing *ConsistentShardsRing) logRequest(consistencyRequest *consistencyRequest) (*consistencyRequest, error) {
	if consistencyRequest.isInitiateMultipartUploadRequest {
		err := consistentShardRing.watchdog.SupplyRecordWithVersion(consistencyRequest.ConsistencyRecord)
		if err != nil {
			return nil, err
		}
	} else {
		deleteMarker, err := consistentShardRing.watchdog.Insert(consistencyRequest.ConsistencyRecord)
		if err != nil {
			return consistencyRequest, err
		}
		consistencyRequest.DeleteMarker = deleteMarker
	}
	if consistencyRequest.isInitiateMultipartUploadRequest {
		consistencyRequest.
			Header.
			Add(consistentShardRing.versionHeaderName, fmt.Sprintf("%d", consistencyRequest.ConsistencyRecord.ObjectVersion))
	}
	return consistencyRequest, nil
}

func (consistentShardRing *ConsistentShardsRing) logMultipart(consistencyRequest *consistencyRequest, resp *http.Response) error {
	multiPartUploadID, err := utils.ExtractMultiPartUploadIDFrom(resp)
	if err != nil {
		return fmt.Errorf("failed on extracting multipart upload ID from response: %s", err)
	}
	consistencyRequest.ConsistencyRecord.RequestID = multiPartUploadID
	_, err = consistentShardRing.watchdog.Insert(consistencyRequest.ConsistencyRecord)
	if err != nil {
		return err
	}
	return nil
}

func (consistentShardRing *ConsistentShardsRing) ensureConsistency(consistencyRequest *consistencyRequest) (*consistencyRequest, error) {
	if !consistentShardRing.shouldLogRequest(consistencyRequest) {
		return consistencyRequest, nil
	}

	consistencyRecord, err := consistentShardRing.recordFactory.CreateRecordFor(consistencyRequest.Request)
	if err != nil {
		if config.Strong == consistencyRequest.consistencyLevel {
			return nil, err
		}
		return consistencyRequest, nil
	}
	consistencyRequest.ConsistencyRecord = consistencyRecord

	loggedRequest, err := consistentShardRing.logRequest(consistencyRequest)
	if err != nil {
		if config.Strong == consistencyRequest.consistencyLevel {
			return nil, err
		}
		return consistencyRequest, nil
	}
	return loggedRequest, nil
}

func (consistentShardRing *ConsistentShardsRing) logIfInitMultiPart(consistencyRequest *consistencyRequest, response *http.Response) (*http.Response, error) {
	if consistencyRequest.consistencyLevel != config.None {
		err := consistentShardRing.logMultipart(consistencyRequest, response)
		if err != nil && consistencyRequest.consistencyLevel == config.Strong {
			return nil, err
		}
	}
	return response, nil
}

func (consistentShardRing *ConsistentShardsRing) updateExecutionDelay(request *http.Request) {
	reqQuery := request.URL.Query()
	uploadID, _ := reqQuery["uploadId"]
	delta := &watchdog.ExecutionDelay{
		RequestID: uploadID[0],
		Delay:     time.Minute * 5,
	}
	err := consistentShardRing.watchdog.UpdateExecutionDelay(delta)
	if err != nil {
		log.Printf("Failed to update multipart's execution time, reqId = %s, error: %s",
			request.Context().Value(log.ContextreqIDKey), err)
		return
	}
	log.Debugf("Updated execution time for req '%s'", request.Context().Value(log.ContextreqIDKey))
}

func (consistentShardRing *ConsistentShardsRing) performReadRepair(consistencyRequest *consistencyRequest) {
	objectVersion := consistencyRequest.Context().Value(watchdog.ReadRepairObjectVersion).(*int)
	if objectVersion == nil {
		log.Debugf("Can't perform read repair, no version header found, reqID %s", consistencyRequest.Context().Value(log.ContextreqIDKey))
		return
	}
	record, err := consistentShardRing.recordFactory.CreateRecordFor(consistencyRequest.Request)
	if err != nil {
		log.Debugf("Failed to perform read repair, couldn't consistencyRequesteate log record, reqID %s : %s", consistencyRequest.Context().Value(log.ContextreqIDKey), err)
		return
	}
	record.ObjectVersion = *objectVersion
	_, err = consistentShardRing.watchdog.Insert(record)
	if err != nil {
		log.Debugf("Failed to perform read repair for object %s in domain %s: %s", record.ObjectID, record.Domain, err)
	}
	log.Debugf("Performed read repair for object %s in domain %s: %s", record.ObjectID, record.Domain, err)
}

func (consistentShardRing *ConsistentShardsRing) awaitCompletion(consistencyRequest *consistencyRequest) {
	<-consistencyRequest.Context().Done()

	reqID := consistencyRequest.Context().Value(log.ContextreqIDKey)
	readRepairVersion, readRepairCastOk := consistencyRequest.Context().Value(watchdog.ReadRepairObjectVersion).(*int)
	noErrorsDuringRequestProcessing, errorsFlagCastOk := consistencyRequest.Context().Value(watchdog.NoErrorsDuringRequest).(*bool)
	successfulMultiPart, multiPartFlagCastOk := consistencyRequest.Context().Value(watchdog.MultiPartUpload).(*bool)

	if shouldPerformReadRepair(readRepairVersion, readRepairCastOk) {
		consistentShardRing.performReadRepair(consistencyRequest)
		return
	}
	if isSuccessfulMultipart(successfulMultiPart, multiPartFlagCastOk) {
		consistentShardRing.updateExecutionDelay(consistencyRequest.Request)
		return
	}
	if wasReplicationSuccessful(consistencyRequest, noErrorsDuringRequestProcessing, errorsFlagCastOk) {
		err := consistentShardRing.watchdog.Delete(consistencyRequest.DeleteMarker)
		if err != nil {
			log.Printf("Failed to delete records older than record for request %s: %s", reqID, err)
		}
	}
}

func wasReplicationSuccessful(request *consistencyRequest, noErrorsDuringRequestProcessing *bool, castOk bool) bool {
	return castOk && noErrorsDuringRequestProcessing != nil && *noErrorsDuringRequestProcessing && request.DeleteMarker != nil
}

func isSuccessfulMultipart(successfulMultiPart *bool, castResult bool) bool {
	return castResult && successfulMultiPart != nil && *successfulMultiPart
}

func shouldPerformReadRepair(readRepairVersion *int, readRepairPropertyCastSuccessful bool) bool {
	return readRepairPropertyCastSuccessful && readRepairVersion != nil && *readRepairVersion != -1
}

//NewShardingAPI wraps the provided sharingAPI with ConsistentShardsRing to ensure consistency
func NewShardingAPI(shardingAPI ShardsRingAPI,
	consistencyWatchdgo watchdog.ConsistencyWatchdog,
	recordFactory watchdog.ConsistencyRecordFactory,
	versionHeaderName string) ShardsRingAPI {

	return &ConsistentShardsRing{
		watchdog:          consistencyWatchdgo,
		recordFactory:     recordFactory,
		shardsRing:        shardingAPI,
		versionHeaderName: versionHeaderName,
	}
}
