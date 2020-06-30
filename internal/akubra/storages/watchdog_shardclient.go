package storages

import (
	"errors"
	"fmt"
	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/akubra/regions/config"
	"github.com/allegro/akubra/internal/akubra/utils"
	"github.com/allegro/akubra/internal/akubra/watchdog"
	"net/http"
	"strconv"
	"time"
)

//ConsistencyShardClient is a shard that guarantees consistency based on the defined provided consistency level
type ConsistencyShardClient struct {
	watchdog          watchdog.ConsistencyWatchdog
	versionHeaderName string
	recordFactory     watchdog.ConsistencyRecordFactory
	shard             NamedShardClient
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

//Name returns the name of the shard
func (consistencyShard *ConsistencyShardClient) Name() string {
	return consistencyShard.shard.Name()
}

//Backends returns the backends of a shard
func (consistencyShard *ConsistencyShardClient) Backends() []*StorageClient {
	return consistencyShard.shard.Backends()
}

//RoundTrip performs the request and also records the request if the consistency level requires so
func (consistencyShard *ConsistencyShardClient) RoundTrip(req *http.Request) (*http.Response, error) {
	//log.Debug("Request in ConsistencyShardClient %s", utils.RequestID(req))
	//defer log.Debug("Request out ConsistencyShardClient %s", utils.RequestID(req))
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
	consistencyRequest, err = consistencyShard.ensureConsistency(consistencyRequest)
	if err != nil {
		return nil, err
	}

	resp, err := consistencyShard.shard.RoundTrip(consistencyRequest.Request)
	if err != nil {
		return nil, err
	}
	go consistencyShard.awaitCompletion(consistencyRequest)

	if consistencyRequest.isInitiateMultipartUploadRequest {
		return consistencyShard.logIfInitMultiPart(consistencyRequest, resp)
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
func (consistencyShard *ConsistencyShardClient) shouldLogRequest(consistencyRequest *consistencyRequest) bool {
	if consistencyShard.watchdog == nil {
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

func (consistencyShard *ConsistencyShardClient) logRequest(consistencyRequest *consistencyRequest) (*consistencyRequest, error) {
	if consistencyRequest.isInitiateMultipartUploadRequest {
		err := consistencyShard.watchdog.SupplyRecordWithVersion(consistencyRequest.ConsistencyRecord)
		if err != nil {
			return nil, err
		}
	} else {
		deleteMarker, err := consistencyShard.watchdog.Insert(consistencyRequest.ConsistencyRecord)
		if err != nil {
			return consistencyRequest, err
		}
		consistencyRequest.DeleteMarker = deleteMarker
	}
	consistencyRequest.
		Header.
		Add(consistencyShard.versionHeaderName, fmt.Sprintf("%d", consistencyRequest.ConsistencyRecord.ObjectVersion))
	return consistencyRequest, nil
}

func (consistencyShard *ConsistencyShardClient) logMultipart(consistencyRequest *consistencyRequest, resp *http.Response) error {
	multiPartUploadID, err := utils.ExtractMultiPartUploadIDFrom(resp)
	if err != nil {
		return fmt.Errorf("failed on extracting multipart upload ID from response: %s", err)
	}
	consistencyRequest.ConsistencyRecord.RequestID = multiPartUploadID
	_, err = consistencyShard.watchdog.Insert(consistencyRequest.ConsistencyRecord)
	if err != nil {
		return err
	}
	return nil
}

func (consistencyShard *ConsistencyShardClient) ensureConsistency(consistencyRequest *consistencyRequest) (*consistencyRequest, error) {
	if !consistencyShard.shouldLogRequest(consistencyRequest) {
		return consistencyRequest, nil
	}

	consistencyRecord, err := consistencyShard.recordFactory.CreateRecordFor(consistencyRequest.Request)
	if err != nil {
		if config.Strong == consistencyRequest.consistencyLevel {
			return nil, err
		}
		return consistencyRequest, nil
	}
	consistencyRequest.ConsistencyRecord = consistencyRecord

	loggedRequest, err := consistencyShard.logRequest(consistencyRequest)
	if err != nil {
		if config.Strong == consistencyRequest.consistencyLevel {
			return nil, err
		}
		return consistencyRequest, nil
	}
	return loggedRequest, nil
}

func (consistencyShard *ConsistencyShardClient) logIfInitMultiPart(consistencyRequest *consistencyRequest, response *http.Response) (*http.Response, error) {
	if consistencyRequest.consistencyLevel != config.None {
		err := consistencyShard.logMultipart(consistencyRequest, response)
		if err != nil && consistencyRequest.consistencyLevel == config.Strong {
			return nil, err
		}
	}
	return response, nil
}

func (consistencyShard *ConsistencyShardClient) updateExecutionDelay(request *http.Request) {
	reqQuery := request.URL.Query()
	uploadID := reqQuery["uploadId"]
	delta := &watchdog.ExecutionDelay{
		RequestID: uploadID[0],
		Delay:     time.Minute * 5,
	}
	err := consistencyShard.watchdog.UpdateExecutionDelay(delta)
	if err != nil {
		log.Printf("Failed to update multipart's execution time, reqId = %s, error: %s",
			request.Context().Value(log.ContextreqIDKey), err)
		return
	}
	log.Debugf("Updated execution time for req '%s'", request.Context().Value(log.ContextreqIDKey))
}

func (consistencyShard *ConsistencyShardClient) performReadRepair(consistencyRequest *consistencyRequest) {
	objectVersionValue := consistencyRequest.Context().Value(watchdog.ReadRepairObjectVersion).(*string)

	if objectVersionValue == nil {
		log.Debugf("Can't perform read repair, no version header found, reqID %s", consistencyRequest.Context().Value(log.ContextreqIDKey))
		return
	}

	objectVersion, err := strconv.ParseInt(*objectVersionValue, 10, 64)
	if err != nil {
		log.Debugf("Can't perform read repair, failed to parse objectVersion, reqID %s", consistencyRequest.Context().Value(log.ContextreqIDKey))
		return
	}

	record, err := consistencyShard.recordFactory.CreateRecordFor(consistencyRequest.Request)
	if err != nil {
		log.Debugf("Failed to perform read repair, couldn't consistencyRequesteate log record, reqID %s : %s", consistencyRequest.Context().Value(log.ContextreqIDKey), err)
		return
	}
	record.ObjectVersion = int(objectVersion)
	_, err = consistencyShard.watchdog.Insert(record)
	if err != nil {
		log.Debugf("Failed to perform read repair for object %s in domain %s: %s", record.ObjectID, record.Domain, err)
	}
	log.Debugf("Performed read repair for object %s in domain %s: %s", record.ObjectID, record.Domain, err)
}

func (consistencyShard *ConsistencyShardClient) awaitCompletion(consistencyRequest *consistencyRequest) {
	<-consistencyRequest.Context().Done()

	reqID := consistencyRequest.Context().Value(log.ContextreqIDKey)
	readRepairVersion, readRepairCastOk := consistencyRequest.Context().Value(watchdog.ReadRepairObjectVersion).(*string)
	noErrorsDuringRequestProcessing, errorsFlagCastOk := consistencyRequest.Context().Value(watchdog.NoErrorsDuringRequest).(*bool)
	successfulMultiPart, multiPartFlagCastOk := consistencyRequest.Context().Value(watchdog.MultiPartUpload).(*bool)

	if shouldPerformReadRepair(readRepairVersion, readRepairCastOk) {
		consistencyShard.performReadRepair(consistencyRequest)
		return
	}
	if isSuccessfulMultipart(successfulMultiPart, multiPartFlagCastOk) {
		consistencyShard.updateExecutionDelay(consistencyRequest.Request)
		return
	}
	if wasReplicationSuccessful(consistencyRequest, noErrorsDuringRequestProcessing, errorsFlagCastOk) {
		err := consistencyShard.watchdog.Delete(consistencyRequest.DeleteMarker)
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

func shouldPerformReadRepair(readRepairVersion *string, readRepairPropertyCastSuccessful bool) bool {
	return readRepairPropertyCastSuccessful && readRepairVersion != nil && *readRepairVersion != ""
}

// NewConsistentShard wraps the provided shard with ConsistencyShardClient to ensure consistency
func NewConsistentShard(shardClient NamedShardClient,
	consistencyWatchdgo watchdog.ConsistencyWatchdog,
	recordFactory watchdog.ConsistencyRecordFactory,
	versionHeaderName string) NamedShardClient {

	return &ConsistencyShardClient{
		watchdog:          consistencyWatchdgo,
		recordFactory:     recordFactory,
		shard:             shardClient,
		versionHeaderName: versionHeaderName,
	}
}
