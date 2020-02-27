package s3

import (
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/allegro/akubra/internal/brim/model"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"

	// "github.com/allegro/akubra/internal/akubra/config"
	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/akubra/metrics"
)

const (
	// ErrMsgNotFound issued if object does not exist
	ErrMsgNotFound = "404 Not Found"
	// AmzMetadataPrefix is constant for s3 specific header prefix
	AmzMetadataPrefix = "x-amz-meta-"
)

// MigrationAuth keeps auth data
type MigrationAuth struct {
	Endpoint  string
	AccessKey string
	SecretKey string
}

type s3Object struct {
	path          string
	data          io.ReadCloser
	contentLength int64
	headers       http.Header
	contentType   string
	perm          s3.ACL
	options       s3.Options
}

func (obj s3Object) cleanUp() error {
	if obj.data == nil {
		return nil
	}
	var err error
	if _, err = io.Copy(ioutil.Discard, obj.data); err != nil {
		log.Printf("Source object body drain error %s: %s", obj.path, err)
	}
	if err = obj.data.Close(); err != nil {
		log.Printf("Source object body close error %s: %s", obj.path, err)
	}
	return err
}

// GetS3Client returns s3 client for given MigrationAuth
func GetS3Client(s3Auth *MigrationAuth) *s3.S3 {
	cli := s3.New(aws.Auth{AccessKey: s3Auth.AccessKey, SecretKey: s3Auth.SecretKey},
		aws.Region{Name: "generic", S3Endpoint: s3Auth.Endpoint})
	return cli
}

func extractContentTypeAndLength(headers http.Header, multipart bool) (contentType string, contentLength int64, err error) {
	contentLengthValue := headers.Get("Content-Length")
	if contentLengthValue == "" {
		err = ErrZeroContentLenthValue
		return
	}
	contentLength, err = strconv.ParseInt(contentLengthValue, 10, 64)
	if err != nil || contentLength == 0 {
		err = ErrZeroContentLenthValue
		return
	}
	if (contentLength > objectSizeLimit) && !multipart {
		err = ErrContentLengthMaxValue
		return
	}
	contentType = headers.Get("Content-Type")
	return
}

// MigrationTaskResult keeps track on migration task
type MigrationTaskResult struct {
	Tid                uint64
	Pid                uint64
	ErrorType          model.ErrorType
	Error              error
	NowToCreatedAtDiff int64
	Retryable          bool
	Success            bool
	IsPermanent        bool
}

// NewMigrationTaskResult creates MigrationTaskResult
func NewMigrationTaskResult(task model.MigrationTaskItem) MigrationTaskResult {
	return MigrationTaskResult{
		Tid:     task.Tid,
		Pid:     uint64(task.Pid.Int64),
		Success: false,
	}
}

var nonRetryableErrorTypes = map[model.ErrorType]struct{}{
	model.TaskError:        {},
	model.PermissionsError: {},
	model.CredentialsError: {},
}

// MarkRetry checks if error should terminate task execution
func (mtr *MigrationTaskResult) MarkRetry(errType model.ErrorType, err error) {
	mtr.Retryable = true
	if _, isNotRetryable := nonRetryableErrorTypes[errType]; isNotRetryable {
		mtr.Retryable = false
		return
	}
	if (err == ErrZeroContentLenthValue) || (err == ErrEmptyContentType) {
		mtr.Retryable = false
		return
	}
	s3Err, ok := err.(*s3.Error)
	if ok {
		if s3Err.StatusCode == http.StatusNotFound {
			mtr.Retryable = false
			return
		}
		if s3Err.StatusCode == http.StatusForbidden {
			mtr.Retryable = false
			return
		}
	}
	log.Debugf("Error: %q, retryable: %q ", err, mtr.Retryable)
}

// ApplyDefaultErrorStrategy prepares states for MigrationTaskResult and calls MarkRetry for error
func (mtr *MigrationTaskResult) ApplyDefaultErrorStrategy(errType model.ErrorType, err error) {
	mtr.ErrorType = errType
	mtr.Error = err
	mtr.MarkRetry(errType, err)
}

// MigrationTaskData validates and extracts object location data
type MigrationTaskData struct {
	action                                       string
	aclMode                                      model.ACLMode
	hostFrom, hostTo                             string
	dstBucketName, srcBucketName, dstKey, srcKey string
}

//NewMigrationTaskData constructs an instance of MigrationTaskData
func NewMigrationTaskData(action string,
	aclMode model.ACLMode,
	hostFrom, hostTo string,
	srcBucket, srcKey, dstBucket, dstKey string) MigrationTaskData {
	return MigrationTaskData{
		action:        action,
		aclMode:       aclMode,
		hostFrom:      hostFrom,
		hostTo:        hostTo,
		srcBucketName: srcBucket,
		srcKey:        srcKey,
		dstBucketName: dstBucket,
		dstKey:        dstKey,
	}
}

// TaskMigrator encapsulates all necessary routines to execute task
type TaskMigrator struct {
	Task                     MigrationTaskData
	SrcS3Client, DstS3Client *s3.S3
	Multipart                bool
	srcBucket, dstBucket     *s3.Bucket
}

// Run performs task migration actions
func (migrator *TaskMigrator) Run() (srcError, dstError error) {
	migrator.prepareBucketInstances()

	getStart := time.Now()
	object, srcError := migrator.getObjectFromSource()
	defer func() {
		if err := object.cleanUp(); err != nil {
			log.Debugf("Did not clean up object data body, %s", err)
		}
	}()
	if srcError != nil {
		return srcError, nil
	}

	metrics.UpdateSince("runtime.task.get", getStart)
	putStart := time.Now()
	defer metrics.UpdateSince("runtime.task.put", putStart)

	srcError, dstError = migrator.putObject(object)
	if dstError != nil {
		srcError, dstError = migrator.ensureDestinationBucketExistence()
		if srcError != nil || dstError != nil {
			return srcError, dstError
		}
		return migrator.putObject(object)
	}
	return srcError, dstError
}

func (migrator *TaskMigrator) prepareBucketInstances() {
	migrator.srcBucket = migrator.SrcS3Client.Bucket(migrator.Task.srcBucketName)
	migrator.dstBucket = migrator.DstS3Client.Bucket(migrator.Task.dstBucketName)
}

func (migrator *TaskMigrator) getObjectFromSource() (s3Object, error) {
	return s3ObjectData(migrator.Task.srcKey, migrator.srcBucket, migrator.Multipart)
}

func (migrator *TaskMigrator) ensureDestinationBucketExistence() (srcError, dstError error) {
	bucketExists, err := migrator.dstBucket.Exists("")
	if err != nil {
		log.Printf("Couldn't determine destination bucket %s existence: %s", migrator.dstBucket.Name, err)
		return nil, dstError
	}
	if bucketExists {
		return nil, nil
	}
	srcError, dstError = CopyBucket(migrator.srcBucket.Name, migrator.dstBucket.Name, migrator.SrcS3Client,
		migrator.DstS3Client, model.ACLCopyFromSource == migrator.Task.aclMode)
	if srcError != nil || dstError != nil {
		log.Printf("Couldn't copy bucket %s from %s to %s. Error from - source: %s, destination: %s",
			migrator.srcBucket.Name,
			migrator.SrcS3Client.S3Endpoint,
			migrator.DstS3Client.S3Endpoint,
			srcError, dstError)
	}
	return srcError, dstError
}

func (migrator *TaskMigrator) putObject(object s3Object) (srcError, dstError error) {
	defer log.Printf("Put object %s/%s/%s -> %s/%s/%s result %s, %s",
		migrator.Task.hostFrom, migrator.Task.srcBucketName, migrator.Task.srcKey,
		migrator.Task.hostTo, migrator.Task.dstBucketName, migrator.Task.dstKey,
		srcError, dstError,
	)

	if migrator.Multipart {
		return nil, multipartUpload(migrator.dstBucket, migrator.Task.dstKey, object)
	}

	objectACL := migrator.determineACL(object)
	dstError = migrator.dstBucket.PutReader(migrator.Task.dstKey, object.data, object.contentLength,
		object.contentType, objectACL, object.options)
	if dstError != nil {
		return nil, dstError
	}
	if migrator.Task.action == model.ActionMove {
		deleteStart := time.Now()
		srcError = DeleteObject(migrator.srcBucket, migrator.Task.srcKey)
		if srcError == nil {
			log.Printf("Removed object %s/%s", migrator.srcBucket.Name, migrator.Task.srcKey)
		}
		metrics.UpdateSince("runtime.task.delete", deleteStart)
	}

	return
}

func (migrator *TaskMigrator) determineACL(object s3Object) s3.ACL {
	if model.ACLCopyFromSource == migrator.Task.aclMode {
		return object.perm
	}
	return s3.Private
}

// DeleteObject deletes object from cluster
func DeleteObject(bucket *s3.Bucket, object string) error {
	err := bucket.Del(object)
	if err != nil {
		if strings.Contains(err.Error(), ErrMsgNotFound) {
			log.Printf("Object %s/%s/%s not found", bucket.S3Endpoint, bucket.Name, object)
		} else {
			return err
		}
	}
	return nil
}

// CopyBucket creates copy of source bucket on destination cluster
func CopyBucket(srcBucketName, dstBucketName string, srcS3Client *s3.S3, dstS3Client *s3.S3, shouldUseSrcBucketACL bool) (srcError, dstError error) {
	srcBucket := srcS3Client.Bucket(srcBucketName)
	bucketACL := s3.Private
	if shouldUseSrcBucketACL {
		bucketACL, srcError = getBucketACL(srcBucket)
		if srcError != nil {
			log.Printf("Bucket %s on %s ACL retrieval fail: %s", srcBucketName, srcS3Client.S3Endpoint, srcError)
			return srcError, nil
		}
	}

	dstBucket := dstS3Client.Bucket(dstBucketName)
	exists, dstError := dstBucket.Exists("")
	if exists && dstError != nil {
		return nil, nil
	}
	dstError = dstBucket.PutBucket(bucketACL)
	if dstError != nil {
		log.Printf("Bucket %s creation on destination %s failed: %s", dstBucketName, dstS3Client.S3Endpoint, dstError)
		return
	}

	return nil, nil
}

func getBucketACL(bucket *s3.Bucket) (acl s3.ACL, err error) {
	_, err = bucket.Get("")
	if err != nil {
		return acl, err
	}

	bucketACL, err := bucket.GetACL("")
	if err != nil {
		return acl, err
	}

	bucketCannedPolicy := s3.GetCannedPolicyByAcl(*bucketACL)
	return bucketCannedPolicy, nil
}

const objectSizeLimit = 100 * 1024 * 1024

func s3ObjectData(path string, bucket *s3.Bucket, multipart bool) (result s3Object, err error) {
	resp, err := bucket.GetResponseWithHeaders(path,
		map[string][]string{
			"X-Akubra-No-Regression-On-Failure": {"1"},
			"Accept-Encoding": {"*"}})

	if err != nil {
		log.Printf("Object %s/%s/%s headers could not be fetched: %s", bucket.S3Endpoint, bucket.Name, path, err)
		return result, err
	}

	result.data = resp.Body
	result.headers = resp.Header

	log.Printf("Object %s/%s is %s bytes\n", bucket.Name, path, result.headers.Get("content-length"))

	result = prepareMetadataAndHeaders(result)
	result.contentType, result.contentLength, err = extractContentTypeAndLength(result.headers, multipart)
	if err != nil {
		return result, err
	}
	if result.headers.Get("Content-Encoding") != "" {
		result.options.ContentEncoding = result.headers.Get("Content-Encoding")
	}
	log.Debugf("Get bucket acl %s/%s", bucket.S3Endpoint, bucket.Name)
	objACL, err := bucket.GetACL(path)
	if err != nil {
		log.Debugf("Cannot get bucket acl %s/%s", bucket.S3Endpoint, bucket.Name)
		return result, err
	}
	log.Debugf("Got bucket acl %s/%s", bucket.S3Endpoint, bucket.Name)

	objCannedPolicy := s3.GetCannedPolicyByAcl(*objACL)
	result.perm = objCannedPolicy
	return result, nil
}

func prepareMetadataAndHeaders(inputS3Obj s3Object) (outputS3Obj s3Object) {
	outputS3Obj.options.Meta = make(map[string][]string)
	outputS3Obj.headers = make(map[string][]string)
	outputS3Obj.data = inputS3Obj.data
	for name, value := range inputS3Obj.headers {
		key := strings.ToLower(name)
		if strings.HasPrefix(key, AmzMetadataPrefix) {
			keyWithoutMeta := strings.ToLower(strings.Replace(key, AmzMetadataPrefix, "", 1))
			outputS3Obj.options.Meta[keyWithoutMeta] = []string{value[0]}
		}
		if key == "content-encoding" {
			outputS3Obj.options.ContentEncoding = value[0]
		}
		if key == "cache-control" {
			outputS3Obj.options.CacheControl = value[0]
		}
		if key == "redirect-location" {
			outputS3Obj.options.RedirectLocation = value[0]
		}
		if key == "content-disposition" {
			outputS3Obj.options.ContentDisposition = value[0]
		}

		if key == "date" {
			outputS3Obj.headers.Add("Date", value[0])
		}
		if key == "content-length" {
			outputS3Obj.headers.Add("Content-Length", value[0])
		}
		if key == "content-type" {
			outputS3Obj.headers.Add("Content-Type", value[0])
		}
	}
	return outputS3Obj
}

func multipartUpload(bucket *s3.Bucket, objectPath string, srcObj s3Object) error {
	f, err := ioutil.TempFile(os.TempDir(), "")
	if err != nil {
		return err
	}
	_, err = io.Copy(f, srcObj.data)
	if err != nil {
		return err
	}
	defer func() {
		if rmErr := os.Remove(path.Join(os.TempDir(), f.Name())); rmErr != nil {
			log.Debugf("Warning! did not removed file after multipart upload, reason %s", rmErr)
		}
	}()
	multipart, err := bucket.InitMulti(objectPath, srcObj.contentType, srcObj.perm, srcObj.options)
	if err != nil {
		return err
	}

	if _, seekErr := f.Seek(0, 0); seekErr != nil {
		return seekErr
	}
	parts, err := multipart.PutAll(f, 50*1024*1024-2)
	if err != nil {
		return err
	}
	return multipart.Complete(parts)
}

// GetHTTPStatusCodeFromError extracts http code from s3.Error value
func GetHTTPStatusCodeFromError(err error) int {
	s3Err, ok := err.(*s3.Error)
	if ok {
		return s3Err.StatusCode
	}
	return 0
}
