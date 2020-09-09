package s3

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"github.com/allegro/akubra/external/miniotweak/s3signer"
	"io"
	"io/ioutil"
	"net/url"
	"sort"
	"strconv"
)
import goamzS3 "github.com/AdRoll/goamz/s3"
import "net/http"

type MultipartUploader struct {
	Bucket, Key, AccessKey, SecretKey, HostPort string
	PartSize                                    int64
	ObjectBody                                  io.Reader
	UploadId                                    string
	parts                                       completeParts
}

func (uploader *MultipartUploader) Init(contType string, perm goamzS3.ACL, options goamzS3.Options) error {
	headers := map[string][]string{
		"Content-Type":   {contType},
		"Content-Length": {"0"},
		"x-amz-acl":      {string(perm)},
	}
	addOptionalHeaders(options, headers)
	params := map[string][]string{
		"uploads": {""},
	}

	url, err := uploader.URL()
	if err != nil {
		return fmt.Errorf("brim.s3.MultipartUploader::Init err: %w", err)
	}

	req := prepareRequest(url, http.MethodPost, headers, params, nil)

	resp, err := uploader.signAndDo(req, "")
	if err != nil {
		return fmt.Errorf("brim.s3.MultipartUploader::Init sent request error %w", err)
	}

	var response struct {
		UploadId string `xml:"UploadId"`
	}

	responseValue := &response

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("brim.s3.MultipartUploader::Init reading response error %w", err)
	}

	err = xml.Unmarshal(body, responseValue)
	if err != nil {
		return fmt.Errorf("brim.s3.MultipartUploader::Init multipart upload response XML does not unmarshal to response struct %w", err)
	}

	uploader.UploadId = responseValue.UploadId
	return nil
}

func (uploader *MultipartUploader) URL() (*url.URL, error) {
	url, err := url.Parse(fmt.Sprintf("http://%s/%s/%s", uploader.HostPort, uploader.Bucket, uploader.Key))
	return url, fmt.Errorf("brim.s3.MultipartUploader::URL error parsing object URL %w", err)
}

func (uploader *MultipartUploader) UploadParts(size int64) error {
	current := 1
	var err error
	for err != io.EOF {
		r := &io.LimitedReader{
			R: uploader.ObjectBody,
			N: size,
		}
		var data []byte
		data, err = ioutil.ReadAll(r)
		if err != nil {
			return fmt.Errorf("brim.s3.MultipartUploader::UploadPart read part error %w", err)
		}
		if int64(len(data)) < size {
			err = io.EOF
		}
		seeker := bytes.NewReader(data)
		part, err := uploader.UploadPart(current, seeker)
		if err != nil {
			return fmt.Errorf("brim.s3.MultipartUploader::UploadPart error sendig part %w", err)
		}
		uploader.parts = append(uploader.parts, part)
		current++
	}
	return nil
}

func (uploader *MultipartUploader) UploadPart(n int, r io.ReadSeeker) (CompletePart, error) {
	partSize, _, md5b64, err := seekerInfo(r)
	if err != nil {
		return CompletePart{}, err
	}
	headers := map[string][]string{
		"Content-Length": {strconv.FormatInt(partSize, 10)},
		"Content-Md5":    {md5b64},
	}
	params := map[string][]string{
		"uploadId":   {uploader.UploadId},
		"partNumber": {strconv.FormatInt(int64(n), 10)},
	}

	uri, err := uploader.URL()
	if err != nil {
		return CompletePart{}, err
	}

	_, err = r.Seek(0, 0)
	if err != nil {
		return CompletePart{}, err
	}

	req := prepareRequest(uri, http.MethodPut, headers, params, ioutil.NopCloser(r))
	req.ContentLength = partSize

	resp, err := uploader.signAndDo(req, md5b64)
	if err != nil {
		return CompletePart{}, fmt.Errorf("s3.brim.MultipartUploader::UploadPart sending request error %w", err)
	}
	etag := resp.Header.Get("ETag")
	if etag == "" {
		return CompletePart{}, fmt.Errorf("part upload succeeded with no ETag")
	}
	return CompletePart{PartNumber: n, ETag: etag}, nil

}

func (uploader *MultipartUploader) signAndDo(req *http.Request, md5b64 string) (*http.Response, error) {
	req = s3signer.SignV2(req, uploader.AccessKey, uploader.SecretKey, nil)
	client := http.Client{
		Transport:     http.DefaultTransport,
		CheckRedirect: nil,
		Jar:           nil,
		Timeout:       0,
	}
	return client.Do(req)
}

func (uploader *MultipartUploader) Complete() error {
	params := map[string][]string{
		"uploadId": {uploader.UploadId},
	}
	c := completeUpload{}
	for _, p := range uploader.parts {
		c.Parts = append(c.Parts, CompletePart{p.PartNumber, p.ETag})
	}
	sort.Sort(c.Parts)
	data, err := xml.Marshal(&c)
	if err != nil {
		return fmt.Errorf("s3.brim.MultipartUploader::Complete request body marshaling error %w", err)
	}
	uri, err := uploader.URL()
	if err != nil {
		return err
	}
	body := bytes.NewReader(data)

	headers := make(http.Header)
	headers.Add("Content-Length", strconv.FormatInt(int64(len(data)), 10))

	req := prepareRequest(uri, http.MethodPost, headers, params, ioutil.NopCloser(body))

	resp, err := uploader.signAndDo(req, "")
	if err != nil {
		return fmt.Errorf("s3.brim.MultipartUploader::Complete request sent error %w", err)
	}

	decodedResp := &completeUploadResp{}
	if err := uploader.UnmarshalResp(resp, decodedResp); err != nil {
		return err
	}

	if decodedResp.XMLName.Local == "Error" {
		return fmt.Errorf("s3.brim.MultipartUploader::Complete response error %#v", decodedResp)
	}

	if decodedResp.XMLName.Local == "CompleteMultipartUploadResult" {
		return nil
	}
	return fmt.Errorf("s3.brim.MultipartUploader::Complete not determined response: %#v", decodedResp)

}
func (uploader *MultipartUploader) UnmarshalResp(resp *http.Response, value interface{}) error {
	bodyData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	err = xml.Unmarshal(bodyData, value)
	return err
}

const listPartMax = 1000

func (uploader *MultipartUploader) ListParts() error {
	maxParts := listPartMax
	uri, err := uploader.URL()
	if err != nil {
		return err
	}

	params := map[string][]string{
		"uploadId":           {uploader.UploadId},
		"max-parts":          {strconv.FormatInt(int64(maxParts), 10)},
		"part-number-marker": {strconv.FormatInt(int64(0), 10)},
	}
	req := prepareRequest(uri, http.MethodGet, map[string][]string{}, params, nil)

	var parts partSlice
	var deserializeResp listPartsResp

	resp, err := uploader.signAndDo(req, "")
	if err != nil {
		return err
	}
	err = uploader.UnmarshalResp(resp, &deserializeResp)
	if err != nil {
		return err
	}
	parts = append(parts, deserializeResp.Part...)

	if !deserializeResp.IsTruncated {
		sort.Sort(parts)
	}
	return nil
	//params["part-number-marker"] = []string{resp.NextPartNumberMarker}
}

type Part struct {
	N    int `xml:"PartNumber"`
	ETag string
	Size int64
}

type partSlice []Part

func (s partSlice) Len() int           { return len(s) }
func (s partSlice) Less(i, j int) bool { return s[i].N < s[j].N }
func (s partSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type listPartsResp struct {
	NextPartNumberMarker string
	IsTruncated          bool
	Part                 []Part
}

func prepareRequest(uri *url.URL, method string, headers, params map[string][]string, body io.ReadCloser) *http.Request {
	values := uri.Query()
	for key, valuesToSet := range params {
		for _, value := range valuesToSet {
			values.Add(key, value)
		}
	}
	req := &http.Request{
		Method: method,
		URL:    uri,
		Header: headers,
		Body:   body,
		Close:  false,
	}

	req.URL.RawQuery = values.Encode()
	return req
}

func seekerInfo(r io.ReadSeeker) (size int64, md5hex string, md5b64 string, err error) {
	_, err = r.Seek(0, 0)
	if err != nil {
		return 0, "", "", err
	}
	digest := md5.New()
	size, err = io.Copy(digest, r)
	if err != nil {
		return 0, "", "", err
	}
	sum := digest.Sum(nil)
	md5hex = hex.EncodeToString(sum)
	md5b64 = base64.StdEncoding.EncodeToString(sum)
	return size, md5hex, md5b64, nil
}

func addOptionalHeaders(o goamzS3.Options, headers map[string][]string) {
	if o.SSE {
		headers["x-amz-server-side-encryption"] = []string{string(goamzS3.S3Managed)}
	} else if o.SSEKMS {
		headers["x-amz-server-side-encryption"] = []string{string(goamzS3.KMSManaged)}
		if len(o.SSEKMSKeyId) != 0 {
			headers["x-amz-server-side-encryption-aws-kms-key-id"] = []string{o.SSEKMSKeyId}
		}
	} else if len(o.SSECustomerAlgorithm) != 0 && len(o.SSECustomerKey) != 0 && len(o.SSECustomerKeyMD5) != 0 {
		// Amazon-managed keys and customer-managed keys are mutually exclusive
		headers["x-amz-server-side-encryption-customer-algorithm"] = []string{o.SSECustomerAlgorithm}
		headers["x-amz-server-side-encryption-customer-key"] = []string{o.SSECustomerKey}
		headers["x-amz-server-side-encryption-customer-key-MD5"] = []string{o.SSECustomerKeyMD5}
	}
	if len(o.Range) != 0 {
		headers["Range"] = []string{o.Range}
	}
	if len(o.ContentEncoding) != 0 {
		headers["Content-Encoding"] = []string{o.ContentEncoding}
	}
	if len(o.CacheControl) != 0 {
		headers["Cache-Control"] = []string{o.CacheControl}
	}
	if len(o.ContentMD5) != 0 {
		headers["Content-MD5"] = []string{o.ContentMD5}
	}
	if len(o.RedirectLocation) != 0 {
		headers["x-amz-website-redirect-location"] = []string{o.RedirectLocation}
	}
	if len(o.ContentDisposition) != 0 {
		headers["Content-Disposition"] = []string{o.ContentDisposition}
	}
	if len(o.StorageClass) != 0 {
		headers["x-amz-storage-class"] = []string{string(o.StorageClass)}

	}
	for k, v := range o.Meta {
		headers["x-amz-meta-"+k] = v
	}
}

type completeUpload struct {
	XMLName xml.Name      `xml:"CompleteMultipartUpload"`
	Parts   completeParts `xml:"Part"`
}

type CompletePart struct {
	PartNumber int
	ETag       string
}

type completeParts []CompletePart
type completeUploadResp struct {
	XMLName  xml.Name
	InnerXML string `xml:",innerxml"`
}

func (p completeParts) Len() int           { return len(p) }
func (p completeParts) Less(i, j int) bool { return p[i].PartNumber < p[j].PartNumber }
func (p completeParts) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
