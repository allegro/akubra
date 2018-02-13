package types

import "encoding/xml"

type CompleteMultipartUploadResult struct {
	XMLName xml.Name       `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult" json:"-"`
	Location string
	Bucket   string
	Key      string
	ETag     string
}