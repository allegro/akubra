package model

import (
	"database/sql"
	"encoding/json"

	"github.com/lib/pq"
)

// ErrorType denotes model errors
type ErrorType = string

// Common task error
const (
	TaskError        ErrorType = "task_error"
	CredentialsError ErrorType = "credentials_error"
	PermissionsError ErrorType = "permissions_error"
	SourceError      ErrorType = "source_error"
	DestinationError ErrorType = "destination_error"
)

// MigrationTaskItem working struct
type MigrationTaskItem struct {
	HostPortFrom  string         `db:"hostport_from" json:"hostPortFrom" csv:"hostPortFrom"`
	HostPortTo    string         `db:"hostport_to" json:"hostPortTo" csv:"hostPortTo"`
	KeyFrom       string         `db:"key_from" json:"keyFrom" csv:"keyFrom"`
	KeyTo         string         `db:"key_to" json:"keyTo" csv:"keyTo"`
	Action        string         `db:"action" json:"action" csv:"action"`
	Status        string         `db:"status"`
	LastUpdate    pq.NullTime    `db:"last_update" json:"lastUpdate,omitempty"`
	AccessKeyFrom JSONNullString `db:"accesskey_from" json:"access-key" csv:"accessKeyFrom"`
	AccessKeyTo   JSONNullString `db:"accesskey_to" json:"access-key-to" csv:"accessKeyTo"`
	Pid           JSONNullInt64  `db:"pid" json:"pid,omitempty"`
	Mid           JSONNullString `db:"mid" json:"mid,omitempty"`
	NowToDateDiff float64        `db:"now_to_date_diff"`
	Tid           uint64         `db:"tid"`
	IsPermanent   bool           `db:"is_permanent" json:"is_permanent,omitempty"`
	SrcRm         bool
	ACLMode       ACLMode `db:"acl_mode"`
}

//ACLMode indicates how the acl of the destination object should be set
type ACLMode string

const (
	//ACLCopyFromSource indicates that the destination object's ACL should be the same as source object's
	ACLCopyFromSource ACLMode = "copy_src"
	//ACLNone indicates that the destination object's ACL shouldn't be explicitly specified by the migrator
	ACLNone ACLMode = "none"

	// ActionMove object will be copied and removed
	ActionMove = "move"
	// ActionCopy object will be copied
	ActionCopy = "copy"
	// ActionDelete object will be removed
	ActionDelete = "delete"
)

// JSONNullInt64 wraps sql.NullInt64 and implements json.UnmarshalJSON interface
type JSONNullInt64 struct {
	sql.NullInt64
}

//UnmarshalJSON implements json.UnmarshalJSON interface
func (jni64 *JSONNullInt64) UnmarshalJSON(data []byte) error {
	var v int64
	err := json.Unmarshal(data, &v)
	if err != nil {
		return err
	}
	jni64.NullInt64 = sql.NullInt64{Int64: v, Valid: true}
	return nil
}

// JSONNullString wraps sql.NullString and implements json.UnmarshalJSON interface
type JSONNullString struct {
	sql.NullString
}

//UnmarshalJSON implements json.UnmarshalJSON interface
func (jnstr *JSONNullString) UnmarshalJSON(data []byte) error {
	var v string
	err := json.Unmarshal(data, &v)
	if err != nil {
		return err
	}
	jnstr.NullString = sql.NullString{String: v, Valid: true}
	return nil
}
