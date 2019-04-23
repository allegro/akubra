package model

import (
	"github.com/AdRoll/goamz/s3"
	"github.com/allegro/akubra/internal/akubra/watchdog"
)

//WALEntry is an entry of the log that describes the object's lifecycle
type WALEntry struct {
	Record              *watchdog.ConsistencyRecord
	RecordProcessedHook func(record *watchdog.ConsistencyRecord, err error) error
}

//WALTask represents a migration that has to be performed in order for the object to be in sync
type WALTask struct {
	SourceClient        *s3.S3
	DestinationsClients []*s3.S3
	WALEntry            *WALEntry
}
