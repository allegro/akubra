package feeder

import (
	"github.com/allegro/akubra/internal/brim/model"
)

//WALFeeder creates a feed of WALEntries that represent the desired object's state
type WALFeeder interface {
	CreateFeed() <-chan *model.WALEntry
}
