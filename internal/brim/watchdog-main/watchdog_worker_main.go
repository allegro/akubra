package watchdog

import (
	"time"

	"github.com/allegro/akubra/internal/akubra/config"
	"github.com/allegro/akubra/internal/akubra/database"
	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/brim/auth"
	bConf "github.com/allegro/akubra/internal/brim/config"
	"github.com/allegro/akubra/internal/brim/feeder"
	"github.com/allegro/akubra/internal/brim/filter"
	"github.com/allegro/akubra/internal/brim/model"
	"github.com/allegro/akubra/internal/brim/worker"
	feederUtils "github.com/allegro/akubra/pkg/brim/feeder"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

func RunWatchdogWorker(akubraConf *config.Config, brimConf *bConf.BrimConf) {

	sqlFeeder, err := feeder.NewSQLWALFeeder(
		akubraConf,
		&feeder.WALFeederConfig{MaxRecordsPerQuery: uint(brimConf.WALConf.MaxRecordsPerQuery),
			NoRecordsSleepDuration: brimConf.WALConf.NoRecordsSleepDuration,
			FailureDelay:           brimConf.WALConf.FeederTaskFailureDelay},
		database.NewDBClientFactory(
			akubraConf.Watchdog.Props["dialect"],
			"sslmode=disable dbname=:dbname: user=:user: password=:password: host=:host: port=:port: connect_timeout=:conntimeout:",
			[]string{"user", "password", "dbname", "host", "port", "conntimeout"},
		))

	if err != nil {
		log.Fatalf("Failed to configure WAL: %s", err)
	}

	sqlRecordsFeed := sqlFeeder.CreateFeed()
	feedProxyChannel := make(chan interface{})

	go func() {
		for e := range sqlRecordsFeed {
			feedProxyChannel <- e
		}
	}()

	backendResolver := auth.NewConfigBasedBackendResolver(akubraConf, brimConf)
	throtteledFeedChannel := feederUtils.Throttle(feedProxyChannel, &feederUtils.ThrottledPublisherConfig{BurstEnabled: false, TaskEmissionDuration: time.Second * 1, MaxEmittedTasksCount: 1})

	walFilter := filter.NewDefaultWALFilter(backendResolver, &filter.S3VersionFetcher{VersionHeaderName: akubraConf.Watchdog.ObjectVersionHeaderName})
	walWorker := worker.NewTaskMigratorWALWorker(2)

	walEntries := make(chan *model.WALEntry)
	walTasks := walFilter.Filter(walEntries)
	walWorker.Process(walTasks)

	for item := range throtteledFeedChannel {
		switch item.(type) {
		case *model.WALEntry:
			walEntries <- item.(*model.WALEntry)
		}
	}
}
