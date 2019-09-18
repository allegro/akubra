package main

import (
	"github.com/alecthomas/kingpin"
	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/log"
	bConf "github.com/allegro/akubra/internal/brim/config"
	watchdog "github.com/allegro/akubra/internal/brim/watchdog-main"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

var (
	akubraConfig = kingpin.
			Flag("aconfig", "Configuration file path e.g.: \"conf/dev.yaml\"").
			Short('a').
			Required().
			ExistingFile()

	brimConfig = kingpin.
			Flag("bconfig", "Configuration file path e.g.: \"conf/dev.yaml\"").
			Short('b').
			Required().
			ExistingFile()
)

func main() {
	kingpin.Parse()
	akubraConf, err := config.Configure(*akubraConfig)
	if err != nil {
		log.Fatalf("Improperly configured %s", err)
	}
	brimConf, err := bConf.Configure(*brimConfig)
	if err != nil {
		log.Fatalf("Improperly configured %s", err)
	}

	watchdog.RunWatchdogWorker(&akubraConf, &brimConf)
}
