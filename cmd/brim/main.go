package main

import (
	"github.com/alecthomas/kingpin"
	"github.com/allegro/akubra/internal/akubra/config"
	"github.com/allegro/akubra/internal/akubra/log"
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
	configReadCloser, err := config.ReadConfiguration(*akubraConfig)
	if err != nil {
		log.Fatal("No akubra configuration provided")
	}
	defer func() {
		err := configReadCloser.Close()
		if err != nil {
			log.Println("Could not close config file")
		}
	}()
	akubraConf, err := config.Configure(configReadCloser)
	if err != nil {
		log.Fatalf("Improperly configured %s", err)
	}
	brimConf, err := bConf.Configure(*brimConfig)
	if err != nil {
		log.Fatalf("Improperly configured %s", err)
	}

	watchdog.RunWatchdogWorker(&akubraConf, &brimConf)
}
