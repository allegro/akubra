package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	graceful "gopkg.in/tylerb/graceful.v1"

	"github.com/alecthomas/kingpin"
	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/metrics"
	"github.com/allegro/akubra/sharding"
)

var (
	// filled by linker
	version = "development"

	// CLI flags
	configFile = kingpin.
			Flag("config", "Configuration file e.g.: \"conf/dev.yaml\"").
			Short('c').
			Required().
			ExistingFile()
)

func main() {
	versionString := fmt.Sprintf("Akubra (%s version)", version)
	kingpin.Version(versionString)
	kingpin.Parse()
	conf, err := config.Configure(*configFile)
	log.Println(versionString)
	if err != nil {
		log.Fatalf("Improperly configured %s", err)
	}

	mainlog := conf.Mainlog
	mainlog.Printf("starting on port %s", conf.Listen)
	srv := newService(conf)
	startErr := srv.start()
	if startErr != nil {
		mainlog.Fatalf("Could not start service, reason: %q", startErr.Error())
	}
}

type service struct {
	conf config.Config
}

func (s *service) start() error {
	handler, err := sharding.NewHandler(s.conf)

	if err != nil {
		return err
	}
	fmt.Printf("metrics conf %v", s.conf.Metrics)
	err = metrics.Init(s.conf.Metrics)

	if err != nil {
		return err
	}

	srv := &graceful.Server{
		Server: &http.Server{
			Addr:    s.conf.Listen,
			Handler: handler,
		},
		Timeout: 10 * time.Second,
	}

	srv.SetKeepAlivesEnabled(true)
	listener, err := net.Listen("tcp", s.conf.Listen)

	if err != nil {
		return err
	}
	return srv.Serve(listener)
}

func newService(cfg config.Config) *service {
	return &service{conf: cfg}
}
