package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/sharding"
	"gopkg.in/tylerb/graceful.v1"
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

	log.Println(versionString)
	conf, err := config.Configure(*configFile)

	if err != nil {
		log.Fatalf("Improperly configured %s", err)
	}

	mainlog := conf.Mainlog
	mainlog.Printf("starting on port %s", conf.Listen)
	mainlog.Printf("connlimit %v", conf.ConnLimit)
	mainlog.Printf("backends %s", conf.Backends)
	srv := newService(conf)
	startErr := srv.start()
	if startErr != nil {
		mainlog.Printf("Could not start service, reason: %q", startErr.Error())
	}
}

type service struct {
	config config.Config
}

func (s *service) start() error {
	var handler http.Handler
	if len(s.config.Clusters) > 0 {
		handler = sharding.NewHandler(s.config)
	} else {
		handler = httphandler.NewHandler(s.config)
	}
	srv := &graceful.Server{
		Server: &http.Server{
			Addr:    s.config.Listen,
			Handler: handler,
		},
		Timeout: 10 * time.Second,
	}

	srv.SetKeepAlivesEnabled(true)
	listener, err := net.Listen("tcp", s.config.Listen)

	if err != nil {
		return err
	}

	return srv.Serve(listener)
}

func newService(cfg config.Config) *service {
	return &service{config: cfg}
}
