package main

import (
	"log"
	"net"
	"net/http"
	"time"

	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/httphandler"
	"gopkg.in/tylerb/graceful.v1"
)

type service struct {
	config config.Config
}

func (s *service) start() error {
	handler := httphandler.NewHandler(s.config)
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

func main() {

	conf, err := config.Configure()

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
