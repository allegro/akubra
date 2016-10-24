package main

import (
	"fmt"
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
	mainlog := conf.Mainlog
	if err != nil {
		fmt.Printf("Improperly configured %s", err)
		return
	}
	mainlog.Printf("starting on port %s", conf.Listen)
	mainlog.Printf("connlimit %v", conf.ConnLimit)
	mainlog.Printf("backends %s", conf.BackendURLs)
	srv := newService(conf)
	startErr := srv.start()
	if startErr != nil {
		mainlog.Printf("Could not start service, reason: %q", startErr.Error())
	}
}
