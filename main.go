package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/allegro/akubra/log"

	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/storages"

	"github.com/alecthomas/kingpin"
	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/metrics"
	"github.com/allegro/akubra/regions"
	_ "github.com/lib/pq"
	graceful "gopkg.in/tylerb/graceful.v1"
)

// YamlValidationErrorExitCode for problems with YAML config validation
const YamlValidationErrorExitCode = 20

// TechnicalEndpointGeneralTimeout for /configuration/validate endpoint
const TechnicalEndpointGeneralTimeout = 5 * time.Second

type service struct {
	config config.Config
}

var (
	// filled by linker
	version = "development"

	// CLI flags
	configFile = kingpin.
			Flag("config", "Configuration file path e.g.: \"conf/dev.yaml\"").
			Short('c').
			Required().
			ExistingFile()
	testConfig = kingpin.
			Flag("test-config", "Testing only configuration file from 'config' arg. (app. not starting).").
			Short('t').
			Bool()
)

func main() {
	versionString := fmt.Sprintf("Akubra (%s version)", version)
	kingpin.Version(versionString)
	kingpin.Parse()
	conf, err := config.Configure(*configFile)
	if err != nil {
		log.Fatalf("Improperly configured %s", err)
	}

	if *testConfig {
		os.Exit(0)
	}

	log.Printf("Health check endpoint: %s", conf.Service.Server.HealthCheckEndpoint)

	mainlog, err := log.NewDefaultLogger(conf.Logging.Mainlog, "LOG_LOCAL2", false)
	if err != nil {
		log.Fatalf("Could not set up main logger: %q", err)
	}

	log.DefaultLogger = mainlog

	mainlog.Printf("starting on port %s", conf.Service.Server.Listen)
	mainlog.Printf("backends %#v", conf.Backends)

	srv := newService(conf)
	srv.startTechnicalEndpoint()
	startErr := srv.start()
	if startErr != nil {
		mainlog.Fatalf("Could not start service, reason: %q", startErr.Error())
	}
}

func (s *service) start() error {
	roundtripper, err := httphandler.ConfigureHTTPTransport(s.config.Service.Client)
	if err != nil {
		log.Fatalf("Couldn't set up client properties, %q", err)
	}
	// TODO: Decorate ^ roundtripper here now - fix accesslog in configuration
	syncLog, err := log.NewDefaultLogger(s.config.Logging.Synclog, "LOG_LOCAL1", true)

	respHandler := httphandler.LateResponseHandler(syncLog, s.config.Logging.SyncLogMethodsSet)

	storage, err := storages.InitStorages(
		roundtripper,
		s.config.Clusters,
		s.config.Backends,
		respHandler)
	if err != nil {
		log.Fatalf("Storages initialization problem: %q", err)
	}

	regionsRT, err := regions.NewRegions(s.config.Regions, *storage, roundtripper, nil)
	if err != nil {
		return err
	}
	accessLog, err := log.NewDefaultLogger(s.config.Logging.Accesslog, "LOG_LOCAL1", true)

	regionsDecoratedRT := httphandler.DecorateRoundTripper(s.config.Service.Client,
		accessLog, s.config.Service.Server.HealthCheckEndpoint, regionsRT)

	handler, err := httphandler.NewHandlerWithRoundTripper(regionsDecoratedRT, s.config.Service.Server)
	if err != nil {
		return err
	}

	err = metrics.Init(s.config.Metrics)

	if err != nil {
		return err
	}

	srv := &graceful.Server{
		Server: &http.Server{
			Addr:         s.config.Service.Server.Listen,
			Handler:      handler,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
		},
		Timeout: 10 * time.Second,
	}

	srv.SetKeepAlivesEnabled(true)
	listener, err := net.Listen("tcp", s.config.Service.Server.Listen)

	if err != nil {
		log.Fatalln(err)
	}

	return srv.Serve(listener)
}

func newService(cfg config.Config) *service {
	return &service{config: cfg}
}
func (s *service) startTechnicalEndpoint() {
	port := s.config.Service.Server.TechnicalEndpointListen
	log.Printf("Starting technical HTTP endpoint on port: %q", port)
	serveMuxHandler := http.NewServeMux()
	serveMuxHandler.HandleFunc(
		"/configuration/validate",
		config.ValidateConfigurationHTTPHandler,
	)
	go func() {
		srv := &graceful.Server{
			Server: &http.Server{
				Addr:           port,
				Handler:        serveMuxHandler,
				MaxHeaderBytes: 512,
				WriteTimeout:   TechnicalEndpointGeneralTimeout,
				ReadTimeout:    TechnicalEndpointGeneralTimeout,
			},
			Timeout:      TechnicalEndpointGeneralTimeout,
			TCPKeepAlive: 1 * time.Minute,
			Logger:       graceful.DefaultLogger(),
		}

		log.Fatal(srv.ListenAndServe())
	}()
	log.Println("Technical HTTP endpoint is running.")
}
