package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

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
	conf config.Config
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
	log.Println(versionString)
	if err != nil {
		log.Fatalf("Improperly configured %s", err)
	}

	valid, errs := config.ValidateConf(conf.YamlConfig, true)
	if !valid {
		log.Println("Custom YAML Configuration validation error:", errs)
		os.Exit(YamlValidationErrorExitCode)
	}
	log.Println("Configuration checked - OK.")
	if *testConfig {
		os.Exit(0)
	}

	log.Printf("Health check endpoint: %s", conf.HealthCheckEndpoint)

	mainlog := conf.Mainlog
	mainlog.Printf("starting on port %s", conf.Listen)
	mainlog.Printf("backends %s", conf.Backends)

	srv := newService(conf)
	srv.startTechnicalEndpoint(conf)
	startErr := srv.start()
	if startErr != nil {
		mainlog.Fatalf("Could not start service, reason: %q", startErr.Error())
	}
}

func (s *service) start() error {
	handler, err := regions.NewHandler(s.conf)

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
			Addr:         s.conf.Listen,
			Handler:      handler,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
		},
		Timeout: 10 * time.Second,
	}

	srv.SetKeepAlivesEnabled(true)
	listener, err := net.Listen("tcp", s.conf.Listen)

	if err != nil {
		log.Fatalln(err)
	}

	return srv.Serve(listener)
}

func newService(cfg config.Config) *service {
	return &service{conf: cfg}
}
func (s *service) startTechnicalEndpoint(conf config.Config) {
	log.Printf("Starting technical HTTP endpoint on port: %q", conf.TechnicalEndpointListen)
	serveMuxHandler := http.NewServeMux()
	serveMuxHandler.HandleFunc(
		"/configuration/validate",
		config.ValidateConfigurationHTTPHandler,
	)
	go func() {
		srv := &graceful.Server{
			Server: &http.Server{
				Addr:           conf.TechnicalEndpointListen,
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
