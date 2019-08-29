package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	watchdogConfig "github.com/allegro/akubra/internal/akubra/watchdog/config"

	"github.com/alecthomas/kingpin"
	"github.com/allegro/akubra/internal/akubra/config"
	"github.com/allegro/akubra/internal/akubra/crdstore"
	"github.com/allegro/akubra/internal/akubra/httphandler"
	"github.com/allegro/akubra/internal/akubra/log"
	logconfig "github.com/allegro/akubra/internal/akubra/log/config"
	"github.com/allegro/akubra/internal/akubra/watchdog"

	"github.com/allegro/akubra/internal/akubra/metrics"
	"github.com/allegro/akubra/internal/akubra/regions"
	"github.com/allegro/akubra/internal/akubra/storages"
	"github.com/allegro/akubra/internal/akubra/transport"

	_ "github.com/lib/pq"
)

// TechnicalEndpointGeneralTimeout for /configuration/validate endpoint
const TechnicalEndpointGeneralTimeout = 5 * time.Second

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

const (
	postgresConnStringFormat = "sslmode=disable dbname=:dbname: user=:user: password=:password: host=:host: port=:port: connect_timeout=:conntimeout:"
)

func main() {
	versionString := fmt.Sprintf("Akubra (%s version)", version)
	kingpin.Version(versionString)
	kingpin.Parse()
	conf, err := parseConfig(*configFile)
	if err != nil {
		log.Fatalf("Configuration corrupted: %s", err)
	}

	if *testConfig {
		os.Exit(0)
	}

	mainlog, err := log.NewDefaultLogger(conf.Logging.Mainlog, "LOG_LOCAL2", false)
	if err != nil {
		log.Fatalf("Could not set up main logger: %q", err)
	}
	log.DefaultLogger = mainlog

	log.Printf("Health check endpoint: %s", conf.Service.Server.HealthCheckEndpoint)
	mainlog.Printf("starting on port %s", conf.Service.Server.Listen)

	srv := newService(conf, *configFile)
	srv.startTechnicalEndpoint()
	startErr := srv.start()
	if startErr != nil {
		mainlog.Fatalf("Could not start service, reason: %q", startErr.Error())
	}
}
func parseConfig(path string) (config.Config, error) {
	conf, err := config.Configure(*configFile)
	if err != nil {
		return config.Config{}, fmt.Errorf("Improperly configured %s", err)
	}

	valid, errs := config.ValidateConf(conf.YamlConfig, true)
	if !valid {
		return config.Config{}, fmt.Errorf("YAML validation - errors: %q", errs)
	}
	log.Println("Configuration checked - OK.")

	return conf, nil
}

func mkServiceLogs(logConf logconfig.LoggingConfig) (accessLog log.Logger, err error) {
	accessLog, err = log.NewDefaultLogger(logConf.Accesslog, "LOG_LOCAL1", true)
	if err != nil {
		return
	}
	return
}

func newService(cfg config.Config, configPath string) *service {
	hh := func(rw http.ResponseWriter, r *http.Request) {}
	var h = http.HandlerFunc(hh)
	return &service{config: cfg, configPath: configPath, handler: h}
}

type service struct {
	config     config.Config
	configPath string
	handler    http.Handler
	srv        *http.Server
	ctx        context.Context
}

func (s *service) start() (err error) {
	s.ctx = context.Background()
	s.handler, err = s.createHandler(s.config)
	if err != nil {
		log.Fatalf("Handler creation error: %s", err)
	}
	srv := &http.Server{
		Addr:         s.config.Service.Server.Listen,
		Handler:      s,
		ReadTimeout:  s.config.Service.Server.ReadTimeout.Duration,
		WriteTimeout: s.config.Service.Server.WriteTimeout.Duration,
	}

	srv.SetKeepAlivesEnabled(true)
	s.srv = srv
	listener, err := net.Listen("tcp", s.config.Service.Server.Listen)
	if err != nil {
		log.Fatalln(err)
	}
	go s.signalsHandler()
	return srv.Serve(listener)
}

func (s *service) signalsHandler() {

	for {
		hup := make(chan os.Signal, 1)
		signal.Notify(hup, syscall.SIGHUP)
		intr := make(chan os.Signal, 1)
		signal.Notify(intr, syscall.SIGINT)
		select {
		case <-hup:
			conf, err := parseConfig(s.configPath)
			if err != nil {
				log.Printf("New config is corrupted %s", err)
				continue
			}
			handler, err := s.createHandler(conf)
			if err != nil {
				log.Printf("Handler initialization failure %s", err)
			}
			s.handler = handler
			log.Println("Handler replaced")
		case <-intr:
			log.Println("Shutting down")
			err := s.srv.Shutdown(s.ctx)
			if err != nil {
				log.Printf("Server shutsown error: %s", err)
			}
			log.Println("Fin")
		}
	}
}

func (s *service) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	handler := s.handler
	handler.ServeHTTP(rw, r)
}
func (s *service) createHandler(conf config.Config) (http.Handler, error) {
	transportMatcher, err := transport.ConfigureHTTPTransports(conf.Service.Client)
	if err != nil {
		return nil, fmt.Errorf("Couldn't set up client Transports - err: %q", err)
	}
	accessLog, err := mkServiceLogs(conf.Logging)
	if err != nil {
		return nil, err
	}

	crdstore.InitializeCredentialsStores(conf.CredentialsStores)

	watchdogRecordFactory := &watchdog.DefaultConsistencyRecordFactory{}
	consistencyWatchdog := setupWatchdog(s.config.Watchdog)

	storagesFactory := storages.NewStoragesFactory(transportMatcher, &s.config.Watchdog, consistencyWatchdog, watchdogRecordFactory)
	ignoredSignHeaders := map[string]bool{s.config.Watchdog.ObjectVersionHeaderName: true}
	for k, v := range conf.IgnoredCanonicalizedHeaders {
		ignoredSignHeaders[k] = v
	}
	storage, err := storagesFactory.InitStorages(s.config.Shards, s.config.Storages, ignoredSignHeaders)

	if err != nil {
		log.Fatalf("Storages initialization problem: %q", err)
		return nil, err
	}

	regionsRT, err := regions.NewRegions(s.config, storage,
		consistencyWatchdog, watchdogRecordFactory, conf.Watchdog.ObjectVersionHeaderName)
	if err != nil {
		return nil, err
	}

	regionsDecoratedRT := httphandler.DecorateRoundTripper(conf.Service.Client,
		accessLog, conf.Service.Server.HealthCheckEndpoint, regionsRT)

	handler, err := httphandler.NewHandlerWithRoundTripper(regionsDecoratedRT, conf.Service.Server)
	if err != nil {
		return nil, err
	}

	err = metrics.Init(conf.Metrics)
	if err != nil {
		log.Printf("Metrics initialization error: %s", err)
	}
	return handler, nil
}

func setupWatchdog(watchdogConfig watchdogConfig.WatchdogConfig) watchdog.ConsistencyWatchdog {
	if watchdogConfig.Type == "" {
		return nil
	}

	consistencyWatchdog, err := watchdog.CreateSQL("postgres",
		postgresConnStringFormat,
		[]string{"user", "password", "dbname", "host", "port", "conntimeout"},
		&watchdogConfig)

	if err != nil {
		log.Fatalf("Failed to create consistencyWatchdog %s", err)
	}

	return consistencyWatchdog
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
		srv := &http.Server{
			Addr:           port,
			Handler:        serveMuxHandler,
			MaxHeaderBytes: 512,
			WriteTimeout:   TechnicalEndpointGeneralTimeout,
			ReadTimeout:    TechnicalEndpointGeneralTimeout,
		}
		log.Fatal(srv.ListenAndServe())
	}()
	log.Println("Technical HTTP endpoint is running.")
}
