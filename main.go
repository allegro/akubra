package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/allegro/akubra/log"
	logconfig "github.com/allegro/akubra/log/config"
	"gopkg.in/tylerb/graceful.v1"

	"github.com/alecthomas/kingpin"
	"github.com/allegro/akubra/config"

	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/metrics"
	"github.com/allegro/akubra/regions"
	"github.com/allegro/akubra/storages"
	_ "github.com/lib/pq"

	"github.com/allegro/akubra/crdstore"
	"github.com/allegro/akubra/transport"
)

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

	valid, errs := config.ValidateConf(conf.YamlConfig, true)
	if !valid {
		log.Printf("YAML validation - errors: %q", errs)
		os.Exit(1)
	}
	log.Println("Configuration checked - OK.")


	mainlog, err := log.NewDefaultLogger(conf.Logging.Mainlog, "LOG_LOCAL2", false)
	if err != nil {
		log.Fatalf("Could not set up main logger: %q", err)
	}

	log.DefaultLogger = mainlog

	if *testConfig {
		os.Exit(0)
	}

	log.Printf("Health check endpoint: %s", conf.Service.Server.HealthCheckEndpoint)
	mainlog.Printf("starting on port %s", conf.Service.Server.Listen)
	mainlog.Printf("backends %#v", conf.Backends)

	srv := newService(conf)
	srv.startTechnicalEndpoint()
	startErr := srv.start()
	if startErr != nil {
		mainlog.Fatalf("Could not start service, reason: %q", startErr.Error())
	}
}

func mkServiceLogs(logConf logconfig.LoggingConfig) (syncLog, clusterSyncLog, accessLog log.Logger, err error) {
	syncLog, err = log.NewDefaultLogger(logConf.Synclog, "LOG_LOCAL1", true)
	if err != nil {
		return
	}

	clusterSyncLog, err = log.NewDefaultLogger(logConf.ClusterSyncLog, "LOG_LOCAL1", true)
	if err != nil {
		return
	}
	accessLog, err = log.NewDefaultLogger(logConf.Accesslog, "LOG_LOCAL1", true)
	if err != nil {
		return
	}
	return
}

func (s *service) start() error {
	transportMatcher, err := transport.ConfigureHTTPTransports(s.config.Service.Client)
	if err != nil {
		log.Fatalf("Couldn't set up client Transports - err: %q", err)
	}
	syncLog, clusterSyncLog, accessLog, err := mkServiceLogs(s.config.Logging)
	if err != nil {
		return err
	}
	methods := make(map[string]struct{})
	for _, method := range s.config.Logging.SyncLogMethods {
		methods[method] = struct{}{}
	}

	crdstore.InitializeCredentialsStore(s.config.CredentialsStore)
	syncSender := &storages.SyncSender{SyncLog: syncLog, AllowedMethods: methods}
	storage, err := storages.InitStorages(
		transportMatcher,
		s.config.Clusters,
		s.config.Backends,
		syncSender)

	if err != nil {
		log.Fatalf("Storages initialization problem: %q", err)
	}

	regionsRT, err := regions.NewRegions(s.config.Regions, storage, clusterSyncLog)
	if err != nil {
		return err
	}

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
			ReadTimeout:  s.config.Service.Server.ReadTimeout.Duration,
			WriteTimeout: s.config.Service.Server.WriteTimeout.Duration,
		},
		Timeout: s.config.Service.Server.ShutdownTimeout.Duration,
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
