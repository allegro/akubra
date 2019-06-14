package config

import (
	"io"
	"io/ioutil"
	"os"

	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/akubra/metrics"
	"gopkg.in/yaml.v2"

	// "github.com/allegro/akubra/internal/brim/admin" # spare

	"time"

	"github.com/allegro/akubra/internal/brim/admin"
)

//LoggingConfig hold the configuration for loggers
type LoggingConfig struct {
	Mainlog log.LoggerConfig `yaml:"Mainlog"`
	APILog  log.LoggerConfig `yaml:"APILog"`
	DBLog   log.LoggerConfig `yaml:"DBLog"`
}

// RegionConfig brim-specific settings for region
type RegionConfig struct {
	Users []admin.ConfigUser `yaml:"users"`
}

// RegionsConf map of region settings
type RegionsConf map[string]RegionConfig //spare

// RegionData with key and cluster domain
type RegionData struct {
	RegionKey, ClusterDomain string
}

// RegionsData for mapping backend endpoint with RegionsData
type RegionsData map[string]RegionData

// SupervisorConf keeps supervisor limits
type SupervisorConf struct {
	MaxInstancesRunningInAPIMode uint `yaml:"MaxInstancesRunningInAPIMode"`
	MaxInstancesRunningInDbMode  uint `yaml:"MaxInstancesRunningInDbMode"`
	MaxTasksRunningCount         uint `yaml:"MaxTasksRunningCount"`
}

type SourceType = string
type SourceProps = map[string]string

type WALConf struct {
	NoRecordsSleepDuration  time.Duration `yaml:"NoRecordsSleepDuration"`
	MaxRecordsPerQuery      int           `yaml:"MaxRecordsPerQuery"`
	MaxConcurrentMigrations int           `yaml:"MaxConcurrentMigrations"`
	BurstFeeder             bool          `yaml:"BurstFeeder"`
	MaxEmittedTasksCount    int           `yaml:"MaxEmittedTasksCount"`
	TaskEmissionDuration    time.Duration `yaml:"TaskEmissionDuration"`
	FeederTaskFailureDelay  time.Duration `yaml:"FeederTaskFailureDelay"`
}

// BrimConf is read from configuration file
type BrimConf struct {
	// Database    model.DBConfig   `yaml:"database"`
	Admins      admin.AdminsConf `yaml:"admins" validate:"AdminConfValidator=Admins"`
	Regions     RegionsConf      `yaml:"regions" validate:"RegionsConfValidator=Regions"`
	urlToRegion map[string]RegionData
	Logging     LoggingConfig `yaml:"Logging,omitempty"`
	// Creds                     []auth.Creds     `yaml:"Creds,omitempty"`
	Metrics                   metrics.Config `yaml:"Metrics,omitempty"`
	DefaultPermanentProcessID uint64         `yaml:"DefaultPermanentProcessID"`
	Supervisor                SupervisorConf `yaml:"Supervisor"`
	WorkerCount               int            `yaml:"workercount"`
	WALConf                   WALConf        `yaml:"WAL"`
}

// EndpointRegionMapping returns region to endpoint map
func (bc *BrimConf) EndpointRegionMapping() RegionsData {
	if bc.urlToRegion == nil {
		bc.urlToRegion = make(RegionsData)
		for key, endpoints := range bc.Admins {
			for _, adminConf := range endpoints {
				bc.urlToRegion[adminConf.Endpoint] = RegionData{
					RegionKey:     key,
					ClusterDomain: adminConf.ClusterDomain,
				}
			}
		}
	}
	return bc.urlToRegion
}

func doClose(resource io.Closer) {
	err := resource.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func readConfFile(confFilePath string) (BrimConf, error) {
	bc := BrimConf{}
	confFile, err := os.Open(confFilePath)
	if err != nil {
		log.Fatalf("[ ERROR ] Problem with opening config file: '%s' - err: %v !", confFilePath, err)
		return bc, err
	}
	defer doClose(confFile)

	bs, err := ioutil.ReadAll(confFile)
	if err != nil {
		return bc, err
	}
	err = yaml.Unmarshal(bs, &bc)
	return bc, err
}

// Configure creates BrimConf
func Configure(confFilePath string) (BrimConf, error) {

	bc, err := readConfFile(confFilePath)
	if err != nil {
		log.Fatalf("Cannot read brim config %s", err.Error())
	}

	if !ValidateBrimConfig(bc) {
		log.Fatalln("BRIM YAML validation error - exit!")
	}
	return bc, err
}
