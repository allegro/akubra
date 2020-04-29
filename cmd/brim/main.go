package main

import (
	"bytes"
	"fmt"
	"github.com/alecthomas/kingpin"
	"github.com/allegro/akubra/internal/akubra/config"
	"github.com/allegro/akubra/internal/akubra/config/vault"
	"github.com/allegro/akubra/internal/akubra/log"
	bConf "github.com/allegro/akubra/internal/brim/config"
	watchdog "github.com/allegro/akubra/internal/brim/watchdog-main"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"io"
	"net/http"
	"os"
)

var (
	akubraConfig = kingpin.
			Flag("aconfig", "Configuration file path e.g.: \"conf/dev.yaml\"").
			Short('a').
			ExistingFile()

	brimConfig = kingpin.
			Flag("bconfig", "Configuration file path e.g.: \"conf/dev.yaml\"").
			Short('b').
			ExistingFile()
	akubraVersionVarName = "AKUBRA_VERSION"
)

func main() {
	kingpin.Parse()
	akubraConf, err := readAkubraConfiguration()
	if err != nil {
		log.Fatalf("Improperly configured %s", err)
	}
	brimConf, err := bConf.Configure(*brimConfig)
	if err != nil {
		log.Fatalf("Improperly configured %s", err)
	}
	go runHealthCheck()
	watchdog.RunWatchdogWorker(&akubraConf, &brimConf)
}
func runHealthCheck() {
	http.HandleFunc("/status/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "OK")
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}
func readAkubraConfiguration() (config.Config, error) {
	if vault.DefaultClient != nil {
		return readVaultConfiguration()
	}
	return readFileConfiguration()
}

func readVaultConfiguration() (config.Config, error) {
	log.Println("Vault client initialized")
	version := os.Getenv(akubraVersionVarName)
	revPath := fmt.Sprintf("configuration/%s/current", version)

	revData, err := vault.DefaultClient.Read(revPath)
	if err != nil {
		return config.Config{}, err
	}

	revisionMap, ok := revData["secret"].(map[string]interface{})
	if !ok {
		log.Fatalf("Could not map revData to map[string]interface{} %#v", revData)
	}

	revision, ok := revisionMap["revision"].(string)
	if !ok {
		log.Fatalf("Could not assert revision to string %#v", revision)
	}
	log.Printf("Configuration version %s revision: %s\n", version, revision)

	path := fmt.Sprintf("configuration/%s/%s", version, revision)

	v, err := vault.DefaultClient.Read(path)
	if err != nil {
		return config.Config{}, err
	}

	log.Println("Configuration read successful")

	configString, ok := v["secret"].(string)
	if !ok {
		log.Fatal("Could not assert secret to string map")
	}
	configReader := bytes.NewReader([]byte(configString))
	return parseConfig(configReader)
}

func readFileConfiguration() (config.Config, error) {
	configReadCloser, err := config.ReadConfiguration(*akubraConfig)
	log.Println("Read configuration from file")
	defer func() {
		err = configReadCloser.Close()
		if err != nil {
			log.Debugf("Cannot close configuration, reason: %s", err)
		}
	}()

	if err != nil {
		log.Fatalf("Could not read configuration file {}", *akubraConfig)
	}
	return parseConfig(configReadCloser)
}

func parseConfig(reader io.Reader) (config.Config, error) {
	conf, err := config.Configure(reader)
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
