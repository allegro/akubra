package config

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"log/syslog"
	"net/url"
	"os"

	set "github.com/deckarep/golang-set"
	"github.com/go-yaml/yaml"
)

//YamlConfig contains configuration fields of config file
type YamlConfig struct {
	//Listen interface and port e.g. "0:8000", "localhost:9090", ":80"
	Listen string `yaml:"Listen,omitempty"`
	//List of backend uri's e.g. "http://s3.mydaracenter.org"
	Backends []string `yaml:"Backends,omitempty,flow"`
	//Limit of outgoing connections. When limit is reached, akubra will omit external backend
	//with greatest number of stalled connections
	ConnLimit int64 `yaml:"ConnLimit,omitempty"`
	//Additional not amazon specific headers proxy will add to original request
	AdditionalRequestHeaders map[string]string `yaml:"AdditionalRequestHeaders,omitempty"`
	//Additional headers added to backend response
	AdditionalResponseHeaders map[string]string `yaml:"AdditionalResponseHeaders,omitempty"`
	//Read timeout on outgoing connections
	ConnectionTimeout string `yaml:"ConnectionTimeout,omitempty"`
	//Dial timeout on outgoing connections
	ConnectionDialTimeout string `yaml:"ConnectionDialTimeout,omitempty"`
	//Backend in maintenance mode. Akubra will not send data there
	MaintainedBackend string `yaml:"MaintainedBackend,omitempty"`
	//List request methods to be logged in synclog in case of backend failure
	SyncLogMethods []string `yaml:"SyncLogMethods,omitempty"`
	//Should we keep alive connections with backend servers
	KeepAlive bool `yaml:"SyncLogMethods,omitempty"`
}

//Config contains processed YamlConfig data
type Config struct {
	YamlConfig
	BackendURLs       []*url.URL
	SyncLogMethodsSet set.Set
	Synclog           *log.Logger
	Accesslog         *log.Logger
	Mainlog           *log.Logger
}

//Parse json config
func parseConf(file io.Reader) (YamlConfig, error) {
	rc := YamlConfig{}
	bs, err := ioutil.ReadAll(file)

	if err != nil {
		return rc, err
	}

	err = yaml.Unmarshal(bs, &rc)

	if err != nil {
		println("got unmarshal err", err.Error())
		return rc, err
	}
	return rc, nil
}

var confFilePath = flag.String("c", "", "Configuration file e.g.: \"conf/dev.json\"")

func setupLoggers(conf *Config) error {
	accesslog, slErr := syslog.NewLogger(syslog.LOG_LOCAL0, log.LstdFlags)
	conf.Accesslog = accesslog
	conf.Accesslog.SetPrefix("access")
	if slErr != nil {
		return slErr
	}
	conf.Synclog, slErr = syslog.NewLogger(syslog.LOG_LOCAL1, log.LstdFlags)
	conf.Synclog.SetPrefix("")
	if slErr != nil {
		return slErr
	}
	conf.Mainlog, slErr = syslog.NewLogger(syslog.LOG_LOCAL2, log.LstdFlags)
	conf.Mainlog.SetPrefix("main")
	if slErr != nil {
		fmt.Println("co", slErr.Error())
	}
	return slErr
}

// Configure parse configuration file
func Configure() (conf Config, err error) {

	conf = Config{}

	if confFile, openErr := os.Open(*confFilePath); openErr != nil {
		yconf, parseErr := parseConf(confFile)
		if parseErr != nil {
			return conf, parseErr
		}
		conf = Config{YamlConfig: yconf}
	}

	conf.BackendURLs = make([]*url.URL, 0, len(conf.Backends))
	for _, rawstr := range conf.Backends {
		url, perr := url.Parse(rawstr)
		if perr != nil {
			return conf, perr
		}
		conf.BackendURLs = append(conf.BackendURLs, url)
	}

	if len(conf.MaintainedBackend) > 0 {
		u, perr := url.Parse(conf.MaintainedBackend)
		if perr != nil {
			return conf, perr
		}
		conf.MaintainedBackend = u.Host
	}

	if len(conf.SyncLogMethods) > 0 {
		conf.SyncLogMethodsSet = set.NewThreadUnsafeSet()
		for _, v := range conf.SyncLogMethods {
			conf.SyncLogMethodsSet.Add(v)
		}
	} else {
		conf.SyncLogMethodsSet = set.NewThreadUnsafeSetFromSlice(
			[]interface{}{"PUT", "GET", "HEAD", "DELETE", "OPTIONS"})
	}
	err = setupLoggers(&conf)
	return conf, err
}
