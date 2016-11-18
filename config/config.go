package config

import (
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
	Backends []YAMLURL `yaml:"Backends,omitempty,flow"`
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
	KeepAlive bool `yaml:"KeepAlive"`
}



//Config contains processed YamlConfig data
type Config struct {
	YamlConfig
	SyncLogMethodsSet set.Set
	Synclog           *log.Logger
	Accesslog         *log.Logger
	Mainlog           *log.Logger
}

<<<<<<< HEAD

=======
//YAMLURL type fields in yaml configuration will parse urls
>>>>>>> 89f85db8afc02ed6c5faf2d55a5ff7eaaec7b035
type YAMLURL struct {
	*url.URL
}

<<<<<<< HEAD

=======
//UnmarshalYAML parses strings to url.URL
>>>>>>> 89f85db8afc02ed6c5faf2d55a5ff7eaaec7b035
func (j *YAMLURL) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	url, err := url.Parse(s)
<<<<<<< HEAD
=======
	if url.Host == "" {
		return fmt.Errorf("url should match proto://host[:port]/path scheme, got %q", s)
	}
>>>>>>> 89f85db8afc02ed6c5faf2d55a5ff7eaaec7b035
	j.URL = url
	return err
}

<<<<<<< HEAD

=======
>>>>>>> 89f85db8afc02ed6c5faf2d55a5ff7eaaec7b035
//Parse json config
func parseConf(file io.Reader) (YamlConfig, error) {
	rc := YamlConfig{}
	bs, err := ioutil.ReadAll(file)

	if err != nil {
		return rc, err
	}
	err = yaml.Unmarshal(bs, &rc)
	return rc, err
}

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
<<<<<<< HEAD
func Configure() (conf Config, err error) {

	conf = Config{}
	flag.Parse()
	if confFile, openErr := os.Open(*confFilePath); openErr != nil {
		yconf, parseErr := parseConf(confFile)
		if parseErr != nil {
			return conf, parseErr
		}
		conf = Config{YamlConfig: yconf}
	}

	confFile, openErr := os.Open(*confFilePath)
	if openErr == nil {
		yconf, parseErr := parseConf(confFile)
		if parseErr != nil {
			return conf, parseErr
		}
		conf = Config{YamlConfig: yconf}
	} else {
		fmt.Println("Cannot read config file:", openErr.Error())
		return Config{}, openErr
=======
func Configure(configFilePath string) (conf Config, err error) {
	confFile, err := os.Open(configFilePath)
	if err != nil {
		return
	}

	yconf, err := parseConf(confFile)
	if err != nil {
		return
>>>>>>> 89f85db8afc02ed6c5faf2d55a5ff7eaaec7b035
	}
	conf.YamlConfig = yconf

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
	return
}
