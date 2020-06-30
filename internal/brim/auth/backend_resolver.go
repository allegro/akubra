package auth

import (
	"fmt"
	"net/url"
	"strings"

	wc "github.com/allegro/akubra/internal/akubra/watchdog/config"

	"github.com/AdRoll/goamz/s3"
	akubraconfig "github.com/allegro/akubra/internal/akubra/config"
	akubracrdstore "github.com/allegro/akubra/internal/akubra/crdstore"

	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/akubra/sharding"
	"github.com/allegro/akubra/internal/akubra/storages"
	"github.com/allegro/akubra/internal/akubra/transport"

	"github.com/allegro/akubra/internal/brim/admin"
	"github.com/allegro/akubra/internal/brim/config"
	brimS3 "github.com/allegro/akubra/internal/brim/s3"
)

// BackendResolver resolves backends based on urls
type BackendResolver interface {
	//ResolveClient returns a client that should be used for operations for the specified key and host
	ResolveClientForHost(hostURL, key, access string) (*s3.S3, error)
	//ResolveClient returns a client that should be used for operations for the specified backend
	ResolveClientForBackend(backendName, hostURL, access string) (*s3.S3, error)
	//GetShardsRing resolves a shard ring for a given domain
	GetShardsRing(domain string) (sharding.ShardsRingAPI, error)
}

// ConfigBasedBackendResolver points replicas where object should be stored
type ConfigBasedBackendResolver struct {
	akubraConfig         *akubraconfig.Config
	brimConfig           *config.BrimConf
	endpointToAdminCreds map[string]admin.Conf
	akubraLookuper       *akubraConfigLookuper
	credentialsStore     *akubracrdstore.CredentialsStore
}

// NewConfigBasedBackendResolver constructs new BackendSolver
func NewConfigBasedBackendResolver(akubraConfig *akubraconfig.Config, brimConfig *config.BrimConf) *ConfigBasedBackendResolver {
	akubracrdstore.InitializeCredentialsStores(akubraConfig.CredentialsStores)

	credentialsStoreInstance, err := akubracrdstore.GetInstance(akubracrdstore.DefaultCredentialsStoreName)
	if err != nil {
		log.Println("Crdstore client not initialized")
	}
	bs := ConfigBasedBackendResolver{
		akubraConfig:         akubraConfig,
		brimConfig:           brimConfig,
		endpointToAdminCreds: make(map[string]admin.Conf),
		akubraLookuper:       newLookup(akubraConfig),
		credentialsStore:     credentialsStoreInstance,
	}

	bs.mapDomainToAdminConf(brimConfig)
	return &bs
}

func (bs *ConfigBasedBackendResolver) mapDomainToAdminConf(brimConfig *config.BrimConf) {
	for _, admins := range brimConfig.Admins {
		for _, radosGWAdminConf := range admins {
			u, err := url.Parse(radosGWAdminConf.Endpoint)
			if err != nil {
				log.Fatalf("Missconfigured endpoint is not url.Parse'able: %s", radosGWAdminConf.Endpoint)
			}
			bs.endpointToAdminCreds[u.Hostname()] = radosGWAdminConf
		}
	}
}

// ResolveClient returns proper s3 client to perform operations on object
func (bs *ConfigBasedBackendResolver) ResolveClientForHost(hostURL, key, access string) (*s3.S3, error) {
	backendName, ok := bs.akubraLookuper.matchAkubraBackendName(hostURL, key)
	if !ok {
		return nil, fmt.Errorf("hostURL does not fit to akubra configuration %q", hostURL)
	}
	return bs.ResolveClientForBackend(backendName, hostURL, access)
}

// ResolveClient returns proper s3 client to perform operations on object
func (bs *ConfigBasedBackendResolver) ResolveClientForBackend(backendName, hostURL, access string) (*s3.S3, error) {
	s3client, err := bs.lookupUsingCrdStor(backendName, hostURL, access)
	if err != nil {
		return nil, fmt.Errorf("crdstor credentials retrieval failed %s %s %s, reason: %s", backendName, hostURL, access, err)
	}
	log.Printf("Credentials from crdstor retrieval succeed %s %s %s", backendName, hostURL, access)
	return s3client, nil
}

//GetShardsRing finds a ShardsRing for a given domain
func (bs *ConfigBasedBackendResolver) GetShardsRing(domain string) (sharding.ShardsRingAPI, error) {
	policy, regionFound := bs.akubraLookuper.domainToPolicyName[domain]
	if !regionFound {
		return nil, fmt.Errorf("domain '%s' is not assigned to any sharding policy", domain)
	}
	ring, _, err := Ring(bs.akubraConfig, policy)
	if err != nil {
		return nil, err
	}
	return ring, nil
}

func (bs *ConfigBasedBackendResolver) lookupUsingCrdStor(akubraBackendName, hostURL, access string) (*s3.S3, error) {
	csCreds, err := bs.credentialsStore.Get(access, akubraBackendName)
	if err != nil {
		return nil, err
	}
	s3Client := brimS3.GetS3Client(&brimS3.MigrationAuth{
		Endpoint:  hostURL,
		AccessKey: csCreds.AccessKey,
		SecretKey: csCreds.SecretKey,
	})
	return s3Client, nil
}

func (bs *ConfigBasedBackendResolver) detectAdminCreds(hostURL, key string) (admin.Conf, error) {
	u, err := url.Parse(hostURL)
	if err != nil {
		return admin.Conf{}, fmt.Errorf("hostUrl is not parsable %s", err)
	}

	domain := u.Hostname()

	if admin, ok := bs.endpointToAdminCreds[domain]; ok {
		return admin, nil
	}

	backends := bs.akubraLookuper.solveHostKeyBackendNames(hostURL, key)
	if len(backends) > 0 {
		backendName := backends[0]
		backendConf := bs.akubraConfig.Storages[backendName]
		backendURL := backendConf.Backend
		adminConf, ok := bs.endpointToAdminCreds[backendURL.Hostname()]
		if !ok {
			return admin.Conf{}, fmt.Errorf("no admin creds associated with host: %s", backendURL.Hostname())
		}
		return adminConf, nil
	}

	return admin.Conf{}, fmt.Errorf("could not find proper admin for hostUrl %s and key %s", hostURL, key)
}

// Ring initializes shards rings
func Ring(conf *akubraconfig.Config, policy string) (sharding.ShardsRingAPI, string, error) {
	transportMatcher, err := transport.ConfigureHTTPTransports(conf.Service.Client)
	if err != nil {
		log.Fatalf("Couldn't set up client Transports - err: %q", err)
	}

	storagesFactory := storages.NewStoragesFactory(transportMatcher, &wc.WatchdogConfig{}, nil, nil)
	ringStorages, err := storagesFactory.InitStorages(conf.Shards, conf.Storages, conf.IgnoredCanonicalizedHeaders)

	if err != nil {
		return sharding.ShardsRing{}, " ", err
	}

	ringFactory := sharding.NewRingFactory(*conf, ringStorages, nil, nil, "")

	regionCfg, exists := conf.ShardingPolicies[policy]

	if !exists {
		return sharding.ShardsRing{}, "", fmt.Errorf("policy %v not found", policy)
	}
	regionRing, err := ringFactory.RegionRing(policy, *conf, regionCfg)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("regionCfg.Domains %v ", regionCfg.Domains)
	return regionRing, "http://" + regionCfg.Domains[0] + conf.Service.Server.Listen, err
}

type akubraConfigLookuper struct {
	akubraConfig       *akubraconfig.Config
	hostPortName       map[string]string
	domainToPolicyName map[string]string
	regionNameToRing   map[string]sharding.ShardsRingAPI
}

func newLookup(akubraConfig *akubraconfig.Config) *akubraConfigLookuper {
	lookup := &akubraConfigLookuper{akubraConfig: akubraConfig}
	if lookup.hostPortName == nil {
		lookup.hostPortName = make(map[string]string)
		for name, data := range lookup.akubraConfig.Storages {
			lookup.hostPortName[data.Backend.URL.Host] = name
		}
	}
	lookup.mapDomainAndRegionNames()
	err := lookup.mapRegionNameAndRing()
	if err != nil {
		log.Fatalf("Failed to create new Lookuper, couldn't map region name to ring %s", err)
	}
	return lookup
}

// returns akubra backend name if matches
func (lookup *akubraConfigLookuper) matchAkubraBackendName(hostPort, key string) (string, bool) {
	hostURL := getURL(hostPort)
	name, ok := lookup.hostPortName[hostURL.Host]
	if !ok {
		backends := lookup.solveHostKeyBackendNames(hostURL.Host, key)
		if len(backends) > 0 {
			return backends[0], true
		}
	}
	return name, ok
}

func (lookup *akubraConfigLookuper) solveHostKeyBackendNames(host, key string) []string {
	u := getURL(host)
	domain := u.Hostname()
	var ring sharding.ShardsRingAPI
	regionName, ok := lookup.domainToPolicyName[domain]
	if !ok {
		return nil
	}

	ring, ok = lookup.regionNameToRing[regionName]
	if !ok {
		return nil
	}

	namedCluster, err := ring.Pick(key)
	if err != nil {
		return nil
	}

	clusterConf := lookup.akubraConfig.Shards[namedCluster.Name()]
	backends := make([]string, len(clusterConf.Storages))
	for idx := range clusterConf.Storages {
		backends[idx] = clusterConf.Storages[idx].Name
	}
	return backends
}

func (lookup *akubraConfigLookuper) mapDomainAndRegionNames() {
	akubraConfig := lookup.akubraConfig
	lookup.domainToPolicyName = make(map[string]string)
	for name, region := range akubraConfig.ShardingPolicies {
		for _, domain := range region.Domains {
			lookup.domainToPolicyName[domain] = name
		}
	}
}

func (lookup *akubraConfigLookuper) mapRegionNameAndRing() error {
	akubraConfig := lookup.akubraConfig
	lookup.regionNameToRing = make(map[string]sharding.ShardsRingAPI)
	for name := range akubraConfig.ShardingPolicies {
		r, _, err := Ring(akubraConfig, name)
		if err != nil {
			return err
		}
		lookup.regionNameToRing[name] = r
	}
	return nil
}

func getURL(hostPort string) *url.URL {
	if !strings.HasPrefix(hostPort, "http") {
		hostPort = fmt.Sprintf("http://%s", hostPort)
	}
	u, err := url.Parse(hostPort)
	if err != nil {
		log.Printf("URL Parse failure %s", err)
	}
	return u
}
