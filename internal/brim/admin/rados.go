package admin

import (
	"log"
	"strings"
	"sync"
	"time"

	"github.com/allegro/akubra/internal/akubra/metrics"
	radosAPI "github.com/mjarco/go-radosgw/pkg/api"
)

const usersWorkersCount = 5

// ConfigUser for configuration purposes
type ConfigUser struct {
	Name   string `yaml:"uid"`
	Prefix string `yaml:"prefix"`
}

type adminAPI interface {
	GetUser(string) (*radosAPI.User, error)
	GetUsers() ([]string, error)
	GetBucket(conf radosAPI.BucketConfig) (radosAPI.Buckets, error)
}

// Credentials keeps access and secret keys
type Credentials struct {
	AccessKey, SecretKey string
}

// BucketCreds keeps credential to bucket
type BucketCreds struct {
	AccessKey, SecretKey, Endpoint, BucketName string
}

// Conf keeps Credentials and access data for rados
// admin account
type Conf struct {
	BackendName    string `yaml:"backendname"`
	Endpoint       string `yaml:"endpoint"`
	AdminAccessKey string `yaml:"adminaccesskey"`
	AdminSecretKey string `yaml:"adminsecretkey"`
	AdminPrefix    string `yaml:"adminprefix"`
	ClusterDomain  string `yaml:"clusterdomain"`
}

// AdminsConf is admin configurations map
type AdminsConf map[string][]Conf

func listBucketsWithAuth(api adminAPI, source string) (map[string][]BucketCreds, error) {

	allUsers, err := api.GetUsers()

	if err != nil {
		return nil, err
	}
	BucketCredsMap := make(map[string][]BucketCreds)
	BucketCredsChan := make(chan struct {
		user  string
		creds []BucketCreds
	})
	userNames := make(chan string)

	wg := sync.WaitGroup{}
	wg.Add(usersWorkersCount)

	for workerNo := 1; workerNo <= usersWorkersCount; workerNo++ {
		go func(workerNo int) {
			usersCredsCount := 0
			for userName := range userNames {
				utasks, err := ProcessUser(api, userName, source, "")
				if err == nil {
					BucketCredsChan <- struct {
						user  string
						creds []BucketCreds
					}{user: userName, creds: utasks}
					usersCredsCount += len(utasks)
				} else {
					log.Printf("ERROR from 'processUser': %v (in worker: %v)\n", err, workerNo)
				}
			}
			log.Printf("STATS processUser - workerNo: %v, usersCredsCount: %v\n", workerNo, usersCredsCount)
			wg.Done()
		}(workerNo)
	}

	go func() {
		for BucketCredsItem := range BucketCredsChan {
			BucketCredsMap[BucketCredsItem.user] = BucketCredsItem.creds
		}
	}()

	for _, userName := range allUsers {
		userNames <- userName
	}

	close(userNames)
	wg.Wait()
	close(BucketCredsChan)
	return BucketCredsMap, nil
}

// ProcessUser lists all user buckets with Credentials
func ProcessUser(api adminAPI, userName, endpoint, forceGreater string) ([]BucketCreds, error) {
	log.Printf("Processing user: %s\n", userName)
	user, err := api.GetUser(userName)
	if err != nil {
		return nil, err
	}

	userBuckets, err := api.GetBucket(radosAPI.BucketConfig{UID: userName})
	if err != nil {
		return nil, err
	}

	tasks := []BucketCreds{}
	for _, bucket := range userBuckets {
		if bucket.Name == "" {
			continue
		}
		if forceGreater != "" && bucket.Name <= forceGreater {
			continue
		}
		tasks = append(tasks, BucketCreds{
			BucketName: bucket.Name,
			AccessKey:  user.Keys[0].AccessKey,
			SecretKey:  user.Keys[0].SecretKey,
			Endpoint:   endpoint,
		})
	}
	return tasks, nil
}

// GetCredsForKey finds Credentials on rgwadmin pointed in ac
func GetCredsForKey(ac Conf, key string) (access, secret string, err error) {
	bucketName := strings.SplitN(key, "/", 2)[0]
	owner, err := getBucketOwner(ac, bucketName)
	if err != nil {
		return
	}
	creds, err := GetUserCreds(ac, owner)
	if err != nil {
		return
	}
	return creds.AccessKey, creds.SecretKey, err
}

func getBucketOwner(ac Conf, bucketName string) (string, error) {
	start := time.Now()
	defer metrics.UpdateSince("req.admin.bucket", start)
	api, err := radosAPI.New(ac.Endpoint, ac.AdminAccessKey, ac.AdminSecretKey, ac.AdminPrefix)
	if err != nil {
		return "", err
	}

	buckets, err := api.GetBucket(radosAPI.BucketConfig{Bucket: bucketName, Stats: true})
	if err != nil {
		log.Printf("Bucket %s at %s info error: %q\n", bucketName, ac.Endpoint, err)
		return "", err
	}
	bucket := buckets[0]
	return bucket.Stats.Owner, nil
}

// GetUserCreds returns Credentials
func GetUserCreds(radosGWAdminConf Conf, owner string) (creds Credentials, err error) {
	defer metrics.UpdateSince("req.admin.user", time.Now())
	api, err := radosAPI.New(radosGWAdminConf.Endpoint, radosGWAdminConf.AdminAccessKey, radosGWAdminConf.AdminSecretKey, radosGWAdminConf.AdminPrefix)
	if err != nil {
		log.Printf("Rados API client initialization error %s", err)
	}

	user, err := api.GetUser(owner)
	if err != nil {
		log.Printf("User %s at %s info error: %q\n", owner, radosGWAdminConf.Endpoint, err)
		return
	}
	return Credentials{
		AccessKey: user.Keys[0].AccessKey,
		SecretKey: user.Keys[0].SecretKey,
	}, nil
}

// GetUserBucketsCredsList returns all buckets for users
func GetUserBucketsCredsList(radosGWAdminConf Conf, users []ConfigUser) (map[string][]BucketCreds, error) {
	source := radosGWAdminConf.Endpoint
	api, err := radosAPI.New(radosGWAdminConf.Endpoint, radosGWAdminConf.AdminAccessKey, radosGWAdminConf.AdminSecretKey, radosGWAdminConf.AdminPrefix)
	if err != nil {

		return nil, err
	}

	var result = make(map[string][]BucketCreds)
	for _, user := range users {
		creds, err := ProcessUser(api, user.Name, source, user.Prefix)
		if err != nil {
			return nil, err
		}
		result[user.Name] = creds
	}

	return result, nil
}

// GetBucketsCredsList returns all buckets on given cluster
func GetBucketsCredsList(radosGWAdminConf Conf) (map[string][]BucketCreds, error) {
	source := radosGWAdminConf.Endpoint
	api, err := radosAPI.New(radosGWAdminConf.Endpoint, radosGWAdminConf.AdminAccessKey, radosGWAdminConf.AdminSecretKey, radosGWAdminConf.AdminPrefix)
	if err != nil {
		return nil, err
	}
	bucketsCreds, err := listBucketsWithAuth(api, source)
	if err != nil {
		return nil, err
	}
	return bucketsCreds, err
}

// New returns rados admin client
func New(radosGWAdminConf Conf) (*radosAPI.API, error) {
	return radosAPI.New(radosGWAdminConf.Endpoint, radosGWAdminConf.AdminAccessKey, radosGWAdminConf.AdminSecretKey, radosGWAdminConf.AdminPrefix)
}

func createUser(uid string, destAPI, sourceAPI *radosAPI.API) (*radosAPI.User, error) {
	user, err := sourceAPI.GetUser(uid)
	if err != nil {
		log.Fatalf("Impossible. Source RadosGW admin api has found no user %s, %q", uid, err)
	}
	uc := radosAPI.UserConfig{
		AccessKey:   user.Keys[0].AccessKey,
		SecretKey:   user.Keys[0].SecretKey,
		DisplayName: user.DisplayName,
		Email:       user.Email,
		GenerateKey: false,
		MaxBuckets:  &user.MaxBuckets,
		UID:         uid,
	}
	return destAPI.CreateUser(uc)
}

//CopyUser creates user copy on destination cluster if needed
func CopyUser(uid string, sourceConf, destConf Conf) *radosAPI.User {
	log.Printf("destAPI setup %v\n", destConf.Endpoint)
	dest, err := New(destConf)
	if err != nil {
		log.Fatalf("Admin is misscofigured %q", err)
	}

	user, err := dest.GetUser(uid)
	if err != nil {
		source, err := New(sourceConf)
		if err != nil {
			log.Printf("Error: Cannot create user %s as source admin not responding %q", uid, err)
		}
		user, err = createUser(uid, dest, source)
		if err != nil {
			log.Printf("Error: Cannot create user %s on dest cluster %q", uid, err)
		}
	}
	return user
}
