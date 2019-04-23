package config

import (
	"testing"

	"github.com/stretchr/testify/assert"

	rados "github.com/allegro/akubra/internal/brim/admin"
	"github.com/allegro/akubra/internal/brim/model"
)

// YamlConfigTest for tests defaults
type BrimConfTest struct {
	BrimConf
}

// RegionsClientsConfTest for tests defaults
type RegionsConfTest struct {
	RegionsConf
}

var adminConfigTestPayload = rados.Conf{
	Endpoint:       "Endpoint",
	AdminAccessKey: "AdminAccessKey",
	AdminSecretKey: "AdminSecretKey",
	AdminPrefix:    "AdminPrefix",
	ClusterDomain:  "ClusterDomain",
}

var supervisorConfig = SupervisorConf{
	MaxTasksRunningCount:         2,
	MaxInstancesRunningInAPIMode: 1,
	MaxInstancesRunningInDbMode:  2,
}

// NewBrimConfTest tests func for updating fields values in tests cases
func (t *BrimConfTest) NewBrimConfTest() *BrimConf {
	var adminsConfig rados.AdminsConf
	t.BrimConf = prepareYamlConfig(adminsConfig, supervisorConfig, "dbUser", "dbPassword", "dbName", "dbHost", false)
	return &t.BrimConf
}

func TestShouldNotValidateEmptyAdmins(t *testing.T) {
	var testConf BrimConfTest
	testConf.NewBrimConfTest().Admins = nil

	result := ValidateBrimConfig(testConf.BrimConf)

	assert.False(t, result, "Should be false")
}

func TestShouldNotValidateAdminsWhenOneSectionAdminConfExists(t *testing.T) {
	var testConf BrimConfTest
	adminsConf := map[string][]rados.Conf{
		"test": {adminConfigTestPayload},
	}

	testConf.NewBrimConfTest().Admins = adminsConf
	result := ValidateBrimConfig(testConf.BrimConf)

	assert.True(t, result, "Should be true")
}

func TestShouldNotValidateAdminsWhenEnvSectionIsEmpty(t *testing.T) {
	var testConf BrimConfTest
	adminsConf := map[string][]rados.Conf{
		"": {adminConfigTestPayload, adminConfigTestPayload},
	}

	testConf.NewBrimConfTest().Admins = adminsConf

	result := ValidateBrimConfig(testConf.BrimConf)

	assert.False(t, result, "Should be false")
}

func TestShouldValidateUserWithMinLength(t *testing.T) {
	var testConf BrimConfTest
	testConf.NewBrimConfTest().Database.User = "u"

	result := ValidateBrimConfig(testConf.BrimConf)

	assert.True(t, result, "Should be true")
}

func TestShouldNotValidateUserWithLowerThanMinLength(t *testing.T) {
	var testConf BrimConfTest
	testConf.NewBrimConfTest().Database.User = ""

	result := ValidateBrimConfig(testConf.BrimConf)

	assert.False(t, result, "Should be false")
}

func TestShouldValidateEmptyPassword(t *testing.T) {
	var testConf BrimConfTest
	testConf.NewBrimConfTest().Database.Password = ""

	result := ValidateBrimConfig(testConf.BrimConf)

	assert.True(t, result, "Should be true")
}

func TestShouldValidateDBNameWithMinLength(t *testing.T) {
	var testConf BrimConfTest
	testConf.NewBrimConfTest().Database.DBName = "db11"

	result := ValidateBrimConfig(testConf.BrimConf)

	assert.True(t, result, "Should be true")
}

func TestShouldNotValidateDBNameWithLowerThanMinLength(t *testing.T) {
	var testConf BrimConfTest
	testConf.NewBrimConfTest().Database.DBName = "db1"

	result := ValidateBrimConfig(testConf.BrimConf)

	assert.False(t, result, "Should be false")
}

func TestShouldValidateHostWithMinLength(t *testing.T) {
	var testConf BrimConfTest
	testConf.NewBrimConfTest().Database.Host = "http://test.internal"

	result := ValidateBrimConfig(testConf.BrimConf)

	assert.True(t, result, "Should be true")
}

func TestShouldNotValidateHostWithLowerThanMinLength(t *testing.T) {
	var testConf BrimConfTest
	testConf.NewBrimConfTest().Database.Host = "htt"

	result := ValidateBrimConfig(testConf.BrimConf)

	assert.False(t, result, "Should be false")
}

func TestShouldValidateRegionsClientsConfValidator(t *testing.T) {
	var regionsConf RegionsConfTest
	regionsConf.RegionsConf = map[string]RegionConfig{
		"myregion1": {Users: []rados.ConfigUser{
			rados.ConfigUser{Name: "user1", Prefix: ""},
			rados.ConfigUser{Name: "user2", Prefix: ""},
		}},
	}

	result := RegionsConfValidator(regionsConf.RegionsConf, "regions")

	assert.Nil(t, result, "Should be nil")
}

func TestShouldConsiderConfigAsValidWhenSupervisorConfigHasAnInvalidMaxTasksProperty(t *testing.T) {
	var testConf BrimConfTest
	testConf.NewBrimConfTest().Supervisor.MaxTasksRunningCount = 0

	err := SupervisorConfValidator(testConf.BrimConf.Supervisor, "")

	assert.Equal(t, err.Error(), "SupervisorConfValidator:  supervisorConf.MaxTasksRunningCount can't be < 1")
}

func TestShouldNotValidateRegionsClientsConfValidatorWithEmptyRegions(t *testing.T) {
	var regionsConf RegionsConfTest

	result := RegionsConfValidator(regionsConf.RegionsConf, "regions")

	assert.NotNil(t, result, "Should not be nil")
}

func prepareYamlConfig(adminsConf rados.AdminsConf, supervisorConfig SupervisorConf, dbUser, dbPassword, dbName,
	dbHost string, sslEnabled bool) BrimConf {
	var bc BrimConf

	if adminsConf == nil {
		adminsConf = map[string][]rados.Conf{
			"test": {adminConfigTestPayload, adminConfigTestPayload},
		}
	}

	dbConfig := model.DBConfig{
		User:       dbUser,
		Password:   dbPassword,
		DBName:     dbName,
		Host:       dbHost,
		SSLEnabled: sslEnabled,
	}

	bc.Supervisor = supervisorConfig
	bc.Database = dbConfig
	bc.Admins = adminsConf
	bc.Regions = map[string]RegionConfig{
		"myregion1": {Users: []rados.ConfigUser{
			rados.ConfigUser{Name: "user1", Prefix: ""},
			rados.ConfigUser{Name: "user2", Prefix: ""},
		}},
	}

	return bc
}
