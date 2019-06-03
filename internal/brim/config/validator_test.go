package config

import (
	"testing"

	"github.com/stretchr/testify/assert"

	rados "github.com/allegro/akubra/internal/brim/admin"
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
	t.BrimConf = prepareYamlConfig(adminsConfig, supervisorConfig)
	return &t.BrimConf
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

func prepareYamlConfig(adminsConf rados.AdminsConf, supervisorConfig SupervisorConf) BrimConf {
	var bc BrimConf

	if adminsConf == nil {
		adminsConf = map[string][]rados.Conf{
			"test": {adminConfigTestPayload, adminConfigTestPayload},
		}
	}

	bc.Supervisor = supervisorConfig
	bc.Admins = adminsConf
	bc.Regions = map[string]RegionConfig{
		"myregion1": {Users: []rados.ConfigUser{
			rados.ConfigUser{Name: "user1", Prefix: ""},
			rados.ConfigUser{Name: "user2", Prefix: ""},
		}},
	}

	return bc
}
