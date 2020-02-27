package config

import (
	"fmt"

	"github.com/allegro/akubra/internal/brim/admin"
)

// ValidateBrimConfig Brim Yaml values validation
func ValidateBrimConfig(bc BrimConf) bool {
	// err := validator.SetValidationFunc("SupervisorConfValidator", SupervisorConfValidator)
	// if err != nil {
	// 	log.Debugf("ValidateBrimConfig SetValidationFunc on SupervisorConfValidator error: %s", err)
	// }
	// err = validator.SetValidationFunc("AdminConfValidator", AdminConfValidator)
	// if err != nil {
	// 	log.Debugf("ValidateBrimConfig SetValidationFunc on AdminConfValidator error: %s", err)
	// }
	// err = validator.SetValidationFunc("RegionsConfValidator", RegionsConfValidator)
	// if err != nil {
	// 	log.Debugf("ValidateBrimConfig SetValidationFunc on RegionsConfValidator error: %s", err)
	// }
	// valid, validationErrors := validator.Validate(bc)
	// for propertyName, validatorMessage := range validationErrors {
	// 	log.Printf("[ ERROR ] BRIM YAML config validation -> propertyName: '%s', validatorMessage: '%s'\n", propertyName, validatorMessage)
	// }
	// if bc.Admins == nil {
	// 	log.Printf("[ ERROR ] BRIM YAML config validation -> propertyName: '%s', validatorMessage: '%s'\n", "admin", "should not be nil")
	// 	return false
	// }
	return true
}

// AdminConfValidator for "admins" section in brim Yaml configuration
func AdminConfValidator(v interface{}, param string) error {
	msgPfx := "AdminConfValidator: "
	adminsConf, ok := v.(admin.AdminsConf)
	if !ok {
		return fmt.Errorf("Cannot assert admins field to AdminConf type, check configuration scheme - param: %q", param)
	}

	if adminsConf == nil || len(adminsConf) == 0 {
		return fmt.Errorf("%sEmpty admins section - param: %q", msgPfx, param)
	}

	for sectionName, adminConfings := range adminsConf {
		if len(sectionName) == 0 {
			return fmt.Errorf("%sEmpty admins section - param: %q", msgPfx, param)
		}

		err := validateCredentials(msgPfx, sectionName, param, adminConfings)
		if err != nil {
			return err
		}
	}

	return nil
}

// // RegionsConfValidator for "regions" section in brim Yaml configuration
func RegionsConfValidator(v interface{}, param string) error {
	msgPfx := "RegionsConfValidator: "
	regionsConf, ok := v.(RegionsConf)
	if !ok {
		return fmt.Errorf("%sRegionsConf type mismatch in section %q", msgPfx, param)
	}

	if regionsConf == nil || len(regionsConf) == 0 {
		return fmt.Errorf("%sEmpty regions in section %q", msgPfx, param)
	}

	return nil
}

// // SupervisorConfValidator for "SupervisorConfValidator" section in brim Yaml configuration
func SupervisorConfValidator(v interface{}, param string) error {
	msgPfx := "SupervisorConfValidator: "
	supervisorConf, ok := v.(SupervisorConf)
	if !ok {
		return fmt.Errorf("%s SupervisorConf type mismatch in section %q", msgPfx, param)
	}
	if supervisorConf.MaxTasksRunningCount == 0 {
		return fmt.Errorf("%s supervisorConf.MaxTasksRunningCount can't be < 1", msgPfx)
	}
	return nil
}

// TODO: WHY NEVER USED
func WALConfValidator(v interface{}, param string) error {
	msgPfx := "WALConfValidator: "
	walConf, ok := v.(WALConf)
	if !ok {
		return fmt.Errorf("%s WALConfValidator type mismatch in section %q", msgPfx, param)
	}
	if walConf.MaxRecordsPerQuery < 1 {
		return fmt.Errorf("%s WALConfValidator.MaxRecordsPerQuery can't be < 1", msgPfx)
	}
	if walConf.MaxConcurrentMigrations < 1 {
		return fmt.Errorf("%s WALConfValidator.MaxConcurrentMigrations can't be < 1", msgPfx)
	}
	if walConf.MaxEmittedTasksCount < 1 {
		return fmt.Errorf("%s WALConfValidator.MaxEmittedTasksCountcan't be < 1", msgPfx)
	}
	return nil
}

func validateCredentials(msgPfx, sectionName, param string, adminConfings []admin.Conf) error {
	if len(adminConfings) < 1 {
		return fmt.Errorf("%sCount of clusters must be greather then zero - param: %q", msgPfx, param)
	}
	for _, adminConfValue := range adminConfings {
		if len(adminConfValue.AdminAccessKey) == 0 {
			return fmt.Errorf("%sEmpty or missing 'AdminAccessKey' in section %q - param: %q", msgPfx, sectionName, param)
		}
		if len(adminConfValue.AdminSecretKey) == 0 {
			return fmt.Errorf("%sEmpty or missing 'AdminSecretKey' in section %q - param: %q", msgPfx, sectionName, param)
		}
		if len(adminConfValue.Endpoint) == 0 {
			return fmt.Errorf("%sEmpty or missing 'Endpoint' in section %q - param: %q", msgPfx, sectionName, param)
		}
		if len(adminConfValue.AdminPrefix) == 0 {
			return fmt.Errorf("%sEmpty or missing 'AdminPrefix' in section %q - param: %q", msgPfx, sectionName, param)
		}
		if len(adminConfValue.ClusterDomain) == 0 {
			return fmt.Errorf("%sEmpty or missing 'ClusterDomain' in section %q - param: %q", msgPfx, sectionName, param)
		}
	}
	return nil
}
