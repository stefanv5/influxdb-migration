package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/migration-tools/influx-migrator/pkg/types"
	"github.com/spf13/viper"
)

func Load(configPath string) (*types.MigrationConfig, error) {
	v := viper.New()

	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")

	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	if err := substituteEnvVars(v); err != nil {
		return nil, fmt.Errorf("failed to substitute env vars: %w", err)
	}

	var cfg types.MigrationConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}

func substituteEnvVars(v *viper.Viper) error {
	configMap := v.AllSettings()

	visitFunc := func(key string, value interface{}) error {
		if str, ok := value.(string); ok {
			if strings.HasPrefix(str, "${") && strings.HasSuffix(str, "}") {
				envVar := str[2 : len(str)-1]
				parts := strings.Split(envVar, ":")
				envKey := parts[0]

				envValue := os.Getenv(envKey)
				if envValue == "" && len(parts) > 1 {
					envValue = parts[1]
				}

				v.Set(key, envValue)
			}
		}
		return nil
	}

	return visitMap(configMap, visitFunc)
}

func visitMap(m map[string]interface{}, fn func(string, interface{}) error) error {
	for k, v := range m {
		if err := fn(k, v); err != nil {
			return err
		}
		switch val := v.(type) {
		case map[string]interface{}:
			if err := visitMap(val, fn); err != nil {
				return err
			}
		}
	}
	return nil
}
