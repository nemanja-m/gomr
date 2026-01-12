package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// CoordinatorConfig contains all configuration for the coordinator service.
type CoordinatorConfig struct {
	REST    RESTConfig    `mapstructure:"rest"`
	GRPC    GRPCConfig    `mapstructure:"grpc"`
	Health  HealthConfig  `mapstructure:"health"`
	Logging LoggingConfig `mapstructure:"logging"`
}

// RESTConfig contains REST API server configuration.
type RESTConfig struct {
	Addr         string        `mapstructure:"addr"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	IdleTimeout  time.Duration `mapstructure:"idle_timeout"`
}

// GRPCConfig contains gRPC server configuration.
type GRPCConfig struct {
	Addr              string        `mapstructure:"addr"`
	EnableReflection  bool          `mapstructure:"enable_reflection"`
	KeepaliveMinTime  time.Duration `mapstructure:"keepalive_min_time"`
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`
}

// HealthConfig contains worker health checking configuration.
type HealthConfig struct {
	CheckInterval time.Duration `mapstructure:"check_interval"`
	StaleTimeout  time.Duration `mapstructure:"stale_timeout"`
}

// LoadCoordinator loads the coordinator configuration from the given path.
// If configPath is empty, it looks for coordinator.yaml in the config/ directory.
// Environment variables with GOMR_COORDINATOR_ prefix override config file values.
func LoadCoordinator(configPath string) (*CoordinatorConfig, error) {
	v := viper.New()

	v.SetDefault("rest.addr", ":8080")
	v.SetDefault("rest.read_timeout", 15*time.Second)
	v.SetDefault("rest.write_timeout", 15*time.Second)
	v.SetDefault("rest.idle_timeout", 60*time.Second)
	v.SetDefault("grpc.addr", ":9090")
	v.SetDefault("grpc.enable_reflection", true)
	v.SetDefault("grpc.keepalive_min_time", 30*time.Second)
	v.SetDefault("grpc.heartbeat_interval", 15*time.Second)
	v.SetDefault("health.check_interval", 5*time.Second)
	v.SetDefault("health.stale_timeout", 15*time.Second)
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")

	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		v.SetConfigName("coordinator")
		v.SetConfigType("yaml")
		v.AddConfigPath("./config")
		v.AddConfigPath(".")
	}

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	v.SetEnvPrefix("GOMR_COORDINATOR")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg CoordinatorConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	return &cfg, nil
}
