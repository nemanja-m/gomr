package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// WorkerConfig contains all configuration for the worker service.
type WorkerConfig struct {
	Server      ServerConfig         `mapstructure:"server"`
	Coordinator CoordinatorConnConfig `mapstructure:"coordinator"`
	Logging     LoggingConfig        `mapstructure:"logging"`
}

// ServerConfig contains worker server configuration.
type ServerConfig struct {
	Addr string `mapstructure:"addr"`
}

// CoordinatorConnConfig contains coordinator connection configuration.
type CoordinatorConnConfig struct {
	Addr string           `mapstructure:"addr"`
	GRPC WorkerGRPCConfig `mapstructure:"grpc"`
}

// WorkerGRPCConfig contains worker gRPC client configuration.
type WorkerGRPCConfig struct {
	KeepaliveTime    time.Duration `mapstructure:"keepalive_time"`
	KeepaliveTimeout time.Duration `mapstructure:"keepalive_timeout"`
}

// LoadWorker loads the worker configuration from the given path.
// If configPath is empty, it looks for worker.yaml in the config/ directory.
// Environment variables with GOMR_WORKER_ prefix override config file values.
func LoadWorker(configPath string) (*WorkerConfig, error) {
	v := viper.New()

	v.SetDefault("server.addr", ":50051")
	v.SetDefault("coordinator.addr", "localhost:9090")
	v.SetDefault("coordinator.grpc.keepalive_time", 30*time.Second)
	v.SetDefault("coordinator.grpc.keepalive_timeout", 5*time.Second)
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")

	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		v.SetConfigName("worker")
		v.SetConfigType("yaml")
		v.AddConfigPath("./config")
		v.AddConfigPath(".")
	}

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	v.SetEnvPrefix("GOMR_WORKER")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg WorkerConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	return &cfg, nil
}
