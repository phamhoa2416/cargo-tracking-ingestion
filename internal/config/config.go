package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server    ServerConfig
	Database  DatabaseConfig
	Redis     RedisConfig
	RabbitMQ  RabbitMQConfig
	MQTT      MQTTConfig
	JWT       JWTConfig
	Worker    WorkerConfig
	RateLimit RateLimitConfig
}
type ServerConfig struct {
	Port            int
	Host            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
}
type DatabaseConfig struct {
	Host            string
	Port            int
	User            string
	Password        string
	DBName          string
	SSLMode         string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}
type RedisConfig struct {
	Host         string
	Port         int
	DB           int
	PoolSize     int
	MinIdleConns int
	MaxRetries   int
}
type RabbitMQConfig struct {
	URL               string
	Exchange          string
	EventQueue        string
	DeviceUpdateQueue string
	PrefetchCount     int
	Durable           bool
	CustomerTag       string
}
type MQTTConfig struct {
	Broker            string
	Port              int
	ClientID          string
	Username          string
	Password          string
	QoS               int
	CleanSession      bool
	KeepAlive         time.Duration
	ConnectTimeout    time.Duration
	AutoReconnect     bool
	MaxReconnectDelay time.Duration
	TelemetryTopic    string
	HeartbeatTopic    string
	CommandTopic      string
}

type JWTConfig struct {
	PublicKeyPath string
	Issuer        string
	Audience      string
}

type WorkerConfig struct {
	BatchSize            int
	BatchTimeout         time.Duration
	EventDetectorWorkers int
	CacheSyncInterval    time.Duration
}

type RateLimitConfig struct {
	RequestsPerSecond int
	Burst             int
}

func setDefaults() {
	viper.SetDefault("SERVER_PORT", 8080)
	viper.SetDefault("SERVER_HOST", "localhost")
	viper.SetDefault("SERVER_READ_TIMEOUT", "15s")
	viper.SetDefault("SERVER_WRITE_TIMEOUT", "15s")
	viper.SetDefault("SERVER_SHUTDOWN_TIMEOUT", "10s")

	viper.SetDefault("DATABASE_HOST", "localhost")
	viper.SetDefault("DATABASE_PORT", 5432)
	viper.SetDefault("DATABASE_USER", "postgres")
	viper.SetDefault("DATABASE_PASSWORD", "postgres")
	viper.SetDefault("DATABASE_NAME", "cargo_tracking")
	viper.SetDefault("DATABASE_SSLMODE", "disable")
	viper.SetDefault("DATABASE_MAX_OPEN_CONNS", 25)
	viper.SetDefault("DATABASE_MAX_IDLE_CONNS", 5)
	viper.SetDefault("DATABASE_CONN_MAX_LIFETIME", "5m")
	viper.SetDefault("DATABASE_CONN_MAX_IDLE_TIME", "10m")

	viper.SetDefault("REDIS_HOST", "localhost")
	viper.SetDefault("REDIS_PORT", 6379)
	viper.SetDefault("REDIS_DB", 0)
	viper.SetDefault("REDIS_POOL_SIZE", 10)
	viper.SetDefault("REDIS_MIN_IDLE_CONNS", 2)
	viper.SetDefault("REDIS_MAX_RETRIES", 3)

	viper.SetDefault("RABBIT_URL", "amqp://guest:guest@localhost:5672/")
	viper.SetDefault("RABBIT_EXCHANGE", "cargo.events")
	viper.SetDefault("RABBIT_EVENT_QUEUE", "device.events")
	viper.SetDefault("RABBIT_DEVICE_UPDATE_QUEUE", "device.updates")
	viper.SetDefault("RABBIT_CUSTOMER_TAG", "device.ingestion.service")
	viper.SetDefault("RABBIT_PREFETCH_COUNT", 10)
	viper.SetDefault("RABBIT_DURABLE", true)

	viper.SetDefault("MQTT_BROKER", "localhost")
	viper.SetDefault("MQTT_PORT", 1883)
	viper.SetDefault("MQTT_CLIENT_ID", "cargo-ingestion-service")
	viper.SetDefault("MQTT_USERNAME", "")
	viper.SetDefault("MQTT_QOS", 1)
	viper.SetDefault("MQTT_CLEAN_SESSION", false)
	viper.SetDefault("MQTT_KEEP_ALIVE", "60s")
	viper.SetDefault("MQTT_CONNECT_TIMEOUT", "10s")
	viper.SetDefault("MQTT_AUTO_RECONNECT", true)
	viper.SetDefault("MQTT_MAX_RECONNECT_DELAY", "5m")
	viper.SetDefault("MQTT_TELEMETRY_TOPIC", "device/+/telemetry")
	viper.SetDefault("MQTT_HEARTBEAT_TOPIC", "device/+/heartbeat")
	viper.SetDefault("MQTT_COMMAND_TOPIC", "device/+/command")

	viper.SetDefault("JWT_PUBLIC_KEY_PATH", "./resources/keys/public.pem")
	viper.SetDefault("JWT_ISSUER", "cargo-backend-service")
	viper.SetDefault("JWT_AUDIENCE", "cargo-services")

	viper.SetDefault("WORKER_BATCH_SIZE", 100)
	viper.SetDefault("WORKER_BATCH_TIMEOUT", "5s")
	viper.SetDefault("WORKER_EVENT_DETECTOR_WORKERS", 4)
	viper.SetDefault("WORKER_CACHE_SYNC_INTERVAL", "5m")
	
	viper.SetDefault("RATE_LIMIT_REQUEST_PER_SECOND", 100)
	viper.SetDefault("RATE_LIMIT_BURST", 200)
}

func Load() (*Config, error) {
	setDefaults()

	viper.SetConfigFile(".env")
	viper.AddConfigPath(".")
	if homeDir, err := os.UserHomeDir(); err == nil {
		viper.AddConfigPath(homeDir)
	}
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if errors.As(err, &configFileNotFoundError) {
			fmt.Println("Config file not found, using defaults and env vars")
		}
	}

	config := &Config{
		Server: ServerConfig{
			Port:            viper.GetInt("SERVER_PORT"),
			Host:            viper.GetString("SERVER_HOST"),
			ReadTimeout:     viper.GetDuration("SERVER_READ_TIMEOUT"),
			WriteTimeout:    viper.GetDuration("SERVER_WRITE_TIMEOUT"),
			ShutdownTimeout: viper.GetDuration("SERVER_SHUTDOWN_TIMEOUT"),
		},
		Database: DatabaseConfig{
			Host:            viper.GetString("DATABASE_HOST"),
			Port:            viper.GetInt("DATABASE_PORT"),
			User:            viper.GetString("DATABASE_USER"),
			Password:        viper.GetString("DATABASE_PASSWORD"),
			DBName:          viper.GetString("DATABASE_NAME"),
			SSLMode:         viper.GetString("DATABASE_SSLMODE"),
			MaxOpenConns:    viper.GetInt("DATABASE_MAX_OPEN_CONNS"),
			MaxIdleConns:    viper.GetInt("DATABASE_MAX_IDLE_CONNS"),
			ConnMaxLifetime: viper.GetDuration("DATABASE_CONN_MAX_LIFETIME"),
			ConnMaxIdleTime: viper.GetDuration("DATABASE_CONN_MAX_IDLE_TIME"),
		},
		Redis: RedisConfig{
			Host:         viper.GetString("REDIS_HOST"),
			Port:         viper.GetInt("REDIS_PORT"),
			DB:           viper.GetInt("REDIS_DB"),
			PoolSize:     viper.GetInt("REDIS_POOL_SIZE"),
			MinIdleConns: viper.GetInt("REDIS_MIN_IDLE_CONNS"),
			MaxRetries:   viper.GetInt("REDIS_MAX_RETRIES"),
		},
		RabbitMQ: RabbitMQConfig{
			URL:               viper.GetString("RABBIT_URL"),
			Exchange:          viper.GetString("RABBIT_EXCHANGE"),
			EventQueue:        viper.GetString("RABBIT_EVENT_QUEUE"),
			CustomerTag:       viper.GetString("RABBIT_CUSTOMER_TAG"),
			DeviceUpdateQueue: viper.GetString("RABBIT_DEVICE_UPDATE_QUEUE"),
			PrefetchCount:     viper.GetInt("RABBIT_PREFETCH_COUNT"),
			Durable:           viper.GetBool("RABBIT_DURABLE"),
		},
		MQTT: MQTTConfig{
			Broker:            viper.GetString("MQTT_BROKER"),
			Port:              viper.GetInt("MQTT_PORT"),
			ClientID:          viper.GetString("MQTT_CLIENT_ID"),
			Username:          viper.GetString("MQTT_USERNAME"),
			QoS:               viper.GetInt("MQTT_QOS"),
			CleanSession:      viper.GetBool("MQTT_CLEAN_SESSION"),
			KeepAlive:         viper.GetDuration("MQTT_KEEP_ALIVE"),
			ConnectTimeout:    viper.GetDuration("MQTT_CONNECT_TIMEOUT"),
			AutoReconnect:     viper.GetBool("MQTT_AUTO_RECONNECT"),
			MaxReconnectDelay: viper.GetDuration("MQTT_MAX_RECONNECT_DELAY"),
			TelemetryTopic:    viper.GetString("MQTT_TELEMETRY_TOPIC"),
			HeartbeatTopic:    viper.GetString("MQTT_HEARTBEAT_TOPIC"),
			CommandTopic:      viper.GetString("MQTT_COMMAND_TOPIC"),
		},
		JWT: JWTConfig{
			PublicKeyPath: viper.GetString("JWT_PUBLIC_KEY_PATH"),
			Issuer:        viper.GetString("JWT_ISSUER"),
			Audience:      viper.GetString("JWT_AUDIENCE"),
		},
		Worker: WorkerConfig{
			BatchSize:            viper.GetInt("WORKER_BATCH_SIZE"),
			BatchTimeout:         viper.GetDuration("WORKER_BATCH_TIMEOUT"),
			EventDetectorWorkers: viper.GetInt("WORKER_EVENT_DETECTOR_WORKERS"),
			CacheSyncInterval:    viper.GetDuration("WORKER_CACHE_SYNC_INTERVAL"),
		},
		RateLimit: RateLimitConfig{
			RequestsPerSecond: viper.GetInt("RATE_LIMIT_REQUEST_PER_SECOND"),
			Burst:             viper.GetInt("RATE_LIMIT_BURST"),
		},
	}

	return config, nil
}

func (c *Config) Validate() error {
	if c.Server.Port <= 0 {
		return errors.New("invalid server port")
	}
	if c.Worker.BatchSize <= 0 {
		return errors.New("invalid worker batch size")
	}

	return nil
}

func (c *DatabaseConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host,
		c.Port,
		c.User,
		c.Password,
		c.DBName,
		c.SSLMode,
	)
}
