package config

import (
	"fmt"
	"os"
)

// Config holds runtime configuration from environment.
type Config struct {
	// DATABASE_URL: Postgres 連線字串 (必填)
	DatabaseURL string
	// STATICS_HOST: 靜態圖片 host，例如 https://v3-statics-dev.mirrormedia.mg/images (必填)
	StaticsHost string
	// PORT: 服務監聽埠，未設定時預設 8080 (選填)
	Port string
}

// Load reads required environment variables.
// DATABASE_URL and STATICS_HOST are mandatory.
// PORT is optional; defaults to "8080".
func Load() (Config, error) {
	cfg := Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		StaticsHost: os.Getenv("STATICS_HOST"),
		Port:        os.Getenv("PORT"),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL not set")
	}
	if cfg.StaticsHost == "" {
		return Config{}, fmt.Errorf("STATICS_HOST not set")
	}
	if cfg.Port == "" {
		cfg.Port = "8080"
	}
	return cfg, nil
}
