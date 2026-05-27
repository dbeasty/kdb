package config

import (
	"encoding/json"
	"os"
)

// ProductConfig is loaded from kdb-product.json (Kotlin kdb-config parity).
type ProductConfig struct {
	Features map[string]bool `json:"features"`
	AuthPath string          `json:"authPath,omitempty"`
	TLS      *TLSConfig      `json:"tls,omitempty"`
}

type TLSConfig struct {
	Enabled  bool   `json:"enabled"`
	CertFile string `json:"certFile,omitempty"`
	KeyFile  string `json:"keyFile,omitempty"`
}

func LoadProduct(path string) (ProductConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return ProductConfig{}, err
	}
	var cfg ProductConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return ProductConfig{}, err
	}
	return cfg, nil
}

func DefaultFeatures() map[string]bool {
	return map[string]bool{
		"peerSync": true,
		"stream":   true,
		"sql":      true,
	}
}
