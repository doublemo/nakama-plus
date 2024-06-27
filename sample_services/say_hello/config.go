package main

import (
	"os"
	"path/filepath"

	"github.com/doublemo/nakama-kit/kit"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type Configuration struct {
	Config      []string          `yaml:"config" json:"config" usage:"The absolute file path to configuration YAML file."`
	Server      *kit.ServerConfig `yaml:"server" json:"server" usage:"server"`
	CurrentPath string
}

func (c *Configuration) Check(log *zap.Logger) error {
	return c.Server.Valid()
}

func (c *Configuration) GetServer() kit.Config {
	return c.Server
}

func (c *Configuration) Parse() error {
	for _, path := range c.Config {
		f, err := os.Stat(path)
		if os.IsNotExist(err) {
			return err
		}

		if !f.IsDir() {
			return c.parseYml(path)
		}

		matches, err := filepath.Glob(filepath.Join(path, "*.yml"))
		if err != nil {
			return err
		}

		for _, cfg := range matches {
			if err := c.parseYml(cfg); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Configuration) parseYml(cfg string) error {
	data, err := os.ReadFile(cfg)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(data, c)
	if err != nil {
		return err
	}
	return nil
}

func NewConfiguration(log *zap.Logger) *Configuration {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal("Error getting current working directory.", zap.Error(err))
	}

	serverConfig := kit.NewServerConfig()
	serverConfig.AllowStream = true
	return &Configuration{
		Server:      serverConfig,
		CurrentPath: cwd,
	}
}
