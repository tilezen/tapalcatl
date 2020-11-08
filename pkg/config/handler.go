package config

import (
	"encoding/json"
	"fmt"
)

type HandlerConfig struct {
	Aws     *awsConfig
	Storage map[string]storageDefinition
	Pattern map[string]routeHandlerConfig
	Mime    map[string]string
}

func (h *HandlerConfig) String() string {
	return fmt.Sprintf("%#v", *h)
}

func (h *HandlerConfig) Set(line string) error {
	err := json.Unmarshal([]byte(line), h)
	if err != nil {
		return fmt.Errorf("Unable to parse value as a JSON object: %s", err.Error())
	}
	return nil
}

// the handler config is the container for the json configuration
// storageDefinition contains the base options for a particular storage
// storageConfig contains the specific options for a particular pattern
// pattern ties together request patterns with storageConfig
// awsConfig contains session-wide options for aws backed storage

// "s3" and "file" are the possible storage definition types

// generic aws configuration applied to whole session
type awsConfig struct {
	Region *string
}

type storageDefinition struct {
	Type string

	// common fields across all storage types
	// these can be overridden in specific storage configuration
	MetatileSize int

	// TileSize indicates the size of tile for this pattern. The default is 1.
	TileSize *int

	// S3 key or file path to check for during healthcheck
	Healthcheck string

	// s3 specific fields
	Layer      string
	Bucket     string
	KeyPattern string

	// file specific fields
	BaseDir string
}

// storage configuration, specific to a pattern
type storageConfig struct {
	// matches storage definition name
	Storage string

	MetatileSize *int

	// TileSize indicates the size of tile for this pattern. The default is 1.
	TileSize *int

	// Prefix is required to be set for s3 storage
	Prefix     *string
	KeyPattern *string
	Layer      *string

	BaseDir *string
}

type routeHandlerConfig struct {
	storageConfig
	Type *string
}

