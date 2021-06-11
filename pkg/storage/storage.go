package storage

import (
	"time"

	"github.com/tilezen/tapalcatl/pkg/state"
	"github.com/tilezen/tapalcatl/pkg/tile"
)

type Storage interface {
	Fetch(t tile.TileCoord, c state.Condition, prefixOverride string) (*StorageResponse, error)
	TileJson(f state.TileJsonFormat, c state.Condition, prefixOverride string) (*StorageResponse, error)
	HealthCheck() error
}

type SuccessfulResponse struct {
	Body         []byte
	LastModified *time.Time
	ETag         *string
	Size         uint64
}

type StorageResponse struct {
	Response    *SuccessfulResponse
	NotModified bool
	NotFound    bool
}
