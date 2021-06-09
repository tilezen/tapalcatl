package storage

import (
	"fmt"
	"io"
	"time"

	"github.com/tilezen/tapalcatl/pkg/tile"
)

type Storage interface {
	Fetch(t tile.TileCoord, c Condition, prefixOverride string) (*StorageResponse, error)
	TileJson(f TileJsonFormat, c Condition, prefixOverride string) (*StorageResponse, error)
	HealthCheck() error
}

type Condition struct {
	IfModifiedSince *time.Time
	IfNoneMatch     *string
}

type SuccessfulResponse struct {
	Body         io.ReadCloser
	LastModified *time.Time
	ETag         *string
	Size         uint64
}

type StorageResponse struct {
	Response    *SuccessfulResponse
	NotModified bool
	NotFound    bool
}

type TileJsonFormat int

const (
	TileJsonFormat_Mvt = iota
	TileJsonFormat_Json
	TileJsonFormat_Topojson
)

func (f *TileJsonFormat) Name() string {
	switch *f {
	case TileJsonFormat_Mvt:
		return "mapbox"
	case TileJsonFormat_Json:
		return "geojson"
	case TileJsonFormat_Topojson:
		return "topojson"
	}
	panic(fmt.Sprintf("Unknown tilejson format: %d", int(*f)))
}

func NewTileJsonFormat(name string) *TileJsonFormat {
	var format TileJsonFormat
	switch name {
	case "mapbox":
		format = TileJsonFormat_Mvt
	case "geojson":
		format = TileJsonFormat_Json
	case "topojson":
		format = TileJsonFormat_Topojson
	default:
		return nil
	}
	return &format
}
