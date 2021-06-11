package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/tilezen/tapalcatl/pkg/state"
	"github.com/tilezen/tapalcatl/pkg/tile"
	"github.com/vmihailenco/msgpack/v5"
)

type Cache interface {
	GetTile(ctx context.Context, req *state.ParseResult) (*state.VectorTileResponseData, error)
	SetTile(ctx context.Context, req *state.ParseResult, resp *state.VectorTileResponseData, ttl time.Duration) error
	GetMetatile(ctx context.Context, req *state.ParseResult, metaCoord tile.TileCoord) (*state.MetatileResponseData, error)
	SetMetatile(ctx context.Context, req *state.ParseResult, metaCoord tile.TileCoord, resp *state.MetatileResponseData, ttl time.Duration) error
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
}

func buildVectorTileKey(req *state.ParseResult) string {
	buildID := "default"
	if req.BuildID != "" {
		buildID = req.BuildID
	}

	if metatileHandlerExtra, ok := req.AdditionalData.(*state.MetatileParseData); ok {
		return fmt.Sprintf(
			"vector:%s:%d/%d/%d.%s",
			buildID,
			metatileHandlerExtra.Coord.Z,
			metatileHandlerExtra.Coord.X,
			metatileHandlerExtra.Coord.Y,
			metatileHandlerExtra.Coord.Format,
		)
	}
	return ""
}

func buildMetatileKey(req *state.ParseResult, coord tile.TileCoord) string {
	buildID := "default"
	if req.BuildID != "" {
		buildID = req.BuildID
	}

	return fmt.Sprintf("metatile:%s:%d/%d/%d.%s", buildID, coord.Z, coord.X, coord.Y, coord.Format)
}

func marshallVectorTileData(data *state.VectorTileResponseData) ([]byte, error) {
	bytes, err := msgpack.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("error marshalling tile data: %w", err)
	}

	return bytes, nil
}

func unmarshallVectorTileData(data []byte) (*state.VectorTileResponseData, error) {
	responseData := &state.VectorTileResponseData{}

	err := msgpack.Unmarshal(data, responseData)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling tile data: %w", err)
	}

	return responseData, nil
}

func marshallMetatileData(data *state.MetatileResponseData) ([]byte, error) {
	bytes, err := msgpack.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("error marshalling metatile data: %w", err)
	}

	return bytes, nil
}

func unmarshallMetatileData(data []byte) (*state.MetatileResponseData, error) {
	responseData := &state.MetatileResponseData{}

	err := msgpack.Unmarshal(data, responseData)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling metatile data: %w", err)
	}

	return responseData, nil
}

// NilCache is the default instance of nilCache, implementing the Cache interface with no-ops.
var NilCache = nilCache{}

// nilCache implements the Cache interface with no-ops.
type nilCache struct {
}

func (n nilCache) GetMetatile(ctx context.Context, req *state.ParseResult, metaCoord tile.TileCoord) (*state.MetatileResponseData, error) {
	return nil, nil
}

func (n nilCache) SetMetatile(ctx context.Context, req *state.ParseResult, metaCoord tile.TileCoord, resp *state.MetatileResponseData, ttl time.Duration) error {
	return nil
}

func (n nilCache) GetTile(ctx context.Context, req *state.ParseResult) (*state.VectorTileResponseData, error) {
	return nil, nil
}

func (n nilCache) SetTile(ctx context.Context, req *state.ParseResult, resp *state.VectorTileResponseData, ttl time.Duration) error {
	return nil
}

func (n nilCache) Get(ctx context.Context, key string) ([]byte, error) {
	return nil, nil
}

func (n nilCache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	return nil
}
