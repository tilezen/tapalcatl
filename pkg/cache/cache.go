package cache

import (
	"context"
	"fmt"

	"github.com/tilezen/tapalcatl/pkg/state"
	"github.com/vmihailenco/msgpack/v5"
)

type Cache interface {
	GetTile(ctx context.Context, req *state.ParseResult) (*state.VectorTileResponseData, error)
	SetTile(ctx context.Context, req *state.ParseResult, resp *state.VectorTileResponseData) error
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, val []byte) error
}

func buildKey(req *state.ParseResult) string {
	if metatileHandlerExtra, ok := req.AdditionalData.(*state.MetatileParseData); ok {
		buildID := "default"
		if req.BuildID != "" {
			buildID = req.BuildID
		}

		return fmt.Sprintf(
			"%s-%d/%d/%d.%s",
			buildID,
			metatileHandlerExtra.Coord.Z,
			metatileHandlerExtra.Coord.X,
			metatileHandlerExtra.Coord.Y,
			metatileHandlerExtra.Coord.Format,
		)
	}
	return ""
}

func marshallData(data *state.VectorTileResponseData) ([]byte, error) {
	bytes, err := msgpack.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("error marshalling tile data: %w", err)
	}

	return bytes, nil
}

func unmarshallData(data []byte) (*state.VectorTileResponseData, error) {
	responseData := &state.VectorTileResponseData{}

	err := msgpack.Unmarshal(data, responseData)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling tile data: %w", err)
	}

	return responseData, nil
}

// NilCache is the default instance of nilCache, implementing the Cache interface with no-ops.
var NilCache = nilCache{}

// nilCache implements the Cache interface with no-ops.
type nilCache struct {
}

func (n nilCache) GetTile(ctx context.Context, req *state.ParseResult) (*state.VectorTileResponseData, error) {
	return nil, nil
}

func (n nilCache) SetTile(ctx context.Context, req *state.ParseResult, resp *state.VectorTileResponseData) error {
	return nil
}

func (n nilCache) Get(ctx context.Context, key string) ([]byte, error) {
	return nil, nil
}

func (n nilCache) Set(ctx context.Context, key string, val []byte) error {
	return nil
}
