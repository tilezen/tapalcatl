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

// NilCache implements the Cache interface with no-ops.
type NilCache struct {
}

func (n NilCache) GetTile(ctx context.Context, req *state.ParseResult) (*state.VectorTileResponseData, error) {
	return nil, nil
}

func (n NilCache) SetTile(ctx context.Context, req *state.ParseResult, resp *state.VectorTileResponseData) error {
	return nil
}
