package cache

import (
	"fmt"

	"github.com/tilezen/tapalcatl/pkg/state"
)

type Cache interface {
	GetTile(req *state.ParseResult) (*state.VectorTileResponseData, error)
	SetTile(req *state.ParseResult, resp *state.VectorTileResponseData) error
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

// NilCache implements the Cache interface with no-ops.
type NilCache struct {
}

func (n NilCache) GetTile(req *state.ParseResult) (*state.VectorTileResponseData, error) {
	return nil, nil
}

func (n NilCache) SetTile(req *state.ParseResult, resp *state.VectorTileResponseData) error {
	return nil
}
