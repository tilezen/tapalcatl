package cache

import (
	"github.com/tilezen/tapalcatl/pkg/handler"
)

type Cache interface {
	GetTile(req *handler.ParseResult) (*handler.VectorTileResponseData, error)
	SetTile(req *handler.ParseResult, resp *handler.VectorTileResponseData)
}

// NilCache implements the Cache interface with no-ops.
type NilCache struct {
}

func (n NilCache) GetTile(req *handler.ParseResult) (*handler.VectorTileResponseData, error) {
	return nil, nil
}

func (n NilCache) SetTile(req *handler.ParseResult, resp *handler.VectorTileResponseData) {
	return
}
