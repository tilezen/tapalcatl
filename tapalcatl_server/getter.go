package main

import (
	"github.com/tilezen/tapalcatl"
)

// a Getter is something that can get (i.e: fetch) tiles
type Getter interface {
	Get(t tapalcatl.TileCoord) (*Response, error)
}
