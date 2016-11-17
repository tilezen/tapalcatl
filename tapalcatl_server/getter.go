package main

import (
	"github.com/tilezen/tapalcatl"
	"time"
)

type Condition struct {
	IfModifiedSince *time.Time
	IfNoneMatch     *string
}

// a Getter is something that can get (i.e: fetch) tiles
type Getter interface {
	Get(t tapalcatl.TileCoord, c Condition) (*Response, error)
}
