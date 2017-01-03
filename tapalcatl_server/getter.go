package main

import (
	"github.com/tilezen/tapalcatl"
	"io"
	"time"
)

type Condition struct {
	IfModifiedSince *time.Time
	IfNoneMatch     *string
}

type SuccessfulResponse struct {
	Body         io.ReadCloser
	LastModified *time.Time
	ETag         *string
}

type GetResponse struct {
	Response    *SuccessfulResponse
	NotModified bool
	NotFound    bool
}

// a Getter is something that can get (i.e: fetch) tiles
type Getter interface {
	Get(t tapalcatl.TileCoord, c Condition) (*GetResponse, error)
}
