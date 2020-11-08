package handler

import (
	"net/http"

	"github.com/tilezen/tapalcatl/pkg/state"
	"github.com/tilezen/tapalcatl/pkg/storage"
)

type ParseResultType int

const (
	ParseResultType_Nil ParseResultType = iota
	ParseResultType_Metatile
	ParseResultType_Tilejson
)

type Parser interface {
	Parse(*http.Request) (*ParseResult, error)
}

type ParseResult struct {
	Type        ParseResultType
	Cond        storage.Condition
	ContentType string
	HttpData    state.HttpRequestData
	// set to be more specific data based on parse type
	AdditionalData interface{}
}
