package metrics

import "github.com/tilezen/tapalcatl/pkg/state"

type MetricsWriter interface {
	WriteMetatileState(*state.RequestState)
	WriteTileJsonState(*state.TileJsonRequestState)
}

type NilMetricsWriter struct{}

func (_ *NilMetricsWriter) WriteMetatileState(reqState *state.RequestState)             {}
func (_ *NilMetricsWriter) WriteTileJsonState(jsonReqState *state.TileJsonRequestState) {}
