package metrics

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/tilezen/tapalcatl/pkg/log"
	"github.com/tilezen/tapalcatl/pkg/state"
)

type StatsdMetricsWriter struct {
	addr   *net.UDPAddr
	prefix string
	logger log.JsonLogger
	queue  chan requestStateContainer
}
type requestStateContainer struct {
	// one of these will be set
	metaReqState     *state.RequestState
	tileJsonReqState *state.TileJsonRequestState
}

func (smw *StatsdMetricsWriter) Process(reqStateContainer requestStateContainer) {
	conn, err := net.DialUDP("udp", nil, smw.addr)
	if err != nil {
		smw.logger.Error(log.LogCategory_Metrics, "Metrics Writer failed to connect to %s: %s\n", smw.addr, err)
		return
	}
	defer conn.Close()

	w := bufio.NewWriter(conn)
	defer w.Flush()

	psw := prefixedStatsdWriter{
		prefix: smw.prefix,
		w:      w,
	}

	psw.WriteCount("count", 1)

	// variables to handle writing of common elements
	var respState *state.ReqResponseState
	var fetchState *state.ReqFetchState
	var storageMetadata *state.ReqStorageMetadata
	var isResponseWriteError *bool
	var isCondError *bool

	if reqStateContainer.metaReqState != nil {
		reqState := reqStateContainer.metaReqState

		psw.WriteCount("metatile", 1)

		respState = &reqState.ResponseState
		fetchState = &reqState.FetchState

		if reqState.FetchSize.BodySize > 0 {
			psw.WriteGauge("fetchsize.body-size", int(reqState.FetchSize.BodySize))
			psw.WriteGauge("fetchsize.buffer-length", int(reqState.FetchSize.BytesLength))
			psw.WriteGauge("fetchsize.buffer-capacity", int(reqState.FetchSize.BytesCap))
		}

		storageMetadata = &reqState.StorageMetadata
		isResponseWriteError = &reqState.IsResponseWriteError
		isCondError = &reqState.IsCondError

		psw.WriteTimer("timers.parse", reqState.Duration.Parse)
		psw.WriteTimer("timers.storage-fetch", reqState.Duration.StorageFetch)
		psw.WriteTimer("timers.storage-read", reqState.Duration.StorageRead)
		psw.WriteTimer("timers.metatile-find", reqState.Duration.MetatileFind)
		psw.WriteTimer("timers.response-write", reqState.Duration.RespWrite)
		psw.WriteTimer("timers.total", reqState.Duration.Total)

		if format := reqState.Format; format != "" {
			psw.WriteCount(fmt.Sprintf("formats.%s", format), 1)
		}
		if responseSize := reqState.ResponseSize; responseSize > 0 {
			psw.WriteGauge("response-size", responseSize)
		}
	} else if reqStateContainer.tileJsonReqState != nil {
		tileJsonReqState := reqStateContainer.tileJsonReqState

		psw.WriteCount("tilejson", 1)

		respState = &tileJsonReqState.ResponseState
		fetchState = &tileJsonReqState.FetchState
		isResponseWriteError = &tileJsonReqState.IsResponseWriteError
		isCondError = &tileJsonReqState.IsCondError

		psw.WriteTimer("timers.parse", tileJsonReqState.Duration.Parse)
		psw.WriteTimer("timers.storage-fetch", tileJsonReqState.Duration.StorageFetch)
		// count the response writing and storage reading together as storage read
		psw.WriteTimer("timers.storage-read", tileJsonReqState.Duration.StorageReadRespWrite)

		if tileJsonReqState.Format != nil {
			formatMetricName := fmt.Sprintf("tilejson.formats.%s", tileJsonReqState.Format.Name())
			psw.WriteCount(formatMetricName, 1)
		}

		psw.WriteGauge("fetchsize.body-size", int(tileJsonReqState.FetchSize))
		psw.WriteGauge("response-size", int(tileJsonReqState.FetchSize))

	} else {
		smw.logger.Warning(log.LogCategory_InvalidCodeState, "Metric processing: no state")
	}

	if respState != nil {
		if *respState > state.ResponseState_Nil && *respState < state.ResponseState_Count {
			respStateName := respState.String()
			respMetricName := fmt.Sprintf("responsestate.%s", respStateName)
			psw.WriteCount(respMetricName, 1)
		} else {
			smw.logger.Error(log.LogCategory_InvalidCodeState, "Invalid response state: %d", int32(*respState))
		}
	}
	if fetchState != nil {
		if *fetchState > state.FetchState_Nil && *fetchState < state.FetchState_Count {
			fetchStateName := fetchState.String()
			fetchMetricName := fmt.Sprintf("fetchstate.%s", fetchStateName)
			psw.WriteCount(fetchMetricName, 1)
		} else if *fetchState != state.FetchState_Nil {
			smw.logger.Error(log.LogCategory_InvalidCodeState, "Invalid fetch state: %d", int32(*fetchState))
		}
	}
	if storageMetadata != nil {
		psw.WriteBool("counts.lastmodified", storageMetadata.HasLastModified)
		psw.WriteBool("counts.etag", storageMetadata.HasEtag)
	}

	if isResponseWriteError != nil {
		psw.WriteBool("errors.response-write-error", *isResponseWriteError)
	}
	if isCondError != nil {
		psw.WriteBool("errors.condition-parse-error", *isCondError)
	}

}

func (smw *StatsdMetricsWriter) enqueue(container requestStateContainer) {
	select {
	case smw.queue <- container:
	default:
		smw.logger.Warning(log.LogCategory_Metrics, "Metrics Writer queue full\n")
	}
}

func (smw *StatsdMetricsWriter) WriteMetatileState(reqState *state.RequestState) {
	smw.enqueue(requestStateContainer{metaReqState: reqState})
}

func (smw *StatsdMetricsWriter) WriteTileJsonState(tileJsonReqState *state.TileJsonRequestState) {
	smw.enqueue(requestStateContainer{tileJsonReqState: tileJsonReqState})
}

func NewStatsdMetricsWriter(addr *net.UDPAddr, metricsPrefix string, logger log.JsonLogger) MetricsWriter {
	maxQueueSize := 4096
	queue := make(chan requestStateContainer, maxQueueSize)

	smw := &StatsdMetricsWriter{
		addr:   addr,
		prefix: metricsPrefix,
		logger: logger,
		queue:  queue,
	}

	go func(smw *StatsdMetricsWriter) {
		for reqStateContainer := range smw.queue {
			smw.Process(reqStateContainer)
		}
	}(smw)

	return smw
}

func makeMetricPrefix(prefix string, metric string) string {
	if prefix == "" {
		return metric
	} else {
		return fmt.Sprintf("%s.%s", prefix, metric)
	}
}

func makeStatsdLineCount(prefix string, metric string, value int) string {
	return fmt.Sprintf("%s:%d|c\n", makeMetricPrefix(prefix, metric), value)
}

func makeStatsdLineGauge(prefix string, metric string, value int) string {
	return fmt.Sprintf("%s:%d|g\n", makeMetricPrefix(prefix, metric), value)
}

func makeStatsdLineTimer(prefix string, metric string, value time.Duration) string {
	millis := value.Milliseconds()
	return fmt.Sprintf("%s:%d|ms\n", makeMetricPrefix(prefix, metric), millis)
}

func writeStatsdCount(w io.Writer, prefix string, metric string, value int) {
	w.Write([]byte(makeStatsdLineCount(prefix, metric, value)))
}

func writeStatsdGauge(w io.Writer, prefix string, metric string, value int) {
	w.Write([]byte(makeStatsdLineGauge(prefix, metric, value)))
}

func writeStatsdTimer(w io.Writer, prefix string, metric string, value time.Duration) {
	w.Write([]byte(makeStatsdLineTimer(prefix, metric, value)))
}

type prefixedStatsdWriter struct {
	prefix string
	w      io.Writer
}

func (psw *prefixedStatsdWriter) WriteCount(metric string, value int) {
	writeStatsdCount(psw.w, psw.prefix, metric, value)
}

func (psw *prefixedStatsdWriter) WriteGauge(metric string, value int) {
	writeStatsdGauge(psw.w, psw.prefix, metric, value)
}

func (psw *prefixedStatsdWriter) WriteBool(metric string, value bool) {
	if value {
		psw.WriteCount(metric, 1)
	}
}

func (psw *prefixedStatsdWriter) WriteTimer(metric string, value time.Duration) {
	writeStatsdTimer(psw.w, psw.prefix, metric, value)
}
