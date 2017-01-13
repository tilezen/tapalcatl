package main

import (
	"bytes"
	"fmt"
	"github.com/pubnub/go-metrics-statsd"
	"github.com/rcrowley/go-metrics"
	"github.com/tilezen/tapalcatl"
	"io"
	"log"
	"net"
	"net/http"
	"time"
)

type ParseResult struct {
	Coord       tapalcatl.TileCoord
	Cond        Condition
	ContentType string
}

type Parser interface {
	Parse(*http.Request) (*ParseResult, error)
}

type BufferManager interface {
	Get() *bytes.Buffer
	Put(*bytes.Buffer)
}

// the reqState structures and string generation serve to emit a single log entry line
// a log parser will pick this up and use it to persist metrics
// the string functions here are specific to the format used and should be updated with care

type reqResponseState int32

const (
	ResponseState_Nil reqResponseState = iota
	ResponseState_Success
	ResponseState_NotModified
	ResponseState_NotFound
	ResponseState_BadRequest
	ResponseState_Error
	ResponseState_Count
)

func (rrs reqResponseState) String() string {
	switch rrs {
	case ResponseState_Nil:
		return "nil"
	case ResponseState_Success:
		return "ok"
	case ResponseState_NotModified:
		return "notmod"
	case ResponseState_NotFound:
		return "notfound"
	case ResponseState_BadRequest:
		return "badreq"
	case ResponseState_Error:
		return "err"
	default:
		return "unknown"
	}
}

type reqFetchState int32

const (
	FetchState_Nil reqFetchState = iota
	FetchState_Success
	FetchState_NotFound
	FetchState_FetchError
	FetchState_ReadError
	FetchState_Count
)

func (rfs reqFetchState) String() string {
	switch rfs {
	case FetchState_Nil:
		return "nil"
	case FetchState_Success:
		return "ok"
	case FetchState_NotFound:
		return "notfound"
	case FetchState_FetchError:
		return "fetcherr"
	case FetchState_ReadError:
		return "readerr"
	default:
		return "unknown"
	}
}

type reqFetchSize struct {
	bodySize, bytesLength, bytesCap int64
}

type reqStorageMetadata struct {
	hasLastModified, hasEtag bool
}

type requestState struct {
	responseState        reqResponseState
	fetchState           reqFetchState
	fetchSize            reqFetchSize
	storageMetadata      reqStorageMetadata
	isZipError           bool
	isResponseWriteError bool
	isCondError          bool
}

func logBool(x bool) string {
	if x {
		return "1"
	} else {
		return "0"
	}
}

// create a log string
func (reqState *requestState) String() string {

	var fetchSize string
	if reqState.fetchSize.bodySize > 0 {
		fetchSize = fmt.Sprintf(
			"%d %d %d",
			reqState.fetchSize.bodySize,
			reqState.fetchSize.bytesLength,
			reqState.fetchSize.bytesCap,
		)
	} else {
		fetchSize = "nil"
	}

	hasLastMod := logBool(reqState.storageMetadata.hasLastModified)
	hasEtag := logBool(reqState.storageMetadata.hasEtag)

	isZipErr := logBool(reqState.isZipError)
	isRespErr := logBool(reqState.isResponseWriteError)
	isCondErr := logBool(reqState.isCondError)

	result := fmt.Sprintf(
		"METRICS: respstate(%s) fetchstate(%s) fetchsize(%s) ziperr(%s) resperr(%s) conderr(%s) lastmod(%s) etag(%s)",
		reqState.responseState,
		reqState.fetchState,
		fetchSize,
		isZipErr,
		isRespErr,
		isCondErr,
		hasLastMod,
		hasEtag,
	)

	return result
}

type metricsWriter interface {
	Write(*requestState)
}

type nilMetricsWriter struct{}

func (_ *nilMetricsWriter) Write(reqState *requestState) {}

type statsdMetricsWriter struct {
	respState  [ResponseState_Count]metrics.Counter
	fetchState [FetchState_Count]metrics.Counter

	fetchSizeBody metrics.Gauge
	fetchSizeLen  metrics.Gauge
	fetchSizeCap  metrics.Gauge

	lastMod metrics.Counter
	etag    metrics.Counter

	zipErr       metrics.Counter
	respWriteErr metrics.Counter
	condErr      metrics.Counter
}

func (smw *statsdMetricsWriter) Write(reqState *requestState) {
	respStateInt := int32(reqState.responseState)
	if respStateInt > 0 && respStateInt < int32(ResponseState_Count) {
		counter := smw.respState[respStateInt]
		counter.Inc(1)
	}

	fetchStateInt := int32(reqState.fetchState)
	if fetchStateInt > 0 && fetchStateInt < int32(FetchState_Count) {
		counter := smw.fetchState[fetchStateInt]
		counter.Inc(1)
	}

	if reqState.fetchSize.bodySize > 0 {
		smw.fetchSizeBody.Update(reqState.fetchSize.bodySize)
		smw.fetchSizeLen.Update(reqState.fetchSize.bytesLength)
		smw.fetchSizeCap.Update(reqState.fetchSize.bytesCap)
	}

	if reqState.storageMetadata.hasLastModified {
		smw.lastMod.Inc(1)
	}
	if reqState.storageMetadata.hasEtag {
		smw.etag.Inc(1)
	}

	if reqState.isZipError {
		smw.zipErr.Inc(1)
	}
	if reqState.isResponseWriteError {
		smw.respWriteErr.Inc(1)
	}
	if reqState.isCondError {
		smw.condErr.Inc(1)
	}
}

func NewStatsdMetricsWriter(addr *net.UDPAddr, metricsPrefix string, duration time.Duration) metricsWriter {

	result := &statsdMetricsWriter{}

	registry := metrics.DefaultRegistry

	// set up response state counters
	for i := 1; i < int(ResponseState_Count); i += 1 {
		rrs := reqResponseState(i)
		stateName := rrs.String()
		metricName := fmt.Sprintf("responsestate.%s", stateName)
		result.respState[i] = metrics.NewRegisteredCounter(metricName, registry)
	}

	// set up fetch state counters
	for i := 1; i < int(FetchState_Count); i += 1 {
		rfs := reqFetchState(i)
		stateName := rfs.String()
		metricName := fmt.Sprintf("fetchstate.%s", stateName)
		result.fetchState[i] = metrics.NewRegisteredCounter(metricName, registry)
	}

	// fetch sizes
	result.fetchSizeBody = metrics.NewRegisteredGauge("fetchsize.body-size", registry)
	result.fetchSizeLen = metrics.NewRegisteredGauge("fetchsize.buffer-length", registry)
	result.fetchSizeCap = metrics.NewRegisteredGauge("fetchsize.buffer-capacity", registry)

	// storage response metadata counters
	result.lastMod = metrics.NewRegisteredCounter("lastmodified", registry)
	result.etag = metrics.NewRegisteredCounter("etag", registry)

	// error counters
	result.zipErr = metrics.NewRegisteredCounter("zip-error", registry)
	result.respWriteErr = metrics.NewRegisteredCounter("response-write-error", registry)
	result.condErr = metrics.NewRegisteredCounter("condition-parse-error", registry)

	// this goroutine ticks at the specified duration and sends out the data
	go statsd.StatsD(registry, duration, metricsPrefix, addr)

	return result
}

func MetatileHandler(p Parser, metatileSize int, mimeMap map[string]string, storage Storage, bufferManager BufferManager, mw metricsWriter, logger *log.Logger) http.Handler {

	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {

		reqState := requestState{}

		numRequests.Add(1)
		start_time := time.Now()
		defer func() {
			// update expvar state
			updateCounters(time.Since(start_time))

			// relies on the Stringer implementation to format the record correctly
			logger.Printf("INFO: %s", &reqState)

			// write out metrics
			mw.Write(&reqState)

		}()

		parseResult, err := p.Parse(req)
		if err != nil {
			requestParseErrors.Add(1)
			var sc int
			var response string

			if pe, ok := err.(*ParseError); ok {
				logger.Printf("WARNING: Parse failure: %s", err.Error())
				if pe.MimeError != nil {
					sc = http.StatusNotFound
					reqState.responseState = ResponseState_NotFound
					response = pe.MimeError.Error()
				} else if pe.CoordError != nil {
					sc = http.StatusBadRequest
					reqState.responseState = ResponseState_BadRequest
					response = pe.CoordError.Error()
				} else if pe.CondError != nil {
					reqState.isCondError = true
					logger.Printf("WARNING: Condition Error: %s", pe.CondError)
				}
			} else {
				logger.Printf("ERROR: Unknown parse error: %#v\n", err)
				sc = http.StatusInternalServerError
				response = "Internal server error"
				reqState.responseState = ResponseState_Error
			}

			// only return an error response when not a condition parse error
			// NOTE: maybe it's better to not consider this an error, but
			// capture it in the parse result state and handle it that way?
			if sc > 0 {
				http.Error(rw, response, sc)
				return
			}
		}

		metaCoord, offset := parseResult.Coord.MetaAndOffset(metatileSize)

		storageResult, err := storage.Fetch(metaCoord, parseResult.Cond)
		if err != nil || storageResult.NotFound {
			if err != nil {
				storageFetchErrors.Add(1)
				logger.Printf("WARNING: Metatile storage fetch failure: %s", err.Error())
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				reqState.fetchState = FetchState_FetchError
				reqState.responseState = ResponseState_Error
			} else {
				numStorageMisses.Add(1)
				http.NotFound(rw, req)
				reqState.fetchState = FetchState_NotFound
				reqState.responseState = ResponseState_NotFound
			}
			return
		}
		numStorageHits.Add(1)
		reqState.fetchState = FetchState_Success

		if storageResult.NotModified {
			numStorageNotModified.Add(1)
			rw.WriteHeader(http.StatusNotModified)
			reqState.responseState = ResponseState_NotModified
			return
		}
		numStorageReads.Add(1)

		// metatile reader needs to be able to seek in the buffer and know
		// its size. the easiest way to ensure that is to buffer the whole
		// thing into memory.
		storageResp := storageResult.Response

		buf := bufferManager.Get()
		defer bufferManager.Put(buf)

		bodySize, err := io.Copy(buf, storageResp.Body)
		if err != nil {
			storageReadErrors.Add(1)
			logger.Printf("ERROR: Failed to read storage body: %s", err.Error())
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			reqState.fetchState = FetchState_ReadError
			reqState.responseState = ResponseState_Error
			return
		}
		reqState.fetchState = FetchState_Success

		storageBytes := buf.Bytes()
		reqState.fetchSize.bodySize = bodySize
		reqState.fetchSize.bytesLength = int64(len(storageBytes))
		reqState.fetchSize.bytesCap = int64(cap(storageBytes))

		headers := rw.Header()
		headers.Set("Content-Type", parseResult.ContentType)
		if lastMod := storageResp.LastModified; lastMod != nil {
			lastModifiedFormatted := lastMod.Format(time.RFC1123Z)
			headers.Set("Last-Modified", lastModifiedFormatted)
			reqState.storageMetadata.hasLastModified = true
		}
		if etag := storageResp.ETag; etag != nil {
			headers.Set("ETag", *etag)
			reqState.storageMetadata.hasEtag = true
		}

		reader, err := tapalcatl.NewMetatileReader(offset, bytes.NewReader(storageBytes), bodySize)
		if err != nil {
			metatileReadErrors.Add(1)
			logger.Printf("ERROR: Failed to read metatile: %s", err.Error())
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			reqState.isZipError = true
			reqState.responseState = ResponseState_Error
			return
		}

		rw.WriteHeader(http.StatusOK)
		reqState.responseState = ResponseState_Success
		_, err = io.Copy(rw, reader)
		if err != nil {
			responseWriteErrors.Add(1)
			logger.Printf("ERROR: Failed to write response body: %s", err.Error())
			reqState.isResponseWriteError = true
		}
	})
}
