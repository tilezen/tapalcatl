package main

import (
	"bufio"
	"bytes"
	"fmt"
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
	addr   *net.UDPAddr
	prefix string
	logger *log.Logger
	queue  chan *requestState
}

func makeMetricPrefix(prefix string, metric string) string {
	if prefix == "" {
		return metric
	} else {
		return fmt.Sprintf("%s.%s", prefix, metric)
	}
}

func makeStatsdLineCount(prefix string, metric string, value int) string {
	return fmt.Sprintf("%s.count:%d|c\n", makeMetricPrefix(prefix, metric), value)
}

func makeStatsdLineGauge(prefix string, metric string, value int) string {
	return fmt.Sprintf("%s.value:%d|g\n", makeMetricPrefix(prefix, metric), value)
}

func writeStatsdCount(w io.Writer, prefix string, metric string, value int) {
	w.Write([]byte(makeStatsdLineCount(prefix, metric, value)))
}

func writeStatsdGauge(w io.Writer, prefix string, metric string, value int) {
	w.Write([]byte(makeStatsdLineGauge(prefix, metric, value)))
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

func (smw *statsdMetricsWriter) Process(reqState *requestState) {
	conn, err := net.DialUDP("udp", nil, smw.addr)
	if err != nil {
		smw.logger.Printf("ERROR: Metrics Writer failed to connect to %s: %s\n", smw.addr, err)
		return
	}
	defer conn.Close()

	w := bufio.NewWriter(conn)
	defer w.Flush()

	psw := prefixedStatsdWriter{
		prefix: smw.prefix,
		w:      w,
	}

	respStateInt := int32(reqState.responseState)
	if respStateInt > 0 && respStateInt < int32(ResponseState_Count) {
		respStateName := reqState.responseState.String()
		respMetricName := fmt.Sprintf("responsestate.%s", respStateName)
		psw.WriteCount(respMetricName, 1)
	} else {
		smw.logger.Printf("ERROR: Invalid response state: %s", reqState.responseState)
	}

	fetchStateInt := int32(reqState.fetchState)
	if fetchStateInt > 0 && fetchStateInt < int32(FetchState_Count) {
		fetchStateName := reqState.fetchState.String()
		fetchMetricName := fmt.Sprintf("fetchstate.%s", fetchStateName)
		psw.WriteCount(fetchMetricName, 1)
	} else {
		smw.logger.Printf("ERROR: Invalid fetch state: %s", reqState.responseState)
	}

	if reqState.fetchSize.bodySize > 0 {
		psw.WriteGauge("fetchsize.body-size", int(reqState.fetchSize.bodySize))
		psw.WriteGauge("fetchsize.buffer-length", int(reqState.fetchSize.bytesLength))
		psw.WriteGauge("fetchsize.buffer-capacity", int(reqState.fetchSize.bytesCap))
	}

	psw.WriteCount("counts.all", 1)
	psw.WriteBool("counts.lastmodified", reqState.storageMetadata.hasLastModified)
	psw.WriteBool("counts.etag", reqState.storageMetadata.hasEtag)

	psw.WriteCount("errors.all", 1)
	psw.WriteBool("errors.response-write-error", reqState.isResponseWriteError)
	psw.WriteBool("errors.condition-parse-error", reqState.isCondError)
}

func (smw *statsdMetricsWriter) Write(reqState *requestState) {
	select {
	case smw.queue <- reqState:
	default:
		smw.logger.Printf("WARNING: Metrics Writer queue full\n")
	}
}

func NewStatsdMetricsWriter(addr *net.UDPAddr, metricsPrefix string, logger *log.Logger) metricsWriter {
	maxQueueSize := 4096
	queue := make(chan *requestState, maxQueueSize)

	smw := &statsdMetricsWriter{
		addr:   addr,
		prefix: metricsPrefix,
		logger: logger,
		queue:  queue,
	}

	go func(smw *statsdMetricsWriter) {
		for reqState := range smw.queue {
			smw.Process(reqState)
		}
	}(smw)

	return smw
}

func MetatileHandler(p Parser, metatileSize int, mimeMap map[string]string, storage Storage, bufferManager BufferManager, mw metricsWriter, logger *log.Logger) http.Handler {

	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {

		reqState := requestState{}

		numRequests.Add(1)
		start_time := time.Now()
		defer func() {
			// update expvar state
			updateCounters(time.Since(start_time))

			if reqState.responseState == ResponseState_Nil {
				logger.Printf("ERROR: Code error: handler did not set response state")
			}

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
