package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/tilezen/tapalcatl"
	"io"
	"net"
	"net/http"
	"time"
)

type HttpRequestData struct {
	Path      string
	ApiKey    string
	UserAgent string
	Referrer  string
	Format    string
}

type ParseResult struct {
	Coord       tapalcatl.TileCoord
	Cond        Condition
	ContentType string
	HttpData    HttpRequestData
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

type ReqResponseState int32

const (
	ResponseState_Nil ReqResponseState = iota
	ResponseState_Success
	ResponseState_NotModified
	ResponseState_NotFound
	ResponseState_BadRequest
	ResponseState_Error
	ResponseState_Count
)

func (rrs ReqResponseState) String() string {
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

func (rrs ReqResponseState) AsStatusCode() int {
	switch rrs {
	case ResponseState_Nil:
		return 0
	case ResponseState_Success:
		return 200
	case ResponseState_NotModified:
		return 304
	case ResponseState_NotFound:
		return 404
	case ResponseState_BadRequest:
		return 400
	case ResponseState_Error:
		return 500
	default:
		return -1
	}
}

type ReqFetchState int32

const (
	FetchState_Nil ReqFetchState = iota
	FetchState_Success
	FetchState_NotFound
	FetchState_FetchError
	FetchState_ReadError
	FetchState_ConfigError
	FetchState_Count
)

func (rfs ReqFetchState) String() string {
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
	case FetchState_ConfigError:
		return "configerr"
	default:
		return "unknown"
	}
}

type ReqFetchSize struct {
	BodySize    int64
	BytesLength int64
	BytesCap    int64
}

type ReqStorageMetadata struct {
	HasLastModified bool
	HasEtag         bool
}

type ReqDuration struct {
	Parse, StorageFetch, StorageRead, MetatileFind, RespWrite, Total time.Duration
}

// durations will be logged in milliseconds
type JsonReqDuration struct {
	Parse        int64
	StorageFetch int64
	StorageRead  int64
	MetatileFind int64
	RespWrite    int64
	Total        int64
}

type RequestState struct {
	ResponseState        ReqResponseState
	FetchState           ReqFetchState
	FetchSize            ReqFetchSize
	StorageMetadata      ReqStorageMetadata
	IsZipError           bool
	IsResponseWriteError bool
	IsCondError          bool
	Duration             ReqDuration
	Coord                *tapalcatl.TileCoord
	HttpData             *HttpRequestData
	ResponseSize         int
}

func convertDurationToMillis(x time.Duration) int64 {
	return int64(x / time.Millisecond)
}

func (reqState *RequestState) AsJsonMap() map[string]interface{} {

	result := make(map[string]interface{})

	result["response_state"] = reqState.ResponseState.AsStatusCode()
	result["fetch_state"] = reqState.FetchState.String()

	reqStateErrs := make(map[string]bool)
	if reqState.IsZipError {
		reqStateErrs["zip"] = true
	}
	if reqState.IsResponseWriteError {
		reqStateErrs["response_write"] = true
	}
	if reqState.IsCondError {
		reqStateErrs["cond"] = true
	}
	if len(reqStateErrs) > 0 {
		result["error"] = reqStateErrs
	}

	if reqState.FetchSize.BodySize > 0 {
		result["fetch_size"] = map[string]int64{
			"body_size":    reqState.FetchSize.BodySize,
			"bytes_length": reqState.FetchSize.BytesLength,
			"bytes_cap":    reqState.FetchSize.BytesCap,
		}
	}

	result["storageMetadata"] = map[string]bool{
		"has_last_modified": reqState.StorageMetadata.HasLastModified,
		"has_etag":          reqState.StorageMetadata.HasEtag,
	}

	result["duration"] = map[string]int64{
		"parse":         convertDurationToMillis(reqState.Duration.Parse),
		"storage_fetch": convertDurationToMillis(reqState.Duration.StorageFetch),
		"storage_read":  convertDurationToMillis(reqState.Duration.StorageRead),
		"metatile_find": convertDurationToMillis(reqState.Duration.MetatileFind),
		"resp_write":    convertDurationToMillis(reqState.Duration.RespWrite),
		"total":         convertDurationToMillis(reqState.Duration.Total),
	}

	if reqState.Coord != nil {
		result["coord"] = map[string]int{
			"x": reqState.Coord.X,
			"y": reqState.Coord.Y,
			"z": reqState.Coord.Z,
		}
	}

	if reqState.HttpData != nil {
		httpJsonData := make(map[string]interface{})
		httpJsonData["path"] = reqState.HttpData.Path
		if userAgent := reqState.HttpData.UserAgent; userAgent != "" {
			httpJsonData["user_agent"] = userAgent
		}
		if referrer := reqState.HttpData.Referrer; referrer != "" {
			httpJsonData["referer"] = referrer
		}
		if apiKey := reqState.HttpData.ApiKey; apiKey != "" {
			httpJsonData["api_key"] = apiKey
		}
		if format := reqState.HttpData.Format; format != "" {
			httpJsonData["format"] = format
		}
		if responseSize := reqState.ResponseSize; responseSize > 0 {
			httpJsonData["response_size"] = responseSize
		}
		result["http"] = httpJsonData
	}

	return result
}

type metricsWriter interface {
	Write(*RequestState)
}

type nilMetricsWriter struct{}

func (_ *nilMetricsWriter) Write(reqState *RequestState) {}

type statsdMetricsWriter struct {
	addr   *net.UDPAddr
	prefix string
	logger JsonLogger
	queue  chan *RequestState
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
	millis := convertDurationToMillis(value)
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

func (smw *statsdMetricsWriter) Process(reqState *RequestState) {
	conn, err := net.DialUDP("udp", nil, smw.addr)
	if err != nil {
		smw.logger.Error(LogCategory_Metrics, "Metrics Writer failed to connect to %s: %s\n", smw.addr, err)
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

	if reqState.ResponseState > ResponseState_Nil && reqState.ResponseState < ResponseState_Count {
		respStateName := reqState.ResponseState.String()
		respMetricName := fmt.Sprintf("responsestate.%s", respStateName)
		psw.WriteCount(respMetricName, 1)
	} else {
		smw.logger.Error(LogCategory_InvalidCodeState, "Invalid response state: %d", int32(reqState.ResponseState))
	}

	if reqState.FetchState > FetchState_Nil && reqState.FetchState < FetchState_Count {
		fetchStateName := reqState.FetchState.String()
		fetchMetricName := fmt.Sprintf("fetchstate.%s", fetchStateName)
		psw.WriteCount(fetchMetricName, 1)
	} else if reqState.FetchState != FetchState_Nil {
		smw.logger.Error(LogCategory_InvalidCodeState, "Invalid fetch state: %d", int32(reqState.FetchState))
	}

	if reqState.FetchSize.BodySize > 0 {
		psw.WriteGauge("fetchsize.body-size", int(reqState.FetchSize.BodySize))
		psw.WriteGauge("fetchsize.buffer-length", int(reqState.FetchSize.BytesLength))
		psw.WriteGauge("fetchsize.buffer-capacity", int(reqState.FetchSize.BytesCap))
	}

	psw.WriteBool("counts.lastmodified", reqState.StorageMetadata.HasLastModified)
	psw.WriteBool("counts.etag", reqState.StorageMetadata.HasEtag)

	psw.WriteBool("errors.response-write-error", reqState.IsResponseWriteError)
	psw.WriteBool("errors.condition-parse-error", reqState.IsCondError)

	psw.WriteTimer("timers.parse", reqState.Duration.Parse)
	psw.WriteTimer("timers.storage-fetch", reqState.Duration.StorageFetch)
	psw.WriteTimer("timers.storage-read", reqState.Duration.StorageRead)
	psw.WriteTimer("timers.metatile-find", reqState.Duration.MetatileFind)
	psw.WriteTimer("timers.response-write", reqState.Duration.RespWrite)
	psw.WriteTimer("timers.total", reqState.Duration.Total)

	if format := reqState.HttpData.Format; format != "" {
		psw.WriteCount(fmt.Sprintf("formats.%s", format), 1)
	}
	if responseSize := reqState.ResponseSize; responseSize > 0 {
		psw.WriteGauge("response-size", responseSize)
	}
}

func (smw *statsdMetricsWriter) Write(reqState *RequestState) {
	select {
	case smw.queue <- reqState:
	default:
		smw.logger.Warning(LogCategory_Metrics, "Metrics Writer queue full\n")
	}
}

func NewStatsdMetricsWriter(addr *net.UDPAddr, metricsPrefix string, logger JsonLogger) metricsWriter {
	maxQueueSize := 4096
	queue := make(chan *RequestState, maxQueueSize)

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

func MetatileHandler(p Parser, metatileSize, tileSize int, mimeMap map[string]string, storage Storage, bufferManager BufferManager, mw metricsWriter, logger JsonLogger) http.Handler {

	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {

		reqState := RequestState{}

		numRequests.Add(1)

		startTime := time.Now()

		defer func() {
			totalDuration := time.Since(startTime)
			reqState.Duration.Total = totalDuration

			// update expvar state
			updateCounters(totalDuration)

			if reqState.ResponseState == ResponseState_Nil {
				logger.Error(LogCategory_InvalidCodeState, "handler did not set response state")
			}

			jsonReqData := reqState.AsJsonMap()
			logger.Metrics(jsonReqData)

			// write out metrics
			mw.Write(&reqState)

		}()

		parseStart := time.Now()
		parseResult, err := p.Parse(req)
		reqState.Duration.Parse = time.Since(parseStart)
		if err != nil {
			requestParseErrors.Add(1)
			var sc int
			var response string

			if pe, ok := err.(*ParseError); ok {
				logger.Warning(LogCategory_ParseError, err.Error())
				if pe.MimeError != nil {
					sc = http.StatusNotFound
					reqState.ResponseState = ResponseState_NotFound
					response = pe.MimeError.Error()
				} else if pe.CoordError != nil {
					sc = http.StatusBadRequest
					reqState.ResponseState = ResponseState_BadRequest
					response = pe.CoordError.Error()
				} else if pe.CondError != nil {
					reqState.IsCondError = true
					logger.Warning(LogCategory_ConditionError, pe.CondError.Error())
				}
			} else {
				logger.Error(LogCategory_ParseError, "Unknown parse error: %#v\n", err)
				sc = http.StatusInternalServerError
				response = "Internal server error"
				reqState.ResponseState = ResponseState_Error
			}

			// only return an error response when not a condition parse error
			// NOTE: maybe it's better to not consider this an error, but
			// capture it in the parse result state and handle it that way?
			if sc > 0 {
				http.Error(rw, response, sc)
				return
			}
		}

		reqState.Coord = &parseResult.Coord
		reqState.HttpData = &parseResult.HttpData

		metaCoord, offset, err := parseResult.Coord.MetaAndOffset(metatileSize, tileSize)
		if err != nil {
			configErrors.Add(1)
			logger.Warning(LogCategory_ConfigError, "MetaAndOffset could not be calculated: %s", err.Error())
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			reqState.ResponseState = ResponseState_Error
			// Note: FetchState is left as nil, since no fetch was performed
			return
		}

		storageFetchStart := time.Now()
		storageResult, err := storage.Fetch(metaCoord, parseResult.Cond)
		reqState.Duration.StorageFetch = time.Since(storageFetchStart)

		if err != nil || storageResult.NotFound {
			if err != nil {
				storageFetchErrors.Add(1)
				logger.Warning(LogCategory_StorageError, "Metatile storage fetch failure: %#v", err)
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				reqState.FetchState = FetchState_FetchError
				reqState.ResponseState = ResponseState_Error
			} else {
				numStorageMisses.Add(1)
				http.NotFound(rw, req)
				reqState.FetchState = FetchState_NotFound
				reqState.ResponseState = ResponseState_NotFound
			}
			return
		}
		numStorageHits.Add(1)
		reqState.FetchState = FetchState_Success

		if storageResult.NotModified {
			numStorageNotModified.Add(1)
			rw.WriteHeader(http.StatusNotModified)
			reqState.ResponseState = ResponseState_NotModified
			return
		}
		numStorageReads.Add(1)

		// metatile reader needs to be able to seek in the buffer and know
		// its size. the easiest way to ensure that is to buffer the whole
		// thing into memory.
		storageResp := storageResult.Response

		buf := bufferManager.Get()
		defer bufferManager.Put(buf)

		storageReadStart := time.Now()
		bodySize, err := io.Copy(buf, storageResp.Body)
		reqState.Duration.StorageRead = time.Since(storageReadStart)
		if err != nil {
			storageReadErrors.Add(1)
			logger.Error(LogCategory_StorageError, "Failed to read storage body: %#v", err)
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			reqState.FetchState = FetchState_ReadError
			reqState.ResponseState = ResponseState_Error
			return
		}
		reqState.FetchState = FetchState_Success

		storageBytes := buf.Bytes()
		reqState.FetchSize.BodySize = bodySize
		reqState.FetchSize.BytesLength = int64(len(storageBytes))
		reqState.FetchSize.BytesCap = int64(cap(storageBytes))

		headers := rw.Header()
		headers.Set("Content-Type", parseResult.ContentType)
		if lastMod := storageResp.LastModified; lastMod != nil {
			// important! we must format times in an HTTP-compliant way, which
			// apparently doesn't match any existing Go time format string, so the
			// recommended way is to switch to UTC and use the format string that
			// the net/http package exposes.
			lastModifiedFormatted := lastMod.UTC().Format(http.TimeFormat)
			headers.Set("Last-Modified", lastModifiedFormatted)
			reqState.StorageMetadata.HasLastModified = true
		}
		if etag := storageResp.ETag; etag != nil {
			headers.Set("ETag", *etag)
			reqState.StorageMetadata.HasEtag = true
		}

		metatileReaderFindStart := time.Now()
		reader, formatSize, err := tapalcatl.NewMetatileReader(offset, bytes.NewReader(storageBytes), bodySize)
		reqState.Duration.MetatileFind = time.Since(metatileReaderFindStart)
		if err != nil {
			metatileReadErrors.Add(1)
			logger.Error(LogCategory_MetatileError, "Failed to read metatile: %#v", err)
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			reqState.IsZipError = true
			reqState.ResponseState = ResponseState_Error
			return
		}
		reqState.ResponseSize = int(formatSize)

		rw.WriteHeader(http.StatusOK)
		reqState.ResponseState = ResponseState_Success
		respWriteStart := time.Now()
		_, err = io.Copy(rw, reader)
		reqState.Duration.RespWrite = time.Since(respWriteStart)
		if err != nil {
			responseWriteErrors.Add(1)
			logger.Error(LogCategory_ResponseError, "Failed to write response body: %#v", err)
			reqState.IsResponseWriteError = true
		}
	})
}
