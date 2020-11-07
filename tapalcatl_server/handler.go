package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/tilezen/tapalcatl"
)

type HttpRequestData struct {
	Path      string
	ApiKey    string
	UserAgent string
	Referrer  string
}

type ParseResultType int

const (
	ParseResultType_Nil ParseResultType = iota
	ParseResultType_Metatile
	ParseResultType_Tilejson
)

type ParseResult struct {
	Type        ParseResultType
	Cond        Condition
	ContentType string
	HttpData    HttpRequestData
	// set to be more specific data based on parse type
	AdditionalData interface{}
}

type MetatileParseData struct {
	Coord tapalcatl.TileCoord
}

type TileJsonParseData struct {
	Format TileJsonFormat
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
	HttpData             HttpRequestData
	Format               string
	ResponseSize         int
}

func convertDurationToMillis(x time.Duration) int64 {
	return int64(x / time.Millisecond)
}

func (reqState *RequestState) AsJsonMap() map[string]interface{} {

	result := make(map[string]interface{})

	if reqState.FetchState > FetchState_Nil {
		fetchResult := make(map[string]interface{})

		fetchResult["state"] = reqState.FetchState.String()

		if reqState.FetchSize.BodySize > 0 {
			fetchResult["size"] = map[string]int64{
				"body":      reqState.FetchSize.BodySize,
				"bytes_len": reqState.FetchSize.BytesLength,
				"bytes_cap": reqState.FetchSize.BytesCap,
			}
		}

		fetchResult["metadata"] = map[string]bool{
			"has_last_modified": reqState.StorageMetadata.HasLastModified,
			"has_etag":          reqState.StorageMetadata.HasEtag,
		}

		result["fetch"] = fetchResult
	}

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

	result["timing"] = map[string]int64{
		"parse":         convertDurationToMillis(reqState.Duration.Parse),
		"storage_fetch": convertDurationToMillis(reqState.Duration.StorageFetch),
		"storage_read":  convertDurationToMillis(reqState.Duration.StorageRead),
		"metatile_find": convertDurationToMillis(reqState.Duration.MetatileFind),
		"resp_write":    convertDurationToMillis(reqState.Duration.RespWrite),
		"total":         convertDurationToMillis(reqState.Duration.Total),
	}

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
	if format := reqState.Format; format != "" {
		httpJsonData["format"] = format
	}
	if reqState.Coord != nil {
		result["coord"] = map[string]int{
			"x": reqState.Coord.X,
			"y": reqState.Coord.Y,
			"z": reqState.Coord.Z,
		}
		httpJsonData["format"] = reqState.Coord.Format
	}

	if responseSize := reqState.ResponseSize; responseSize > 0 {
		httpJsonData["response_size"] = responseSize
	}
	httpJsonData["status"] = reqState.ResponseState.AsStatusCode()
	result["http"] = httpJsonData

	return result
}

type metricsWriter interface {
	WriteMetatileState(*RequestState)
	WriteTileJsonState(*TileJsonRequestState)
}

type nilMetricsWriter struct{}

func (_ *nilMetricsWriter) WriteMetatileState(reqState *RequestState)             {}
func (_ *nilMetricsWriter) WriteTileJsonState(jsonReqState *TileJsonRequestState) {}

type statsdMetricsWriter struct {
	addr   *net.UDPAddr
	prefix string
	logger JsonLogger
	queue  chan RequestStateContainer
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

func (smw *statsdMetricsWriter) Process(reqStateContainer RequestStateContainer) {
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

	// variables to handle writing of common elements
	var respState *ReqResponseState
	var fetchState *ReqFetchState
	var storageMetadata *ReqStorageMetadata
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
		smw.logger.Warning(LogCategory_InvalidCodeState, "Metric processing: no state")
	}

	if respState != nil {
		if *respState > ResponseState_Nil && *respState < ResponseState_Count {
			respStateName := respState.String()
			respMetricName := fmt.Sprintf("responsestate.%s", respStateName)
			psw.WriteCount(respMetricName, 1)
		} else {
			smw.logger.Error(LogCategory_InvalidCodeState, "Invalid response state: %d", int32(*respState))
		}
	}
	if fetchState != nil {
		if *fetchState > FetchState_Nil && *fetchState < FetchState_Count {
			fetchStateName := fetchState.String()
			fetchMetricName := fmt.Sprintf("fetchstate.%s", fetchStateName)
			psw.WriteCount(fetchMetricName, 1)
		} else if *fetchState != FetchState_Nil {
			smw.logger.Error(LogCategory_InvalidCodeState, "Invalid fetch state: %d", int32(*fetchState))
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

type RequestStateContainer struct {
	// one of these will be set
	metaReqState     *RequestState
	tileJsonReqState *TileJsonRequestState
}

func (smw *statsdMetricsWriter) enqueue(container RequestStateContainer) {
	select {
	case smw.queue <- container:
	default:
		smw.logger.Warning(LogCategory_Metrics, "Metrics Writer queue full\n")
	}
}

func (smw *statsdMetricsWriter) WriteMetatileState(reqState *RequestState) {
	smw.enqueue(RequestStateContainer{metaReqState: reqState})
}

func (smw *statsdMetricsWriter) WriteTileJsonState(tileJsonReqState *TileJsonRequestState) {
	smw.enqueue(RequestStateContainer{tileJsonReqState: tileJsonReqState})
}

func NewStatsdMetricsWriter(addr *net.UDPAddr, metricsPrefix string, logger JsonLogger) metricsWriter {
	maxQueueSize := 4096
	queue := make(chan RequestStateContainer, maxQueueSize)

	smw := &statsdMetricsWriter{
		addr:   addr,
		prefix: metricsPrefix,
		logger: logger,
		queue:  queue,
	}

	go func(smw *statsdMetricsWriter) {
		for reqStateContainer := range smw.queue {
			smw.Process(reqStateContainer)
		}
	}(smw)

	return smw
}

type TileJsonDuration struct {
	Total, Parse, StorageFetch, StorageReadRespWrite time.Duration
}

type TileJsonRequestState struct {
	Duration             TileJsonDuration
	Format               *TileJsonFormat
	ResponseState        ReqResponseState
	FetchState           ReqFetchState
	FetchSize            uint64
	StorageMetadata      ReqStorageMetadata
	IsCondError          bool
	IsResponseWriteError bool
	HttpData             HttpRequestData
}

func (tileJsonReqState *TileJsonRequestState) AsJsonMap() map[string]interface{} {
	result := make(map[string]interface{})

	if tileJsonReqState.FetchState > FetchState_Nil {
		fetchResult := make(map[string]interface{})

		fetchResult["state"] = tileJsonReqState.FetchState.String()
		if tileJsonReqState.FetchSize > 0 {
			fetchResult["size"] = tileJsonReqState.FetchSize
		}
		fetchResult["metadata"] = map[string]bool{
			"has_last_modified": tileJsonReqState.StorageMetadata.HasLastModified,
			"has_etag":          tileJsonReqState.StorageMetadata.HasEtag,
		}

		result["fetch"] = fetchResult
	}

	tileJsonReqErrs := make(map[string]bool)
	if tileJsonReqState.IsResponseWriteError {
		tileJsonReqErrs["response_write"] = true
	}
	if tileJsonReqState.IsCondError {
		tileJsonReqErrs["cond"] = true
	}
	if len(tileJsonReqErrs) > 0 {
		result["error"] = tileJsonReqErrs
	}

	result["timing"] = map[string]int64{
		"parse":                   convertDurationToMillis(tileJsonReqState.Duration.Parse),
		"storage_fetch":           convertDurationToMillis(tileJsonReqState.Duration.StorageFetch),
		"storage_read_resp_write": convertDurationToMillis(tileJsonReqState.Duration.StorageReadRespWrite),
		"total":                   convertDurationToMillis(tileJsonReqState.Duration.Total),
	}

	httpJsonData := make(map[string]interface{})
	httpJsonData["path"] = tileJsonReqState.HttpData.Path
	if userAgent := tileJsonReqState.HttpData.UserAgent; userAgent != "" {
		httpJsonData["user_agent"] = userAgent
	}
	if referrer := tileJsonReqState.HttpData.Referrer; referrer != "" {
		httpJsonData["referer"] = referrer
	}
	if apiKey := tileJsonReqState.HttpData.ApiKey; apiKey != "" {
		httpJsonData["api_key"] = apiKey
	}
	if format := tileJsonReqState.Format; format != nil {
		httpJsonData["format"] = format.Name()
	}
	result["http"] = httpJsonData

	return result
}

func TileJsonHandler(p Parser, storage Storage, mw metricsWriter, logger JsonLogger) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		tileJsonReqState := TileJsonRequestState{}
		numRequests.Add(1)

		startTime := time.Now()

		defer func() {
			totalDuration := time.Since(startTime)
			tileJsonReqState.Duration.Total = totalDuration

			logger.TileJson(tileJsonReqState.AsJsonMap())

			mw.WriteTileJsonState(&tileJsonReqState)
		}()

		parseStart := time.Now()
		parseResult, err := p.Parse(req)
		tileJsonReqState.Duration.Parse = time.Since(parseStart)
		if parseResult != nil {
			// set the http data here so that on 404s we log the path too
			tileJsonReqState.HttpData = parseResult.HttpData
		}
		if err != nil {
			switch err := err.(type) {
			case *TileJsonParseError:
				tileJsonReqState.ResponseState = ResponseState_NotFound
				// format not found
				http.Error(rw, "Not Found", http.StatusNotFound)
				logger.Warning(LogCategory_ParseError, "foo bar!")
				return
			case *CondParseError:
				logger.Warning(LogCategory_ConditionError, err.Error())
				tileJsonReqState.IsCondError = true
				// we can continue down this path, just not use the condition in the storage fetch
			default:
				logger.Warning(LogCategory_ParseError, fmt.Sprintf("Unknown parse error: %#v", err))
			}
		}
		tileJsonReqState.HttpData = parseResult.HttpData
		tileJsonData := parseResult.AdditionalData.(*TileJsonParseData)
		tileJsonReqState.Format = &tileJsonData.Format

		storageFetchStart := time.Now()
		storageResult, err := storage.TileJson(tileJsonData.Format, parseResult.Cond)
		tileJsonReqState.Duration.StorageFetch = time.Since(storageFetchStart)
		if err != nil {
			http.Error(rw, "Internal Server Error", http.StatusInternalServerError)
			logger.Warning(LogCategory_StorageError, "Metatile storage fetch failure: %#v", err)
			tileJsonReqState.ResponseState = ResponseState_Error
			tileJsonReqState.FetchState = FetchState_FetchError
			storageFetchErrors.Add(1)
			return
		}
		if storageResult.NotFound {
			numStorageMisses.Add(1)
			http.NotFound(rw, req)
			tileJsonReqState.ResponseState = ResponseState_NotFound
			tileJsonReqState.FetchState = FetchState_NotFound
			return
		}
		numStorageHits.Add(1)
		tileJsonReqState.FetchState = FetchState_Success

		if storageResult.NotModified {
			numStorageNotModified.Add(1)
			rw.WriteHeader(http.StatusNotModified)
			tileJsonReqState.ResponseState = ResponseState_NotModified
			return
		}
		numStorageReads.Add(1)
		storageResp := storageResult.Response

		defer storageResp.Body.Close()

		headers := rw.Header()
		headers.Set("Content-Type", parseResult.ContentType)
		headers.Set("Content-Length", fmt.Sprintf("%d", storageResp.Size))
		tileJsonReqState.FetchSize = storageResp.Size
		if lastMod := storageResp.LastModified; lastMod != nil {
			lastModifiedFormatted := lastMod.UTC().Format(http.TimeFormat)
			headers.Set("Last-Modified", lastModifiedFormatted)
			tileJsonReqState.StorageMetadata.HasLastModified = true
		}
		if etag := storageResp.ETag; etag != nil {
			headers.Set("ETag", *etag)
			tileJsonReqState.StorageMetadata.HasEtag = true
		}

		rw.WriteHeader(http.StatusOK)
		tileJsonReqState.ResponseState = ResponseState_Success
		storageReadRespWriteStart := time.Now()
		_, err = io.Copy(rw, storageResp.Body)
		tileJsonReqState.Duration.StorageReadRespWrite = time.Since(storageReadRespWriteStart)
		if err != nil {
			responseWriteErrors.Add(1)
			logger.Error(LogCategory_ResponseError, "Failed to write response body: %#v", err)
			tileJsonReqState.IsResponseWriteError = true
		}
	})
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
			mw.WriteMetatileState(&reqState)

		}()

		parseStart := time.Now()
		parseResult, err := p.Parse(req)
		reqState.Duration.Parse = time.Since(parseStart)
		metatileData := parseResult.AdditionalData.(*MetatileParseData)
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

		reqState.Coord = &metatileData.Coord
		reqState.Format = reqState.Coord.Format
		reqState.HttpData = parseResult.HttpData

		metaCoord, offset, err := metatileData.Coord.MetaAndOffset(metatileSize, tileSize)
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
				http.Error(rw, "Internal Server Error", http.StatusInternalServerError)
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

		storageResp := storageResult.Response

		// ensure that now we have a body to read, it always gets closed
		defer storageResp.Body.Close()

		// grab a buffer used to store the response in memory
		buf := bufferManager.Get()
		defer bufferManager.Put(buf)

		// metatile reader needs to be able to seek in the buffer and know
		// its size. the easiest way to ensure that is to buffer the whole
		// thing into memory.
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
		// make sure to close zip file reader
		defer reader.Close()
		reqState.ResponseSize = int(formatSize)
		headers.Set("Content-Length", fmt.Sprintf("%d", formatSize))

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

func HealthCheckHandler(storages []Storage, logger JsonLogger) http.Handler {

	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		healthy := true

		for _, storage := range storages {
			storageErr := storage.HealthCheck()

			if storageErr != nil {
				logger.Error(LogCategory_StorageError, "Healthcheck on storage %s failed: %s", storage, storageErr.Error())
				healthy = false
				break
			}
		}

		if healthy {
			rw.WriteHeader(200)
		} else {
			rw.WriteHeader(500)
		}
	})
}
