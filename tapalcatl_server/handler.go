package main

import (
	"bytes"
	"fmt"
	"github.com/tilezen/tapalcatl"
	"io"
	"log"
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

// the logState structures and string generation serve to emit a single log entry line
// a log parser will pick this up and use it to persist metrics
// the string functions here are specific to the format used and should be updated with care

type logResponseState int32

const (
	ResponseState_Nil logResponseState = iota
	ResponseState_Success
	ResponseState_NotModified
	ResponseState_NotFound
	ResponseState_BadRequest
	ResponseState_Error
)

func (lrs logResponseState) String() string {
	switch lrs {
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

type logFetchState int32

const (
	FetchState_Nil logFetchState = iota
	FetchState_Success
	FetchState_NotFound
	FetchState_FetchError
	FetchState_ReadError
)

func (lfs logFetchState) String() string {
	switch lfs {
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

type logFetchSize struct {
	bodySize, bytesLength, bytesCap int64
}

type logStorageMetadata struct {
	hasLastModified, hasEtag bool
}

type logEntryState struct {
	responseState        logResponseState
	fetchState           logFetchState
	fetchSize            logFetchSize
	storageMetadata      logStorageMetadata
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
func (logState *logEntryState) String() string {

	var fetchSize string
	if logState.fetchSize.bodySize > 0 {
		fetchSize = fmt.Sprintf(
			"%d %d %d",
			logState.fetchSize.bodySize,
			logState.fetchSize.bytesLength,
			logState.fetchSize.bytesCap,
		)
	} else {
		fetchSize = "nil"
	}

	hasLastMod := logBool(logState.storageMetadata.hasLastModified)
	hasEtag := logBool(logState.storageMetadata.hasEtag)

	isZipErr := logBool(logState.isZipError)
	isRespErr := logBool(logState.isResponseWriteError)
	isCondErr := logBool(logState.isCondError)

	result := fmt.Sprintf(
		"METRICS: respstate(%s) fetchstate(%s) fetchsize(%s) ziperr(%s) resperr(%s) conderr(%s) lastmod(%s) etag(%s)",
		logState.responseState,
		logState.fetchState,
		fetchSize,
		isZipErr,
		isRespErr,
		isCondErr,
		hasLastMod,
		hasEtag,
	)

	return result
}

func MetatileHandler(p Parser, metatileSize int, mimeMap map[string]string, storage Storage, bufferManager BufferManager, logger *log.Logger) http.Handler {

	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {

		logState := logEntryState{}

		numRequests.Add(1)
		start_time := time.Now()
		defer func() {
			updateCounters(time.Since(start_time))

			// relies on the Stringer implementation to format the record correctly
			logger.Printf("%s", &logState)

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
					logState.responseState = ResponseState_NotFound
					response = pe.MimeError.Error()
				} else if pe.CoordError != nil {
					sc = http.StatusBadRequest
					logState.responseState = ResponseState_BadRequest
					response = pe.CoordError.Error()
				} else if pe.CondError != nil {
					logState.isCondError = true
					logger.Printf("WARNING: Condition Error: %s", pe.CondError)
				}
			} else {
				logger.Printf("ERROR: Unknown parse error: %#v\n", err)
				sc = http.StatusInternalServerError
				response = "Internal server error"
				logState.responseState = ResponseState_Error
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
				logState.fetchState = FetchState_FetchError
				logState.responseState = ResponseState_Error
			} else {
				numStorageMisses.Add(1)
				http.NotFound(rw, req)
				logState.fetchState = FetchState_NotFound
				logState.responseState = ResponseState_NotFound
			}
			return
		}
		numStorageHits.Add(1)
		logState.fetchState = FetchState_Success

		if storageResult.NotModified {
			numStorageNotModified.Add(1)
			rw.WriteHeader(http.StatusNotModified)
			logState.responseState = ResponseState_NotModified
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
			logState.fetchState = FetchState_ReadError
			logState.responseState = ResponseState_Error
			return
		}
		logState.fetchState = FetchState_Success

		storageBytes := buf.Bytes()
		logState.fetchSize.bodySize = bodySize
		logState.fetchSize.bytesLength = int64(len(storageBytes))
		logState.fetchSize.bytesCap = int64(cap(storageBytes))

		headers := rw.Header()
		headers.Set("Content-Type", parseResult.ContentType)
		if lastMod := storageResp.LastModified; lastMod != nil {
			lastModifiedFormatted := lastMod.Format(time.RFC1123Z)
			headers.Set("Last-Modified", lastModifiedFormatted)
			logState.storageMetadata.hasLastModified = true
		}
		if etag := storageResp.ETag; etag != nil {
			headers.Set("ETag", *etag)
			logState.storageMetadata.hasEtag = true
		}

		reader, err := tapalcatl.NewMetatileReader(offset, bytes.NewReader(storageBytes), bodySize)
		if err != nil {
			metatileReadErrors.Add(1)
			logger.Printf("ERROR: Failed to read metatile: %s", err.Error())
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			logState.isZipError = true
			logState.responseState = ResponseState_Error
			return
		}

		rw.WriteHeader(http.StatusOK)
		logState.responseState = ResponseState_Success
		_, err = io.Copy(rw, reader)
		if err != nil {
			responseWriteErrors.Add(1)
			logger.Printf("ERROR: Failed to write response body: %s", err.Error())
			logState.isResponseWriteError = true
		}
	})
}
