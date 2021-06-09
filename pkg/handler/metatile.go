package handler

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"github.com/tilezen/tapalcatl/pkg/buffer"
	"github.com/tilezen/tapalcatl/pkg/log"
	"github.com/tilezen/tapalcatl/pkg/metrics"
	"github.com/tilezen/tapalcatl/pkg/state"
	"github.com/tilezen/tapalcatl/pkg/storage"
	"github.com/tilezen/tapalcatl/pkg/tile"
)

func MetatileHandler(
	p Parser,
	metatileSize, tileSize, metatileMaxDetailZoom int,
	mimeMap map[string]string,
	stg storage.Storage,
	bufferManager buffer.BufferManager,
	mw metrics.MetricsWriter,
	logger log.JsonLogger) http.Handler {

	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {

		reqState := state.RequestState{}

		startTime := time.Now()

		defer func() {
			totalDuration := time.Since(startTime)
			reqState.Duration.Total = totalDuration

			if reqState.ResponseState == state.ResponseState_Nil {
				logger.Error(log.LogCategory_InvalidCodeState, "handler did not set response state")
			}

			jsonReqData := reqState.AsJsonMap()
			logger.Metrics(jsonReqData)

			// write out metrics
			mw.WriteMetatileState(&reqState)

		}()

		parseStart := time.Now()
		parseResult, err := p.Parse(req)
		reqState.Duration.Parse = time.Since(parseStart)
		if err != nil {
			var sc int
			var response string

			if pe, ok := err.(*ParseError); ok {
				logger.Warning(log.LogCategory_ParseError, err.Error())
				if pe.MimeError != nil {
					sc = http.StatusNotFound
					reqState.ResponseState = state.ResponseState_NotFound
					response = pe.MimeError.Error()
				} else if pe.CoordError != nil {
					sc = http.StatusBadRequest
					reqState.ResponseState = state.ResponseState_BadRequest
					response = pe.CoordError.Error()
				} else if pe.CondError != nil {
					reqState.IsCondError = true
					logger.Warning(log.LogCategory_ConditionError, pe.CondError.Error())
				}
			} else {
				logger.Error(log.LogCategory_ParseError, "Unknown parse error: %#v\n", err)
				sc = http.StatusInternalServerError
				response = "Internal server error"
				reqState.ResponseState = state.ResponseState_Error
			}

			// only return an error response when not a condition parse error
			// NOTE: maybe it's better to not consider this an error, but
			// capture it in the parse result state and handle it that way?
			if sc > 0 {
				http.Error(rw, response, sc)
				return
			}
		}

		metatileData := parseResult.AdditionalData.(*MetatileParseData)
		reqState.Coord = &metatileData.Coord
		reqState.Format = reqState.Coord.Format
		reqState.HttpData = parseResult.HttpData

		metaCoord, offset, err := metatileData.Coord.MetaAndOffset(metatileSize, tileSize, metatileMaxDetailZoom)
		if err != nil {
			logger.Warning(log.LogCategory_ConfigError, "MetaAndOffset could not be calculated: %s", err.Error())
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			reqState.ResponseState = state.ResponseState_Error
			// Note: FetchState is left as nil, since no fetch was performed
			return
		}

		storageFetchStart := time.Now()
		storageResult, err := stg.Fetch(metaCoord, parseResult.Cond, parseResult.BuildID)
		reqState.Duration.StorageFetch = time.Since(storageFetchStart)

		if err != nil || storageResult.NotFound {
			if err != nil {
				logger.Warning(log.LogCategory_StorageError, "Metatile storage fetch failure: %#v", err)
				http.Error(rw, "Internal Server Error", http.StatusInternalServerError)
				reqState.FetchState = state.FetchState_FetchError
				reqState.ResponseState = state.ResponseState_Error
			} else {
				http.NotFound(rw, req)
				reqState.FetchState = state.FetchState_NotFound
				reqState.ResponseState = state.ResponseState_NotFound
			}
			return
		}

		reqState.FetchState = state.FetchState_Success

		if storageResult.NotModified {
			rw.WriteHeader(http.StatusNotModified)
			reqState.ResponseState = state.ResponseState_NotModified
			return
		}

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
			logger.Error(log.LogCategory_StorageError, "Failed to read storage body: %#v", err)
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			reqState.FetchState = state.FetchState_ReadError
			reqState.ResponseState = state.ResponseState_Error
			return
		}
		reqState.FetchState = state.FetchState_Success

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
		reader, formatSize, err := tile.NewMetatileReader(offset, bytes.NewReader(storageBytes), bodySize)
		reqState.Duration.MetatileFind = time.Since(metatileReaderFindStart)
		if err != nil {
			logger.Error(log.LogCategory_MetatileError, "Failed to read metatile: %#v", err)
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			reqState.IsZipError = true
			reqState.ResponseState = state.ResponseState_Error
			return
		}
		// make sure to close zip file reader
		defer reader.Close()
		reqState.ResponseSize = int(formatSize)
		headers.Set("Content-Length", fmt.Sprintf("%d", formatSize))

		rw.WriteHeader(http.StatusOK)
		reqState.ResponseState = state.ResponseState_Success
		respWriteStart := time.Now()
		_, err = io.Copy(rw, reader)
		reqState.Duration.RespWrite = time.Since(respWriteStart)
		if err != nil {
			logger.Error(log.LogCategory_ResponseError, "Failed to write response body: %#v", err)
			reqState.IsResponseWriteError = true
		}
	})
}

type MetatileMuxParser struct {
	MimeMap map[string]string
}

func (mp *MetatileMuxParser) Parse(req *http.Request) (*ParseResult, error) {
	m := mux.Vars(req)

	var contentType string
	var err error
	var ok bool

	parseResult := &ParseResult{
		Type:     ParseResultType_Metatile,
		HttpData: ParseHttpData(req),
	}
	metatileData := &MetatileParseData{}
	parseResult.AdditionalData = metatileData

	fmt := m["fmt"]
	if contentType, ok = mp.MimeMap[fmt]; !ok {
		return parseResult, &ParseError{
			MimeError: &MimeParseError{
				BadFormat: fmt,
			},
		}
	}
	parseResult.ContentType = contentType
	t := &metatileData.Coord
	t.Format = fmt

	parseResult.BuildID = req.URL.Query().Get("buildid")

	var coordError CoordParseError
	z := m["z"]
	t.Z, err = strconv.Atoi(z)
	if err != nil {
		coordError.BadZ = z
	}

	x := m["x"]
	t.X, err = strconv.Atoi(x)
	if err != nil {
		coordError.BadX = x
	}

	y := m["y"]
	t.Y, err = strconv.Atoi(y)
	if err != nil {
		coordError.BadY = y
	}

	if coordError.IsError() {
		return parseResult, &ParseError{
			CoordError: &coordError,
		}
	}
	var condErr *CondParseError
	parseResult.Cond, condErr = ParseCondition(req)
	if condErr != nil {
		return parseResult, &ParseError{CondError: condErr}
	}

	return parseResult, nil
}

type MetatileParseData struct {
	Coord tile.TileCoord
}
