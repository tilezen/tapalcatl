package handler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/tilezen/tapalcatl/pkg/cache"

	"github.com/tilezen/tapalcatl/pkg/buffer"
	"github.com/tilezen/tapalcatl/pkg/log"
	"github.com/tilezen/tapalcatl/pkg/metrics"
	"github.com/tilezen/tapalcatl/pkg/state"
	"github.com/tilezen/tapalcatl/pkg/storage"
	"github.com/tilezen/tapalcatl/pkg/tile"
)

const (
	// cacheTimeout is the amount of time to wait for tile cache to do it's job before timing out.
	cacheTimeout = 100 * time.Millisecond
)

func MetatileHandler(
	p state.Parser,
	metatileSize, tileSize, metatileMaxDetailZoom int,
	stg storage.Storage,
	bufferManager buffer.BufferManager,
	mw metrics.MetricsWriter,
	logger log.JsonLogger,
	tileCache cache.Cache) http.Handler {

	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		reqState := &state.RequestState{}

		startTime := time.Now()

		defer func() {
			totalDuration := time.Since(startTime)
			reqState.Duration.Total = totalDuration

			if reqState.ResponseState == state.ResponseState_Nil {
				logger.Error(log.LogCategory_InvalidCodeState, "handler did not set response state for tile %+v", reqState.Coord)
			}

			jsonReqData := reqState.AsJsonMap()
			logger.Metrics(jsonReqData)

			// write out metrics
			mw.WriteMetatileState(reqState)

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

		metatileData := parseResult.AdditionalData.(*state.MetatileParseData)
		reqState.Coord = &metatileData.Coord
		reqState.Format = reqState.Coord.Format
		reqState.HttpData = parseResult.HttpData

		if tileCache == nil {
			tileCache = &cache.NilCache{}
		}

		cacheLookupStart := time.Now()
		timeoutCtx, cancel := context.WithTimeout(req.Context(), cacheTimeout)
		cached, err := tileCache.GetTile(timeoutCtx, parseResult)
		cancel()
		reqState.Duration.CacheLookup = time.Since(cacheLookupStart)
		if err != nil {
			reqState.IsCacheLookupError = true
			logger.Warning(log.LogCategory_ResponseError, "Error checking cache: %+v", err)
		}

		if cached != nil {
			err := writeVectorTileResponse(reqState, rw, cached)
			if err != nil {
				logger.Error(log.LogCategory_ResponseError, "Failed to write cached response body: %#v", err)
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				reqState.ResponseState = state.ResponseState_Error
				return
			}

			reqState.Cache.VectorCacheHit = true
			reqState.ResponseState = state.ResponseState_Success
			return
		}

		responseData, err := extractVectorTileFromMetatile(reqState, stg, bufferManager, parseResult, metatileSize, tileSize, metatileMaxDetailZoom)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			reqState.ResponseState = state.ResponseState_Error
			return
		}

		if responseData.ResponseState == state.ResponseState_NotFound {
			http.NotFound(rw, req)
			reqState.ResponseState = state.ResponseState_NotFound
			return
		} else if responseData.ResponseState == state.ResponseState_NotModified {
			rw.WriteHeader(http.StatusNotModified)
			reqState.ResponseState = state.ResponseState_NotModified
			return
		}

		err = writeVectorTileResponse(reqState, rw, responseData)
		if err != nil {
			// TODO Context cancellation might happen here?
			logger.Error(log.LogCategory_ResponseError, "Failed to write response body: %#v", err)
			// Still want to set the cache in this case
		}

		// Cache the response
		// TODO Do this in a goroutine so the handler can exit faster?
		cacheSetStart := time.Now()
		timeoutCtx, cancel = context.WithTimeout(req.Context(), cacheTimeout)
		err = tileCache.SetTile(timeoutCtx, parseResult, responseData)
		cancel()
		reqState.Duration.CacheSet = time.Since(cacheSetStart)
		if err != nil {
			logger.Error(log.LogCategory_ResponseError, "Failed to set cache: %#v", err)
		}
	})
}

func writeVectorTileResponse(reqState *state.RequestState, rw http.ResponseWriter, data *state.VectorTileResponseData) error {
	headers := rw.Header()

	headers.Set("Content-Type", data.ContentType)
	headers.Set("Content-Length", fmt.Sprintf("%d", len(data.Data)))

	if lastMod := data.LastModified; lastMod != nil {
		// It's important to write the last-modified header in an HTTP-compliant way.
		// Go exposes http.TimeFormat for that, but hard-codes "GMT" at the end, though,
		// so we need to make sure we convert the time to UTC before formatting.
		lastModifiedFormatted := lastMod.UTC().Format(http.TimeFormat)
		headers.Set("Last-Modified", lastModifiedFormatted)
		reqState.StorageMetadata.HasLastModified = true
	}

	if etag := data.ETag; etag != nil {
		headers.Set("ETag", *etag)
		reqState.StorageMetadata.HasEtag = true
	}

	rw.WriteHeader(http.StatusOK)
	reqState.ResponseState = state.ResponseState_Success
	respWriteStart := time.Now()
	_, err := rw.Write(data.Data)
	reqState.Duration.RespWrite = time.Since(respWriteStart)
	if err != nil {
		reqState.IsResponseWriteError = true
		return fmt.Errorf("failed to write response body: %w", err)
	}

	return nil
}

func extractVectorTileFromMetatile(reqState *state.RequestState, stg storage.Storage, bufferManager buffer.BufferManager, parseResult *state.ParseResult, metatileSize, tileSize, metatileMaxDetailZoom int) (*state.VectorTileResponseData, error) {
	responseData := &state.VectorTileResponseData{}
	responseData.ContentType = parseResult.ContentType
	metatileData := parseResult.AdditionalData.(*state.MetatileParseData)

	// Get the offset coordinate inside the metatile where we should be able to find the vector tile
	metaCoord, offset, err := metatileData.Coord.MetaAndOffset(metatileSize, tileSize, metatileMaxDetailZoom)
	if err != nil {
		// Note: FetchState is left as nil, since no fetch was performed
		reqState.ResponseState = state.ResponseState_Error
		responseData.ResponseState = state.ResponseState_Error
		return responseData, fmt.Errorf("metaAndOffset could not be calculated: %w", err)
	}

	// Fetch the metatile zip file from storage
	storageFetchStart := time.Now()
	storageResult, err := stg.Fetch(metaCoord, parseResult.Cond, parseResult.BuildID)
	reqState.Duration.StorageFetch = time.Since(storageFetchStart)

	if err != nil || storageResult.NotFound {
		if err != nil {
			reqState.FetchState = state.FetchState_FetchError
			reqState.ResponseState = state.ResponseState_Error
			responseData.ResponseState = state.ResponseState_Error
			return responseData, fmt.Errorf("metatile storage fetch failure: %w", err)
		}

		reqState.FetchState = state.FetchState_NotFound
		reqState.ResponseState = state.ResponseState_NotFound
		responseData.ResponseState = state.ResponseState_NotFound
		return responseData, nil
	}

	reqState.FetchState = state.FetchState_Success

	if storageResult.NotModified {
		reqState.ResponseState = state.ResponseState_NotModified
		responseData.ResponseState = state.ResponseState_NotModified
		return responseData, nil
	}

	// Copy the last-modified and etag headers from the metatile over to the vector tile
	if lastMod := storageResult.Response.LastModified; lastMod != nil {
		responseData.LastModified = lastMod
		reqState.StorageMetadata.HasLastModified = true
	}

	if etag := storageResult.Response.ETag; etag != nil {
		responseData.ETag = etag
		reqState.StorageMetadata.HasLastModified = true
	}

	storageResp := storageResult.Response

	// Ensure that the body always gets closed
	defer storageResp.Body.Close()

	// Grab a buffer used to store the response in memory
	buf := bufferManager.Get()
	defer bufferManager.Put(buf)

	// Read the whole metatile into memory because the metatile reader
	// needs to be able to seek in the buffer and know its size.
	storageReadStart := time.Now()
	bodySize, err := io.Copy(buf, storageResp.Body)
	reqState.Duration.StorageRead = time.Since(storageReadStart)
	if err != nil {
		reqState.FetchState = state.FetchState_ReadError
		reqState.ResponseState = state.ResponseState_Error
		responseData.ResponseState = state.ResponseState_Error
		return responseData, fmt.Errorf("failed to read storage body: %w", err)
	}
	reqState.FetchState = state.FetchState_Success

	storageBytes := buf.Bytes()
	reqState.FetchSize.BodySize = bodySize
	reqState.FetchSize.BytesLength = int64(len(storageBytes))
	reqState.FetchSize.BytesCap = int64(cap(storageBytes))

	// Set up the metatile reader to read the vector tile out of the metatile
	metatileReaderFindStart := time.Now()
	reader, formatSize, err := tile.NewMetatileReader(offset, bytes.NewReader(storageBytes), bodySize)
	reqState.Duration.MetatileFind = time.Since(metatileReaderFindStart)
	if err != nil {
		reqState.IsZipError = true
		reqState.ResponseState = state.ResponseState_Error
		responseData.ResponseState = state.ResponseState_Error
		return responseData, fmt.Errorf("failed to read metatile: %w", err)
	}

	// Copy the bytes of the vector tile from the metatile into another buffer
	tileBuf := bufferManager.Get()
	defer bufferManager.Put(tileBuf)
	_, err = io.Copy(tileBuf, reader)
	if err != nil {
		reqState.IsZipError = true
		reqState.ResponseState = state.ResponseState_Error
		responseData.ResponseState = state.ResponseState_Error
		return responseData, fmt.Errorf("failed to read tile out of metatile: %w", err)
	}

	err = reader.Close()
	if err != nil {
		reqState.IsZipError = true
		reqState.ResponseState = state.ResponseState_Error
		responseData.ResponseState = state.ResponseState_Error
		return responseData, fmt.Errorf("failed to close vector tile reader: %w", err)
	}

	reqState.ResponseSize = int(formatSize)
	responseData.Data = tileBuf.Bytes()

	return responseData, nil
}

type MetatileMuxParser struct {
	MimeMap map[string]string
}

func (mp *MetatileMuxParser) Parse(req *http.Request) (*state.ParseResult, error) {
	m := mux.Vars(req)

	var contentType string
	var err error
	var ok bool

	parseResult := &state.ParseResult{
		Type:     state.ParseResultType_Metatile,
		HttpData: ParseHttpData(req),
	}
	metatileData := &state.MetatileParseData{}
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
