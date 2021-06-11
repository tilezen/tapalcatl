package handler

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/tilezen/tapalcatl/pkg/log"
	"github.com/tilezen/tapalcatl/pkg/metrics"
	"github.com/tilezen/tapalcatl/pkg/state"
	"github.com/tilezen/tapalcatl/pkg/storage"
)

func TileJsonHandler(p state.Parser, stg storage.Storage, mw metrics.MetricsWriter, logger log.JsonLogger) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		tileJsonReqState := state.TileJsonRequestState{}

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
				tileJsonReqState.ResponseState = state.ResponseState_NotFound
				// format not found
				http.Error(rw, "Not Found", http.StatusNotFound)
				logger.Warning(log.LogCategory_ParseError, "foo bar!")
				return
			case *CondParseError:
				logger.Warning(log.LogCategory_ConditionError, err.Error())
				tileJsonReqState.IsCondError = true
				// we can continue down this path, just not use the condition in the storage fetch
			default:
				logger.Warning(log.LogCategory_ParseError, fmt.Sprintf("Unknown parse error: %#v", err))
			}
		}
		tileJsonReqState.HttpData = parseResult.HttpData
		tileJsonData := parseResult.AdditionalData.(*TileJsonParseData)
		tileJsonReqState.Format = &tileJsonData.Format

		storageFetchStart := time.Now()
		storageResult, err := stg.TileJson(tileJsonData.Format, parseResult.Cond, parseResult.BuildID)
		tileJsonReqState.Duration.StorageFetch = time.Since(storageFetchStart)
		if err != nil {
			http.Error(rw, "Internal Server Error", http.StatusInternalServerError)
			logger.Warning(log.LogCategory_StorageError, "Metatile storage fetch failure: %#v", err)
			tileJsonReqState.ResponseState = state.ResponseState_Error
			tileJsonReqState.FetchState = state.FetchState_FetchError
			return
		}
		if storageResult.NotFound {
			http.NotFound(rw, req)
			tileJsonReqState.ResponseState = state.ResponseState_NotFound
			tileJsonReqState.FetchState = state.FetchState_NotFound
			return
		}
		tileJsonReqState.FetchState = state.FetchState_Success

		if storageResult.NotModified {
			rw.WriteHeader(http.StatusNotModified)
			tileJsonReqState.ResponseState = state.ResponseState_NotModified
			return
		}
		storageResp := storageResult.Response

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
		tileJsonReqState.ResponseState = state.ResponseState_Success
		storageReadRespWriteStart := time.Now()
		_, err = rw.Write(storageResp.Body)
		tileJsonReqState.Duration.StorageReadRespWrite = time.Since(storageReadRespWriteStart)
		if err != nil {
			logger.Error(log.LogCategory_ResponseError, "Failed to write response body: %#v", err)
			tileJsonReqState.IsResponseWriteError = true
		}
	})
}

type TileJsonParseData struct {
	Format state.TileJsonFormat
}

type TileJsonParser struct{}

func (tp *TileJsonParser) Parse(req *http.Request) (*state.ParseResult, error) {
	parseResult := &state.ParseResult{
		Type:        state.ParseResultType_Tilejson,
		ContentType: "application/json",
		HttpData:    ParseHttpData(req),
	}
	m := mux.Vars(req)
	formatName := m["fmt"]
	tileJsonFormat := state.NewTileJsonFormat(formatName)
	if tileJsonFormat == nil {
		return parseResult, &TileJsonParseError{
			InvalidFormat: &formatName,
		}
	}
	tileJsonData := &TileJsonParseData{Format: *tileJsonFormat}
	parseResult.AdditionalData = tileJsonData
	var condErr *CondParseError
	parseResult.Cond, condErr = ParseCondition(req)
	if condErr != nil {
		return parseResult, condErr
	}
	return parseResult, nil
}

type TileJsonParseError struct {
	InvalidFormat *string
}

func (te *TileJsonParseError) Error() string {
	if te.InvalidFormat != nil {
		return fmt.Sprintf("Invalid Format: %s", *te.InvalidFormat)
	}
	return ""
}
