package state

import (
	"net/http"
	"time"

	"github.com/tilezen/tapalcatl/pkg/storage"
	"github.com/tilezen/tapalcatl/pkg/tile"
)

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

type HttpRequestData struct {
	Path      string
	ApiKey    string
	UserAgent string
	Referrer  string
}

type ReqCacheData struct {
	VectorCacheHit bool
}

type ParseResultType int

const (
	ParseResultType_Nil ParseResultType = iota
	ParseResultType_Metatile
	ParseResultType_Tilejson
)

type Parser interface {
	Parse(*http.Request) (*ParseResult, error)
}

type ParseResult struct {
	Type        ParseResultType
	Cond        storage.Condition
	ContentType string
	HttpData    HttpRequestData
	BuildID     string
	// set to be more specific data based on parse type
	AdditionalData interface{}
}

type VectorTileResponseData struct {
	ContentType   string
	LastModified  *time.Time
	ETag          *string
	ResponseState ReqResponseState
	Data          []byte
}

type MetatileParseData struct {
	Coord tile.TileCoord
}

type RequestState struct {
	ResponseState        ReqResponseState
	FetchState           ReqFetchState
	FetchSize            ReqFetchSize
	StorageMetadata      ReqStorageMetadata
	Cache                ReqCacheData
	IsZipError           bool
	IsResponseWriteError bool
	IsCondError          bool
	IsCacheLookupError   bool
	Duration             ReqDuration
	Coord                *tile.TileCoord
	HttpData             HttpRequestData
	Format               string
	ResponseSize         int
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
	if reqState.IsCacheLookupError {
		reqStateErrs["cache_lookup"] = true
	}
	if len(reqStateErrs) > 0 {
		result["error"] = reqStateErrs
	}

	result["timing"] = map[string]int64{
		"parse":         reqState.Duration.Parse.Milliseconds(),
		"cache_lookup":  reqState.Duration.CacheLookup.Milliseconds(),
		"cache_set":     reqState.Duration.CacheSet.Milliseconds(),
		"storage_fetch": reqState.Duration.StorageFetch.Milliseconds(),
		"storage_read":  reqState.Duration.StorageRead.Milliseconds(),
		"metatile_find": reqState.Duration.MetatileFind.Milliseconds(),
		"resp_write":    reqState.Duration.RespWrite.Milliseconds(),
		"total":         reqState.Duration.Total.Milliseconds(),
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

	cacheJsonData := make(map[string]interface{})
	cacheJsonData["vector_hit"] = reqState.Cache.VectorCacheHit
	result["cache"] = cacheJsonData

	return result
}

type TileJsonDuration struct {
	Total, Parse, StorageFetch, StorageReadRespWrite time.Duration
}

type TileJsonRequestState struct {
	Duration             TileJsonDuration
	Format               *storage.TileJsonFormat
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
		"parse":                   tileJsonReqState.Duration.Parse.Milliseconds(),
		"storage_fetch":           tileJsonReqState.Duration.StorageFetch.Milliseconds(),
		"storage_read_resp_write": tileJsonReqState.Duration.StorageReadRespWrite.Milliseconds(),
		"total":                   tileJsonReqState.Duration.Total.Milliseconds(),
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
	Parse        time.Duration
	StorageFetch time.Duration
	CacheLookup  time.Duration
	StorageRead  time.Duration
	MetatileFind time.Duration
	RespWrite    time.Duration
	Total        time.Duration
	CacheSet     time.Duration
}

// durations will be logged in milliseconds
type JsonReqDuration struct {
	Parse        int64
	CacheLookup  int64
	StorageFetch int64
	StorageRead  int64
	MetatileFind int64
	RespWrite    int64
	Total        int64
}
