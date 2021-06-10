package storage

import (
	"context"
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/tilezen/tapalcatl/pkg/cache"
	"github.com/tilezen/tapalcatl/pkg/state"
	"github.com/tilezen/tapalcatl/pkg/tile"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/imkira/go-interpol"
	"github.com/vmihailenco/msgpack/v5"
)

type S3Storage struct {
	client          s3iface.S3API
	tileCache       cache.Cache
	bucket          string
	keyPattern      string
	tilejsonPattern string
	defaultPrefix   string
	layer           string
	healthcheck     string
}

func NewS3Storage(api s3iface.S3API, tileCache cache.Cache, bucket, keyPattern, defaultPrefix, layer, healthcheck string) *S3Storage {
	if tileCache == nil {
		tileCache = cache.NilCache
	}

	return &S3Storage{
		client:        api,
		tileCache:     tileCache,
		bucket:        bucket,
		keyPattern:    keyPattern,
		defaultPrefix: defaultPrefix,
		layer:         layer,
		healthcheck:   healthcheck,
	}
}

func (s *S3Storage) s3Hash(t tile.TileCoord) string {
	toHash := fmt.Sprintf("%d/%d/%d.%s", t.Z, t.X, t.Y, t.Format)

	// In versions of code before https://github.com/tilezen/tilequeue/pull/344,
	// we included the layer and leading slash in the hashed string. after that
	// PR, we no longer support having a layer in the path and _also_ drop the
	// leading slash from the hashed string.
	if s.layer != "" {
		toHash = fmt.Sprintf("/%s/%s", s.layer, toHash)
	}

	hash := md5.Sum([]byte(toHash))

	return fmt.Sprintf("%x", hash)[0:5]
}

func (s *S3Storage) objectKey(t tile.TileCoord, prefixOverride string) (string, error) {
	actualPrefix := s.defaultPrefix
	if prefixOverride != "" {
		actualPrefix = prefixOverride
	}

	m := map[string]string{
		"z":      strconv.Itoa(t.Z),
		"x":      strconv.Itoa(t.X),
		"y":      strconv.Itoa(t.Y),
		"fmt":    t.Format,
		"hash":   s.s3Hash(t),
		"prefix": actualPrefix,
		"layer":  s.layer,
	}

	return interpol.WithMap(s.keyPattern, m)
}

func (s *S3Storage) respondWithKey(key string, c state.Condition) (*StorageResponse, error) {
	var result *StorageResponse
	ctx := context.Background()

	cacheKey := fmt.Sprintf("s3://%s/%s", s.bucket, key)
	cached, err := s.tileCache.Get(ctx, cacheKey)
	if err != nil {
		return nil, fmt.Errorf("error fetching from cache: %w", err)
	}

	if cached != nil {
		result = &StorageResponse{}

		err := msgpack.Unmarshal(cached, result)
		if err != nil {
			return nil, fmt.Errorf("couldn't unmarshal cached response: %w", err)
		}

		result.FetchCacheHit = true

		return result, nil
	}

	input := &s3.GetObjectInput{Bucket: &s.bucket, Key: &key}
	input.IfModifiedSince = c.IfModifiedSince
	input.IfNoneMatch = c.IfNoneMatch

	output, err := s.client.GetObject(input)
	// check if we are an error, 304, or 404
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// NOTE: the way to distinguish seems to be string matching on the code ...
			switch awsErr.Code() {
			case "NoSuchKey":
				result = &StorageResponse{
					NotFound:      true,
					FetchCacheHit: false,
				}
				return result, nil
			case "NotModified":
				result = &StorageResponse{
					NotModified:   true,
					FetchCacheHit: false,
				}
				return result, nil
			default:
				return nil, err
			}
		}

		return nil, err
	}

	// ensure that it's safe to always close the body upstream
	var storageSize uint64
	var body []byte
	if output.Body == nil {
		body = make([]byte, 0)
	} else {
		body, err = ioutil.ReadAll(output.Body)
		if err != nil {
			return nil, err
		}

		if output.ContentLength != nil {
			storageSize = uint64(*output.ContentLength)
		}
	}

	result = &StorageResponse{
		FetchCacheHit: false,
		Response: &SuccessfulResponse{
			Body:         body,
			LastModified: output.LastModified,
			ETag:         output.ETag,
			Size:         storageSize,
		},
	}

	if s.tileCache != cache.NilCache {
		marshaledBytes, err := msgpack.Marshal(result)
		if err != nil {
			return nil, fmt.Errorf("couldn't marshal bytes: %w", err)
		}

		err = s.tileCache.Set(ctx, cacheKey, marshaledBytes)
		if err != nil {
			return nil, fmt.Errorf("couldn't set cache: %w", err)
		}
	}

	return result, nil
}

func (s *S3Storage) Fetch(t tile.TileCoord, c state.Condition, prefixOverride string) (*StorageResponse, error) {
	key, err := s.objectKey(t, prefixOverride)
	if err != nil {
		return nil, err
	}

	return s.respondWithKey(key, c)
}

func (s *S3Storage) HealthCheck() error {
	input := &s3.GetObjectInput{Bucket: &s.bucket, Key: &s.healthcheck}
	resp, err := s.client.GetObject(input)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	return err
}

func (s *S3Storage) TileJson(f state.TileJsonFormat, c state.Condition, prefixOverride string) (*StorageResponse, error) {
	filename := f.Name()
	toHash := fmt.Sprintf("/tilejson/%s.json", filename)
	hash := md5.Sum([]byte(toHash))
	hashUrlPathSegment := fmt.Sprintf("%x", hash)[0:5]
	actualPrefix := s.defaultPrefix
	if prefixOverride != "" {
		actualPrefix = prefixOverride
	}
	key := fmt.Sprintf("%s/%s/%s", actualPrefix, hashUrlPathSegment, toHash)
	return s.respondWithKey(key, c)
}
