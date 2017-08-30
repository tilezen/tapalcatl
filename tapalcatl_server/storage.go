package main

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/imkira/go-interpol"
	"github.com/tilezen/tapalcatl"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type Condition struct {
	IfModifiedSince *time.Time
	IfNoneMatch     *string
}

type SuccessfulResponse struct {
	Body         io.ReadCloser
	LastModified *time.Time
	ETag         *string
	Size         uint64
}

type StorageResponse struct {
	Response    *SuccessfulResponse
	NotModified bool
	NotFound    bool
}

type TileJsonFormat int

const (
	TileJsonFormat_Mvt = iota
	TileJsonFormat_Json
	TileJsonFormat_Topojson
)

func (f *TileJsonFormat) Name() string {
	switch *f {
	case TileJsonFormat_Mvt:
		return "mapbox"
	case TileJsonFormat_Json:
		return "geojson"
	case TileJsonFormat_Topojson:
		return "topojson"
	}
	panic(fmt.Sprintf("Unknown tilejson format: %d", int(*f)))
}

func NewTileJsonFormat(name string) *TileJsonFormat {
	var format TileJsonFormat
	switch name {
	case "mapbox":
		format = TileJsonFormat_Mvt
	case "geojson":
		format = TileJsonFormat_Json
	case "topojson":
		format = TileJsonFormat_Topojson
	default:
		return nil
	}
	return &format
}

type Storage interface {
	Fetch(t tapalcatl.TileCoord, c Condition) (*StorageResponse, error)
	TileJson(f TileJsonFormat, c Condition) (*StorageResponse, error)
	HealthCheck() error
}

type S3Storage struct {
	client          s3iface.S3API
	bucket          string
	keyPattern      string
	tilejsonPattern string
	prefix          string
	layer           string
	healthcheck     string
}

func NewS3Storage(api s3iface.S3API, bucket, keyPattern, prefix, layer string, healthcheck string) *S3Storage {
	return &S3Storage{
		client:      api,
		bucket:      bucket,
		keyPattern:  keyPattern,
		prefix:      prefix,
		layer:       layer,
		healthcheck: healthcheck,
	}
}

func (s *S3Storage) s3Hash(t tapalcatl.TileCoord) string {
	to_hash := fmt.Sprintf("/%s/%d/%d/%d.%s", s.layer, t.Z, t.X, t.Y, t.Format)
	hash := md5.Sum([]byte(to_hash))
	return fmt.Sprintf("%x", hash)[0:5]
}

func (s *S3Storage) objectKey(t tapalcatl.TileCoord) (string, error) {
	m := map[string]string{
		"z":      strconv.Itoa(t.Z),
		"x":      strconv.Itoa(t.X),
		"y":      strconv.Itoa(t.Y),
		"fmt":    t.Format,
		"hash":   s.s3Hash(t),
		"prefix": s.prefix,
		"layer":  s.layer,
	}

	return interpol.WithMap(s.keyPattern, m)
}

func (s *S3Storage) respondWithKey(key string, c Condition) (*StorageResponse, error) {
	var result *StorageResponse

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
					NotFound: true,
				}
				return result, nil
			case "NotModified":
				result = &StorageResponse{
					NotModified: true,
				}
				return result, nil
			default:
				return nil, err
			}
		}
	}

	// ensure that it's safe to always close the body upstream
	var storageSize uint64
	var body io.ReadCloser
	if output.Body == nil {
		body = ioutil.NopCloser(&bytes.Buffer{})
	} else {
		body = output.Body
		if output.ContentLength != nil {
			storageSize = uint64(*output.ContentLength)
		}
	}

	result = &StorageResponse{
		Response: &SuccessfulResponse{
			Body:         body,
			LastModified: output.LastModified,
			ETag:         output.ETag,
			Size:         storageSize,
		},
	}

	return result, nil
}

func (s *S3Storage) Fetch(t tapalcatl.TileCoord, c Condition) (*StorageResponse, error) {
	key, err := s.objectKey(t)
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

func (s *S3Storage) TileJson(f TileJsonFormat, c Condition) (*StorageResponse, error) {
	filename := f.Name()
	toHash := fmt.Sprintf("/tilejson/%s.json", filename)
	hash := md5.Sum([]byte(toHash))
	hashUrlPathSegment := fmt.Sprintf("%x", hash)[0:5]
	key := fmt.Sprintf("%s/%s/%s", s.prefix, hashUrlPathSegment, toHash)
	return s.respondWithKey(key, c)
}

type FileStorage struct {
	baseDir     string
	layer       string
	healthcheck string
}

func NewFileStorage(baseDir, layer string, healthcheck string) *FileStorage {
	return &FileStorage{
		baseDir:     baseDir,
		layer:       layer,
		healthcheck: healthcheck,
	}
}

func respondWithPath(path string) (*StorageResponse, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			resp := &StorageResponse{
				NotFound: true,
			}
			return resp, nil

		} else {
			return nil, err
		}
	} else {
		resp := &StorageResponse{
			Response: &SuccessfulResponse{
				Body: file,
			},
		}
		return resp, nil
	}
}

func (f *FileStorage) Fetch(t tapalcatl.TileCoord, c Condition) (*StorageResponse, error) {
	tilepath := filepath.Join(f.baseDir, f.layer, filepath.FromSlash(t.FileName()))
	return respondWithPath(tilepath)
}

func (s *FileStorage) TileJson(f TileJsonFormat, c Condition) (*StorageResponse, error) {
	dirpath := "tilejson"
	tileJsonExt := "json"
	filename := fmt.Sprintf("%s.%s", f.Name(), tileJsonExt)
	tilejsonPath := filepath.Join(s.baseDir, dirpath, filename)
	return respondWithPath(tilejsonPath)
}

func (s *FileStorage) HealthCheck() error {
	tilepath := filepath.Join(s.baseDir, s.healthcheck)
	f, err := os.Open(tilepath)
	if err != nil {
		err = f.Close()
	}
	return err
}
