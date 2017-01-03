package main

import (
	"crypto/md5"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/imkira/go-interpol"
	"github.com/tilezen/tapalcatl"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type S3Storage struct {
	client     s3iface.S3API
	bucket     string
	keyPattern string
	prefix     string
	layer      string
}

func NewS3Storage(api s3iface.S3API, bucket, keyPattern, prefix, layer string) *S3Storage {
	return &S3Storage{
		client:     api,
		bucket:     bucket,
		keyPattern: keyPattern,
		prefix:     prefix,
		layer:      layer,
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

func getHeader(m reflect.Value, f reflect.StructField) (key, value string, ok bool) {
	if ok = m.IsValid(); !ok {
		return
	}

	// don't do private fields
	if ok = f.Name[0:1] != strings.ToLower(f.Name[0:1]); !ok {
		return
	}

	if m.Kind() == reflect.Ptr {
		m = m.Elem()
	}
	if ok = m.IsValid(); !ok {
		return
	}

	location := f.Tag.Get("location")
	if ok = location == "header"; !ok {
		return
	}

	key = f.Tag.Get("locationName")
	if key == "" {
		key = f.Name
	}

	m = reflect.Indirect(m)
	if ok = m.IsValid(); !ok {
		return
	}

	switch val := m.Interface().(type) {
	case string:
		value = val
	case time.Time:
		value = val.Format(time.RFC1123)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr:
		value = fmt.Sprintf("%d", val)
	default:
		ok = false
	}

	return
}

func setHeaders(h http.Header, params interface{}) {
	v := reflect.ValueOf(params).Elem()

	for i := 0; i < v.NumField(); i++ {
		m := v.Field(i)
		f := v.Type().Field(i)

		key, val, ok := getHeader(m, f)
		if ok {
			h.Set(key, val)
		}
	}
}

func (s *S3Storage) Get(t tapalcatl.TileCoord, c Condition) (*GetResponse, error) {
	var result *GetResponse

	key, err := s.objectKey(t)
	if err != nil {
		return nil, err
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
				result = &GetResponse{
					NotFound: true,
				}
				return result, nil
			case "NotModified":
				result = &GetResponse{
					NotModified: true,
				}
				return result, nil
			default:
				return nil, err
			}
		}
	}

	result = &GetResponse{
		Response: &SuccessfulResponse{
			Body:         output.Body,
			LastModified: output.LastModified,
			ETag:         output.ETag,
		},
	}

	return result, nil
}

type FileStorage struct {
	baseDir string
	layer   string
}

func NewFileStorage(baseDir, layer string) *FileStorage {
	return &FileStorage{
		baseDir: baseDir,
		layer:   layer,
	}
}

func (f *FileStorage) Get(t tapalcatl.TileCoord, c Condition) (*GetResponse, error) {
	tilepath := filepath.Join(f.baseDir, f.layer, filepath.FromSlash(t.FileName()))
	file, err := os.Open(tilepath)
	if err != nil {
		if os.IsNotExist(err) {
			resp := &GetResponse{
				NotFound: true,
			}
			return resp, nil

		} else {
			return nil, err
		}
	} else {
		resp := &GetResponse{
			Response: &SuccessfulResponse{
				Body: file,
			},
		}
		return resp, nil
	}
}
