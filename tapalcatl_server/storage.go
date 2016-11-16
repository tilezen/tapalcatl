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
	"reflect"
	"strconv"
	"strings"
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

func (s *S3Storage) Get(t tapalcatl.TileCoord) (*Response, error) {
	key, err := s.objectKey(t)
	if err != nil {
		return nil, err
	}

	input := &s3.GetObjectInput{Bucket: &s.bucket, Key: &key}

	output, err := s.client.GetObject(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// i would have thought there was a better way of detecting this error, but i haven't found one yet.
			if awsErr.Code() == "No Such Key" {
				resp := new(Response)
				resp.StatusCode = 404
				return resp, nil
			}
		}
		return nil, err
	}

	resp := new(Response)
	resp.StatusCode = 200
	resp.Header = make(http.Header)
	setHeaders(resp.Header, output)
	resp.Body = output.Body

	return resp, nil
}
