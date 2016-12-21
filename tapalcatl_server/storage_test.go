package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/tilezen/tapalcatl"
	"strconv"
	"testing"
	"time"
)

type mockS3 struct {
	s3iface.S3API
	expectedKey string
}

func (m *mockS3) GetObject(i *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if *i.Key == m.expectedKey {
		length := new(int64)
		*length = 0

		etag := new(string)
		*etag = "1234"

		last_mod := new(time.Time)
		*last_mod = time.Date(2016, time.November, 17, 12, 27, 0, 0, time.UTC)

		obj := &s3.GetObjectOutput{
			Body:          &emptyReadCloser{},
			ContentLength: length,
			ETag:          etag,
			LastModified:  last_mod,
		}
		return obj, nil

	} else {
		return nil, awserr.New("No Such Key", "The key was not found.", fmt.Errorf("Not Found."))
	}
}

func TestS3StorageEmpty(t *testing.T) {
	api := &mockS3{}

	bucket := "bucket"
	keyPattern := "/{hash}/{prefix}/{layer}/{z}/{x}/{y}.{fmt}"
	prefix := "prefix"
	layer := "layer"

	storage := NewS3Storage(api, bucket, keyPattern, prefix, layer)

	resp, err := storage.Get(tapalcatl.TileCoord{Z: 0, X: 0, Y: 0, Format: "zip"}, Condition{})
	if err != nil {
		t.Fatalf("Unable to Get tile from Mock S3: %s", err.Error())
	}
	if resp.StatusCode != 404 {
		t.Fatalf("Expected 404 response from empty storage, but got %d", resp.StatusCode)
	}
}

func TestS3Storage(t *testing.T) {
	api := &mockS3{expectedKey: "/prefix/fa9bb/layer/0/0/0.zip"}

	bucket := "bucket"
	keyPattern := "/{prefix}/{hash}/{layer}/{z}/{x}/{y}.{fmt}"
	prefix := "prefix"
	layer := "layer"

	storage := NewS3Storage(api, bucket, keyPattern, prefix, layer)

	tile := tapalcatl.TileCoord{Z: 0, X: 0, Y: 0, Format: "zip"}
	key, err := storage.objectKey(tile)
	if err != nil {
		t.Fatalf("Unable to calculate key for tile: %s", err.Error())
	}
	if key != api.expectedKey {
		t.Fatalf("Unexpected key calculation. Expected %#v, got %#v.", api.expectedKey, key)
	}

	resp, err := storage.Get(tile, Condition{})
	if err != nil {
		t.Fatalf("Unable to Get tile from Mock S3: %s", err.Error())
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected 200 OK response from empty storage, but got %d", resp.StatusCode)
	}
	// check string header
	etag := resp.Header.Get("ETag")
	if etag != "1234" {
		t.Fatalf("Expected ETag to be \"1234\", but got %#v", etag)
	}
	// should be formatted in RFC 822 / 1123 format
	last_mod := resp.Header.Get("Last-Modified")
	exp_last_mod := "Thu, 17 Nov 2016 12:27:00 UTC"
	if last_mod != exp_last_mod {
		t.Fatalf("Expected Last-Modified to be %#v, but got %#v", exp_last_mod, last_mod)
	}
	// check integer header
	content_length_hdr := resp.Header.Get("Content-Length")
	content_length, err := strconv.ParseInt(content_length_hdr, 10, 64)
	if err != nil {
		t.Fatalf("Unable to parse Content-Length header %#v as int: %s", content_length_hdr, err.Error())
	}
	var exp_content_length int64 = 0
	if content_length != exp_content_length {
		t.Fatalf("Expected Content-Length to be %d, but was %d.", exp_content_length, content_length)
	}
}
