package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/tilezen/tapalcatl"
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

		lastMod := new(time.Time)
		*lastMod = time.Date(2016, time.November, 17, 12, 27, 0, 0, time.UTC)

		obj := &s3.GetObjectOutput{
			Body:         &emptyReadCloser{},
			ETag:         etag,
			LastModified: lastMod,
		}
		return obj, nil

	} else {
		return nil, awserr.New("NoSuchKey", "The key was not found.", fmt.Errorf("Not Found."))
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
	if !resp.NotFound {
		t.Fatalf("Expected 404 response from empty storage")
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
	if resp.Response == nil {
		t.Fatalf("Expected successful response from empty storage")
	}
	etag := resp.Response.ETag
	if etag == nil {
		t.Fatalf("Expected etag from empty storage")
	}
	if *etag != "1234" {
		t.Fatalf("Expected ETag to be \"1234\", but got %#v", etag)
	}

	lastMod := resp.Response.LastModified
	if lastMod == nil {
		t.Fatalf("Missing last modified from storage")
	}
	// should be formatted in RFC 822 / 1123 format
	expLastModStr := "Thu, 17 Nov 2016 12:27:00 +0000"
	lastModStr := lastMod.Format(time.RFC1123Z)
	if expLastModStr != lastModStr {
		t.Fatalf("Expected Last-Modified to be %#v, but got %#v", expLastModStr, lastModStr)
	}
}
