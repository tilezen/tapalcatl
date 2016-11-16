package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/tilezen/tapalcatl"
	"testing"
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

		obj := &s3.GetObjectOutput{
			Body: &emptyReadCloser{},
			ContentLength: length,
			ETag: etag,
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

	resp, err := storage.Get(tapalcatl.TileCoord{0, 0, 0, "zip"})
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

	tile := tapalcatl.TileCoord{0, 0, 0, "zip"}
	key, err := storage.objectKey(tile)
	if err != nil {
		t.Fatalf("Unable to calculate key for tile: %s", err.Error())
	}
	if key != api.expectedKey {
		t.Fatalf("Unexpected key calculation. Expected %#v, got %#v.", api.expectedKey, key)
	}

	resp, err := storage.Get(tile)
	if err != nil {
		t.Fatalf("Unable to Get tile from Mock S3: %s", err.Error())
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected 200 OK response from empty storage, but got %d", resp.StatusCode)
	}
	etag := resp.Header.Get("ETag")
	if etag != "1234" {
		t.Fatalf("Expected ETag to be \"1234\", but got %#v", etag)
	}
}
