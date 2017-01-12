package main

import (
	"archive/zip"
	"bytes"
	"fmt"
	"github.com/tilezen/tapalcatl"
	"io"
	"log"
	"net/http"
	"os"
	"testing"
	"time"
)

func makeTestZip(tile tapalcatl.TileCoord, content string) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	w := zip.NewWriter(buf)
	f, err := w.Create(tile.FileName())
	if err != nil {
		return nil, fmt.Errorf("Unable to create file %#v in zip: %s", tile.FileName(), err.Error())
	}
	_, err = f.Write([]byte("{}"))
	if err != nil {
		return nil, fmt.Errorf("Unable to write JSON file to zip: %s", err.Error())
	}
	err = w.Close()
	if err != nil {
		return nil, fmt.Errorf("Error while finalizing zip file: %s", err.Error())
	}
	return buf, nil
}

type fakeParser struct {
	tile tapalcatl.TileCoord
}

func (f *fakeParser) Parse(_ *http.Request) (*ParseResult, error) {
	result := &ParseResult{
		Coord:       f.tile,
		ContentType: "application/json",
	}
	return result, nil
}

type fakeStorage struct {
	storage map[tapalcatl.TileCoord]*StorageResponse
}

func (f *fakeStorage) Fetch(t tapalcatl.TileCoord, _ Condition) (*StorageResponse, error) {
	resp, ok := f.storage[t]
	if ok {
		return resp, nil
	} else {
		return &StorageResponse{NotFound: true}, nil
	}
}

type fakeResponseWriter struct {
	header http.Header
	status int
}

func (f *fakeResponseWriter) Header() http.Header {
	return f.header
}

func (f *fakeResponseWriter) Write(buf []byte) (int, error) {
	return len(buf), nil
}

func (f *fakeResponseWriter) WriteHeader(status int) {
	f.status = status
}

func TestHandlerMiss(t *testing.T) {
	tile := tapalcatl.TileCoord{Z: 0, X: 0, Y: 0, Format: "json"}
	parser := &fakeParser{tile: tile}
	mimes := map[string]string{
		"json": "application/json",
	}
	storage := &fakeStorage{storage: make(map[tapalcatl.TileCoord]*StorageResponse)}
	logger := log.New(os.Stdout, "TestHandlerHit", log.LstdFlags)
	h := MetatileHandler(parser, 1, mimes, storage, &OnDemandBufferManager{}, &nilMetricsWriter{}, logger)

	rw := &fakeResponseWriter{header: make(http.Header), status: 0}
	req := &http.Request{}
	h.ServeHTTP(rw, req)

	if rw.status != 404 {
		t.Fatalf("Expected 404 response, but got %d", rw.status)
	}
}

type emptyReadCloser struct{}

func (_ *emptyReadCloser) Read(_ []byte) (int, error) {
	return 0, io.EOF
}

func (_ *emptyReadCloser) Close() error {
	return nil
}

type bufferReadCloser struct {
	reader *bytes.Reader
}

func (b *bufferReadCloser) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

func (b *bufferReadCloser) Close() error {
	return nil
}

func TestHandlerHit(t *testing.T) {
	tile := tapalcatl.TileCoord{Z: 0, X: 0, Y: 0, Format: "json"}
	parser := &fakeParser{tile: tile}
	mimes := map[string]string{
		"json": "application/json",
	}
	storage := &fakeStorage{storage: make(map[tapalcatl.TileCoord]*StorageResponse)}

	metatile := tapalcatl.TileCoord{Z: 0, X: 0, Y: 0, Format: "zip"}
	zipfile, err := makeTestZip(tile, "{}")
	if err != nil {
		t.Fatalf("Unable to make test zip: %s", err.Error())
	}

	etag := "1234"
	lastModifiedStr := "Thu, 17 Nov 2016 12:27:00 +0000"
	lastModified, err := time.Parse(time.RFC1123Z, lastModifiedStr)
	if err != nil {
		t.Fatalf("Couldn't parse time %s: %s", lastModifiedStr, err)
	}
	storage.storage[metatile] = &StorageResponse{
		Response: &SuccessfulResponse{
			Body:         &bufferReadCloser{reader: bytes.NewReader(zipfile.Bytes())},
			LastModified: &lastModified,
			ETag:         &etag,
		},
	}

	logger := log.New(os.Stdout, "TestHandlerHit", log.LstdFlags)
	h := MetatileHandler(parser, 1, mimes, storage, &OnDemandBufferManager{}, &nilMetricsWriter{}, logger)

	rw := &fakeResponseWriter{header: make(http.Header), status: 0}
	req := &http.Request{}
	h.ServeHTTP(rw, req)

	if rw.status != 200 {
		t.Fatalf("Expected 200 OK response, but got %d", rw.status)
	}
	checkHeader := func(key, exp string) {
		act := rw.header.Get(key)
		if act != exp {
			t.Fatalf("Expected HTTP header %#v to be %#v but was %#v", key, exp, act)
		}
	}
	checkHeader("Content-Type", "application/json")
	checkHeader("ETag", etag)
	checkHeader("Last-Modified", lastModifiedStr)
	checkHeader("X-Mz-Ignore-Me", "")
}
