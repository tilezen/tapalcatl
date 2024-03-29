package handler

import (
	"archive/zip"
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/tilezen/tapalcatl/pkg/buffer"
	"github.com/tilezen/tapalcatl/pkg/cache"
	"github.com/tilezen/tapalcatl/pkg/log"
	"github.com/tilezen/tapalcatl/pkg/metrics"
	"github.com/tilezen/tapalcatl/pkg/state"
	"github.com/tilezen/tapalcatl/pkg/storage"
	"github.com/tilezen/tapalcatl/pkg/tile"
)

func makeTestZip(tile tile.TileCoord, content string) (*bytes.Buffer, error) {
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
	tile tile.TileCoord
}

func (f *fakeParser) Parse(_ *http.Request) (*state.ParseResult, error) {
	result := &state.ParseResult{
		AdditionalData: &state.MetatileParseData{Coord: f.tile},
		ContentType:    "application/json",
	}
	return result, nil
}

type fakeStorage struct {
	storage map[tile.TileCoord]*storage.StorageResponse
}

func (f *fakeStorage) Fetch(t tile.TileCoord, _ state.Condition, prefix string) (*storage.StorageResponse, error) {
	resp, ok := f.storage[t]
	if ok {
		return resp, nil
	} else {
		return &storage.StorageResponse{NotFound: true}, nil
	}
}

func (f *fakeStorage) HealthCheck() error {
	return nil
}

func (f *fakeStorage) TileJson(fmt state.TileJsonFormat, c state.Condition, prefix string) (*storage.StorageResponse, error) {
	return nil, nil
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
	theTile := tile.TileCoord{Z: 0, X: 0, Y: 0, Format: "json"}
	parser := &fakeParser{tile: theTile}
	storage := &fakeStorage{storage: make(map[tile.TileCoord]*storage.StorageResponse)}
	h := MetatileHandler(parser, 1, 1, 0, storage, &buffer.OnDemandBufferManager{}, &metrics.NilMetricsWriter{}, &log.NilJsonLogger{}, cache.NilCache)

	rw := &fakeResponseWriter{header: make(http.Header), status: 0}
	req := &http.Request{
		URL: &url.URL{
			Path: "tile?buildid=20210331",
		},
	}
	h.ServeHTTP(rw, req)

	if rw.status != 404 {
		t.Fatalf("Expected 404 response, but got %d", rw.status)
	}
}

func TestHandlerHit(t *testing.T) {
	theTile := tile.TileCoord{Z: 0, X: 0, Y: 0, Format: "json"}
	parser := &fakeParser{tile: theTile}
	stg := &fakeStorage{storage: make(map[tile.TileCoord]*storage.StorageResponse)}

	metatile := tile.TileCoord{Z: 0, X: 0, Y: 0, Format: "zip"}
	zipfile, err := makeTestZip(theTile, "{}")
	if err != nil {
		t.Fatalf("Unable to make test zip: %s", err.Error())
	}

	etag := "1234"
	lastModifiedStr := "Thu, 17 Nov 2016 12:27:00 GMT"
	lastModified, err := time.Parse(http.TimeFormat, lastModifiedStr)
	if err != nil {
		t.Fatalf("Couldn't parse time %s: %s", lastModifiedStr, err)
	}
	stg.storage[metatile] = &storage.StorageResponse{
		Response: &storage.SuccessfulResponse{
			Body:         zipfile.Bytes(),
			LastModified: &lastModified,
			ETag:         &etag,
		},
	}

	h := MetatileHandler(parser, 1, 1, 0, stg, &buffer.OnDemandBufferManager{}, &metrics.NilMetricsWriter{}, &log.NilJsonLogger{}, cache.NilCache)

	rw := &fakeResponseWriter{header: make(http.Header), status: 0}
	req := &http.Request{
		URL: &url.URL{
			Path: "tile?buildid=20210331",
		},
	}
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
