package main

import (
	"bytes"
	"github.com/tilezen/tapalcatl"
	"io"
	"net/http"
	"time"
)

type Parser interface {
	Parse(*http.Request) (tapalcatl.TileCoord, error)
}

func MetatileHandler(p Parser, metatile_size int, mime_type map[string]string, storage Getter, proxy http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		numRequests.Add(1)
		start_time := time.Now()
		defer func() {
			updateCounters(time.Since(start_time))
		}()

		coord, err := p.Parse(req)
		if err != nil {
			parseErrors.Add(1)
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}

		meta_coord, offset := coord.MetaAndOffset(metatile_size)

		resp, err := storage.Get(meta_coord)
		if err != nil || resp.StatusCode == 404 {
			proxiedRequests.Add(1)
			proxy.ServeHTTP(rw, req)
			return
		}

		// metatile reader needs to be able to seek in the buffer and know its size. the easiest way to ensure that is to buffer the whole thing into memory.
		var buf bytes.Buffer
		body_size, err := io.Copy(&buf, resp.Body)
		if err != nil {
			copyErrors.Add(1)
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		reader, err := tapalcatl.NewMetatileReader(offset, bytes.NewReader(buf.Bytes()), body_size)
		if err != nil {
			metatileErrors.Add(1)
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		// we don't want all the headers - some may refer only to things which were valid for the response from storage. we do want to keep the ETag / Last-Modified to preserve cache behaviour.
		new_headers := make(http.Header)
		keep_headers := []string{
			"ETag",
			"Last-Modified",
		}
		for _, key := range keep_headers {
			val := resp.Header.Get(key)
			if val != "" {
				new_headers.Set(key, val)
			}
		}
		if mime, ok := mime_type[coord.Format]; ok {
			new_headers.Set("Content-Type", mime)
		}

		// note: keep status code, perhaps it's a 304 Not Modified.
		resp.Header = new_headers
		resp.Body = reader
		resp.WriteResponse(rw)
	})
}
