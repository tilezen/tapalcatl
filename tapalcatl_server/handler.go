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

		resp.Header.Del("Content-Length")
		resp.Header.Set("Content-Type", mime_type[coord.Format])
		resp.Body = reader
		resp.WriteResponse(rw)
	})
}
