package main

import (
	"bytes"
	"github.com/tilezen/tapalcatl"
	"io"
	"log"
	"net/http"
	"time"
)

type Parser interface {
	Parse(*http.Request) (tapalcatl.TileCoord, Condition, error)
}

func MetatileHandler(p Parser, metatile_size int, mime_type map[string]string, storage Getter, logger *log.Logger) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		numRequests.Add(1)
		start_time := time.Now()
		defer func() {
			updateCounters(time.Since(start_time))
		}()

		coord, cond, err := p.Parse(req)
		if err != nil {
			requestParseErrors.Add(1)
			logger.Printf("WARNING: Failed to parse request: %s", err.Error())
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}

		meta_coord, offset := coord.MetaAndOffset(metatile_size)

		storageResult, err := storage.Get(meta_coord, cond)
		if err != nil || storageResult.NotFound {
			if err != nil {
				storageGetErrors.Add(1)
				logger.Printf("WARNING: Failed metatile storage get: %s", err.Error())
			} else {
				numStorageMisses.Add(1)
			}
			http.NotFound(rw, req)
			return
		}
		numStorageHits.Add(1)
		if storageResult.NotModified {
			numStorageNotModified.Add(1)
			rw.WriteHeader(http.StatusNotModified)
			return
		}
		numStorageReads.Add(1)

		// metatile reader needs to be able to seek in the buffer and know its size. the easiest way to ensure that is to buffer the whole thing into memory.
		storageResp := storageResult.Response
		var buf bytes.Buffer
		bodySize, err := io.Copy(&buf, storageResp.Body)
		if err != nil {
			storageReadErrors.Add(1)
			logger.Printf("ERROR: Failed to read storage body: %s", err.Error())
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		storageBytes := buf.Bytes()

		headers := rw.Header()
		if mime, ok := mime_type[coord.Format]; ok {
			headers.Set("Content-Type", mime)
		}
		if lastMod := storageResp.LastModified; lastMod != nil {
			lastModifiedFormatted := lastMod.Format(time.RFC1123Z)
			headers.Set("Last-Modified", lastModifiedFormatted)
		}
		if etag := storageResp.ETag; etag != nil {
			headers.Set("ETag", *etag)
		}

		reader, err := tapalcatl.NewMetatileReader(offset, bytes.NewReader(storageBytes), bodySize)
		if err != nil {
			metatileReadErrors.Add(1)
			logger.Printf("ERROR: Failed to read metatile: %s", err.Error())
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		rw.WriteHeader(http.StatusOK)
		_, err = io.Copy(rw, reader)
		if err != nil {
			responseWriteErrors.Add(1)
			logger.Printf("ERROR: Failed to write response body: %s", err.Error())
		}
	})
}
