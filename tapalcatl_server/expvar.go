package main

import (
	"expvar"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	requestParseErrors  *expvar.Int
	storageGetErrors    *expvar.Int
	storageReadErrors   *expvar.Int
	metatileReadErrors  *expvar.Int
	responseWriteErrors *expvar.Int

	numStorageMisses      *expvar.Int
	numStorageHits        *expvar.Int
	numStorageNotModified *expvar.Int
	numStorageReads       *expvar.Int

	numRequests *expvar.Int

	avgTotalTime *expvar.Float

	totalRequestTime int64
)

func init() {
	requestParseErrors = expvar.NewInt("requestParseErrors")
	storageGetErrors = expvar.NewInt("storageGetErrors")
	storageReadErrors = expvar.NewInt("storageReadErrors")
	metatileReadErrors = expvar.NewInt("metatileReadErrors")
	responseWriteErrors = expvar.NewInt("responseWriteErrors")

	numStorageMisses = expvar.NewInt("numStorageMisses")
	numStorageHits = expvar.NewInt("numStorageHits")
	numStorageNotModified = expvar.NewInt("numStorageNotModified")
	numStorageReads = expvar.NewInt("numStorageReads")

	numRequests = expvar.NewInt("numRequests")

	avgTotalTime = expvar.NewFloat("avgTotalTime")

	totalRequestTime = 0
}

func milliseconds(t time.Duration) int64 {
	ns := t.Nanoseconds()
	ms := ns / (int64(time.Millisecond) / int64(time.Nanosecond))
	return ms
}

func updateCounters(total time.Duration) {
	total_time := atomic.AddInt64(&totalRequestTime, milliseconds(total))

	req, err := strconv.ParseFloat(numRequests.String(), 64)
	if err != nil {
		return
	}

	avgTotalTime.Set(float64(total_time) / req)
}
