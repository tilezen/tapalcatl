package main

import (
	"expvar"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	parseErrors    *expvar.Int
	copyErrors     *expvar.Int
	metatileErrors *expvar.Int

	numRequests     *expvar.Int
	proxiedRequests *expvar.Int

	avgTotalTime    *expvar.Float

	totalRequestTime    int64
)

func init() {
	parseErrors = expvar.NewInt("parseErrors")
	copyErrors = expvar.NewInt("copyErrors")
	metatileErrors = expvar.NewInt("metatileErrors")

	numRequests = expvar.NewInt("numRequests")
	proxiedRequests = expvar.NewInt("proxiedRequests")

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
