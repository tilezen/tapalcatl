package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// generic Response type to abstract across calls to S3 and upstream to the origin server over HTTP.
type Response struct {
	// HTTP-style status code.
	//
	// e.g: 404 means not found, 200 means OK.
	StatusCode int

	// header key-values associated with the response
	Header http.Header

	// content of the response
	Body io.ReadCloser
}

func (r *Response) WriteResponse(rw http.ResponseWriter) {
	for key, vals := range r.Header {
		rw.Header().Set(key, strings.Join(vals, ", "))
	}

	rw.WriteHeader(r.StatusCode)

	_, err := io.Copy(rw, r.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing body: %s", err.Error())
	}
}
