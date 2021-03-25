package handler

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
)

type fileHandler struct {
	data *bytes.Buffer
}

func (f fileHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Write(f.data.Bytes())
}

func NewFileHandler(filename string) (http.Handler, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("couldn't open file %s for handler: %w", filename, err)
	}

	d := bytes.NewBuffer(nil)

	_, err = io.Copy(d, f)
	if err != nil {
		return nil, fmt.Errorf("couldn't read file %s for handler: %w", filename, err)
	}

	err = f.Close()
	if err != nil {
		return nil, fmt.Errorf("couldn't close file %s after reading for handler: %w", filename, err)
	}

	handler := fileHandler{data: d}

	return handler, nil
}
