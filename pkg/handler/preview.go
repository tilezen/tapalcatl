package handler

import (
	"bytes"
	"fmt"
	"net/http"
	"text/template"
)

type fileHandler struct {
	data *bytes.Buffer
}

func (f fileHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Write(f.data.Bytes())
}

// NewFileHandler parses the filename given as a text template and renders it with the given templateData.
// It returns an HTTP handler that serves the resulting rendered data to all requests.
func NewFileHandler(filename string, templateData map[string]interface{}) (http.Handler, error) {
	tpl, err := template.ParseFiles(filename)
	if err != nil {
		return nil, fmt.Errorf("couldn't parse template file %s for handler: %w", filename, err)
	}

	renderedTemplateBuffer := bytes.NewBuffer(nil)

	err = tpl.Execute(renderedTemplateBuffer, templateData)
	if err != nil {
		return nil, fmt.Errorf("couldn't render template file %s for handler: %w", filename, err)
	}

	handler := fileHandler{data: renderedTemplateBuffer}

	return handler, nil
}
