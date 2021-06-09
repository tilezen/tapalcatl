package handler

import (
	"bytes"
	"fmt"
	"net/http"
	"text/template"

	"github.com/tilezen/tapalcatl/pkg/log"
)

type fileHandler struct {
	// filename is the static html file name
	filename     string
	templateData map[string]interface{}
	logger       log.JsonLogger
}

func (f *fileHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	buildID := request.URL.Query().Get("buildid")

	tpl, err := template.ParseFiles(f.filename)
	if err != nil {
		errMsg := fmt.Sprintf("couldn't parse template file %s for handler: %v", f.filename, err)
		f.logger.Error(log.LogCategory_ConfigError, errMsg)
		http.Error(writer, errMsg, http.StatusInternalServerError)
	}

	renderedTemplateBuffer := bytes.NewBuffer(nil)
	f.templateData["buildid"] = buildID

	err = tpl.Execute(renderedTemplateBuffer, f.templateData)
	if err != nil {
		errMsg := fmt.Sprintf("couldn't render template file %s for handler: %v", f.filename, err)
		f.logger.Error(log.LogCategory_ConfigError, errMsg)
		http.Error(writer, errMsg, http.StatusInternalServerError)
	}

	writer.Write(renderedTemplateBuffer.Bytes())
}

// NewFileHandler parses the filename given as a text template and renders it with the given templateData.
// It returns an HTTP handler that serves the resulting rendered data to all requests.
func NewFileHandler(filename string, templateData map[string]interface{}) (http.Handler, error) {
	return &fileHandler{
		filename:     filename,
		templateData: templateData,
	}, nil
}
