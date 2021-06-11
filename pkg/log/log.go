package log

import (
	"bytes"
	"encoding/json"
	"expvar"
	"fmt"
	"log"
)

// utilities for json logging

// all messages should be logged as json, which allows us to more
// effectively filter logs. The functions here will serve to faciltate
// that through the code, and help guide all messages to contain a
// similar shape.

type LogCategory int32

const (
	LogCategory_Nil LogCategory = iota
	LogCategory_InvalidCodeState
	LogCategory_ParseError
	LogCategory_ConditionError
	LogCategory_StorageError
	LogCategory_MetatileError
	LogCategory_ResponseError
	LogCategory_ConfigError
	LogCategory_Metrics
	LogCategory_ExpVars
	LogCategory_TileJson
)

func (lc LogCategory) String() string {
	switch lc {
	case LogCategory_Nil:
		return "nil"
	case LogCategory_InvalidCodeState:
		return "invalid_code_state"
	case LogCategory_ParseError:
		return "parse"
	case LogCategory_ConditionError:
		return "condition"
	case LogCategory_StorageError:
		return "storage"
	case LogCategory_MetatileError:
		return "metatile"
	case LogCategory_ResponseError:
		return "response"
	case LogCategory_ConfigError:
		return "config"
	case LogCategory_Metrics:
		return "metrics"
	case LogCategory_ExpVars:
		return "expvars"
	case LogCategory_TileJson:
		return "tilejson"
	}
	panic(fmt.Sprintf("Unknown json category: %d\n", int32(lc)))
}

type JsonLogger interface {
	// helpful for basic one liners
	Info(string, ...interface{})
	Warning(LogCategory, string, ...interface{})
	Error(LogCategory, string, ...interface{})

	// for logging metrics specifically
	Metrics(map[string]interface{})
	// for logging tilejson metrics
	TileJson(map[string]interface{})

	// for logging expvars specifically
	ExpVars()

	// allows adding more metadata, and will remain *mostly*
	// unperturbed, will add minimal supplemental metadata before logging
	Log(map[string]interface{}, ...interface{})
}

type JsonLoggerImpl struct {
	Hostname string
	Logger   *log.Logger
}

func (l *JsonLoggerImpl) Log(jsonMap map[string]interface{}, xs ...interface{}) {
	if _, ok := jsonMap["hostname"]; !ok {
		jsonMap["hostname"] = l.Hostname
	}
	// if there are args, interpolate into the "message"
	// that key is assumed to be the string that gets interpolated
	if len(xs) > 0 {
		if msgValue, ok := jsonMap["message"]; ok {
			if msgStr, ok := msgValue.(string); ok {
				jsonMap["message"] = fmt.Sprintf(msgStr, xs...)
			}
		}
	}
	jsonBytes, err := json.Marshal(jsonMap)
	if err != nil {
		panic("ERROR creating json")
	}
	jsonStr := string(jsonBytes)
	l.Logger.Printf(jsonStr)
}

func (l *JsonLoggerImpl) Info(msg string, xs ...interface{}) {
	l.Log(map[string]interface{}{
		"type":    "info",
		"message": msg,
	}, xs...)
}

func (l *JsonLoggerImpl) Warning(category LogCategory, msg string, xs ...interface{}) {
	l.Log(map[string]interface{}{
		"type":     "warning",
		"category": category.String(),
		"message":  msg,
	}, xs...)
}

func (l *JsonLoggerImpl) Error(category LogCategory, msg string, xs ...interface{}) {
	l.Log(map[string]interface{}{
		"type":     "error",
		"category": category.String(),
		"message":  msg,
	}, xs...)
}

func (l *JsonLoggerImpl) Metrics(metricsData map[string]interface{}) {
	metricsData["type"] = "info"
	metricsData["category"] = LogCategory_Metrics.String()
	l.Log(metricsData)
}

func (l *JsonLoggerImpl) TileJson(metricsData map[string]interface{}) {
	metricsData["type"] = "info"
	metricsData["category"] = LogCategory_TileJson.String()
	l.Log(metricsData)
}

func (l *JsonLoggerImpl) ExpVars() {

	// The issue here is that getting the value of the Vars returns back
	// the json encoded representation (eg strings have "" around them). So
	// if we stick that in a map and call json.Marshal on it, it'll escape
	// those. This is why we manually generate the json here, instead of
	// creating a map and calling Log on it like the other paths.

	var buffer bytes.Buffer
	buffer.WriteString("{")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if first {
			first = false
		} else {
			buffer.WriteString(",")
		}
		fmt.Fprintf(&buffer, "\"%s\":%s", kv.Key, kv.Value.String())
	})
	buffer.WriteString("}")
	l.Logger.Printf("{\"type\":\"info\",\"category\":\"%s\",\"expvars\":%s}\n", LogCategory_ExpVars.String(), buffer.String())
}

func NewJsonLogger(logger *log.Logger, hostname string) JsonLogger {
	return &JsonLoggerImpl{
		Logger:   logger,
		Hostname: hostname,
	}
}

type NilJsonLogger struct{}

func (_ *NilJsonLogger) Log(_ map[string]interface{}, _ ...interface{})    {}
func (_ *NilJsonLogger) Info(_ string, _ ...interface{})                   {}
func (_ *NilJsonLogger) Warning(_ LogCategory, _ string, _ ...interface{}) {}
func (_ *NilJsonLogger) Error(_ LogCategory, _ string, _ ...interface{})   {}
func (_ *NilJsonLogger) Metrics(_ map[string]interface{})                  {}
func (_ *NilJsonLogger) TileJson(_ map[string]interface{})                 {}
func (_ *NilJsonLogger) ExpVars()                                          {}
