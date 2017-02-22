package main

import (
	"encoding/json"
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
	jsonBytes, err := json.Marshal(jsonMap)
	if err != nil {
		panic("ERROR creating json")
	}
	jsonStr := string(jsonBytes)
	l.Logger.Printf(jsonStr, xs...)
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

func NewJsonLogger(logger *log.Logger, hostname string) JsonLogger {
	return &JsonLoggerImpl{
		Logger:   logger,
		Hostname: hostname,
	}
}
