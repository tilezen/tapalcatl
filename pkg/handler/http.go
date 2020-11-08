package handler

import (
	"fmt"
	"net/http"
	"time"

	"github.com/tilezen/tapalcatl/pkg/state"
	"github.com/tilezen/tapalcatl/pkg/storage"
)

func ParseHttpData(req *http.Request) state.HttpRequestData {
	var apiKey string
	q := req.URL.Query()
	if apiKeys, ok := q["api_key"]; ok && len(apiKeys) > 0 {
		apiKey = apiKeys[0]
	}
	return state.HttpRequestData{
		Path:      req.URL.Path,
		ApiKey:    apiKey,
		UserAgent: req.UserAgent(),
		Referrer:  req.Referer(),
	}
}

// try and parse a range of different date formats which are allowed by HTTP.
func parseHTTPDates(date string) (*time.Time, error) {
	timeLayouts := []string{
		http.TimeFormat,
		time.RFC1123, time.RFC1123Z,
		time.RFC822, time.RFC822Z,
		time.RFC850, time.ANSIC,
	}

	var err error
	var ts time.Time

	for _, layout := range timeLayouts {
		ts, err = time.Parse(layout, date)
		if err == nil {
			return &ts, nil
		}
	}

	// give the error for our preferred format
	_, err = time.Parse(http.TimeFormat, date)
	return nil, err
}

func ParseCondition(req *http.Request) (storage.Condition, *CondParseError) {
	result := storage.Condition{}
	var err error
	ifNoneMatch := req.Header.Get("If-None-Match")
	if ifNoneMatch != "" {
		result.IfNoneMatch = &ifNoneMatch
	}

	ifModifiedSince := req.Header.Get("If-Modified-Since")
	if ifModifiedSince != "" {
		result.IfModifiedSince, err = parseHTTPDates(ifModifiedSince)
		if err != nil {
			return result, &CondParseError{IfModifiedSinceError: err}
		}
	}

	return result, nil
}

type CondParseError struct {
	IfModifiedSinceError error
}

func (cpe *CondParseError) Error() string {
	return cpe.IfModifiedSinceError.Error()
}

type ParseError struct {
	MimeError  *MimeParseError
	CoordError *CoordParseError
	CondError  *CondParseError
}

func (pe *ParseError) Error() string {
	if pe.MimeError != nil {
		return pe.MimeError.Error()
	} else if pe.CoordError != nil {
		return pe.CoordError.Error()
	} else if pe.CondError != nil {
		return pe.CondError.Error()
	} else {
		panic("ParseError: No error")
	}
}

type MimeParseError struct {
	BadFormat string
}

func (mpe *MimeParseError) Error() string {
	return fmt.Sprintf("Invalid format: %s", mpe.BadFormat)
}

type CoordParseError struct {
	// relevant values are set when parse fails
	BadZ string
	BadX string
	BadY string
}

func (cpe *CoordParseError) IsError() bool {
	return cpe.BadZ != "" || cpe.BadX != "" || cpe.BadY != ""
}

func (cpe *CoordParseError) Error() string {
	// TODO on multiple parse failures, can return back a concatenated string
	if cpe.BadZ != "" {
		return fmt.Sprintf("Invalid z: %s", cpe.BadZ)
	}
	if cpe.BadX != "" {
		return fmt.Sprintf("Invalid x: %s", cpe.BadX)
	}
	if cpe.BadY != "" {
		return fmt.Sprintf("Invalid y: %s", cpe.BadY)
	}
	panic("No coord parse error")
}
