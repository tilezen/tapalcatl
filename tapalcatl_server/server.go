package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/NYTimes/gziphandler"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/namsral/flag"
	"github.com/oxtoacart/bpool"
	"github.com/tilezen/tapalcatl"
	"github.com/whosonfirst/go-httpony/stats"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"
)

// the handler config is the container for the json configuration
// storageDefinition contains the base options for a particular storage
// storageConfig contains the specific options for a particular pattern
// pattern ties together request patterns with storageConfig
// awsConfig contains session-wide options for aws backed storage

// "s3" and "file" are the possible storage definition types

// generic aws configuration applied to whole session
type awsConfig struct {
	Region *string
}

type storageDefinition struct {
	Type string

	// common fields across all storage types
	// these can be overridden in specific storage configuration
	MetatileSize int

	// TileSize indicates the size of tile for this pattern. The default is 1.
	TileSize *int

	// S3 key or file path to check for during healthcheck
	Healthcheck string

	// s3 specific fields
	Layer      string
	Bucket     string
	KeyPattern string

	// file specific fields
	BaseDir string
}

// storage configuration, specific to a pattern
type storageConfig struct {
	// matches storage definition name
	Storage string

	MetatileSize *int

	// TileSize indicates the size of tile for this pattern. The default is 1.
	TileSize *int

	// Prefix is required to be set for s3 storage
	Prefix     *string
	KeyPattern *string
	Layer      *string

	BaseDir *string
}

type handlerConfig struct {
	Aws     *awsConfig
	Storage map[string]storageDefinition
	Pattern map[string]storageConfig
	Mime    map[string]string
}

func (h *handlerConfig) String() string {
	return fmt.Sprintf("%#v", *h)
}

func (h *handlerConfig) Set(line string) error {
	err := json.Unmarshal([]byte(line), h)
	if err != nil {
		return fmt.Errorf("Unable to parse value as a JSON object: %s", err.Error())
	}
	return nil
}

// try and parse a range of different date formats which are allowed by HTTP.
func parseHTTPDates(date string) (*time.Time, error) {
	time_layouts := []string{
		http.TimeFormat,
		time.RFC1123, time.RFC1123Z,
		time.RFC822, time.RFC822Z,
		time.RFC850, time.ANSIC,
	}

	var err error
	var ts time.Time

	for _, layout := range time_layouts {
		ts, err = time.Parse(layout, date)
		if err == nil {
			return &ts, nil
		}
	}

	// give the error for our preferred format
	_, err = time.Parse(http.TimeFormat, date)
	return nil, err
}

// MuxParser parses the tile coordinate from the captured arguments from
// the gorilla mux router.
type MuxParser struct {
	mimeMap map[string]string
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

// Parse ignores its argument and uses values from the capture.
func (mp *MuxParser) Parse(req *http.Request) (*ParseResult, error) {
	m := mux.Vars(req)

	var t tapalcatl.TileCoord
	var contentType string
	var err error
	var ok bool

	var apiKey string
	q := req.URL.Query()
	if apiKeys, ok := q["api_key"]; ok && len(apiKeys) > 0 {
		apiKey = apiKeys[0]
	}

	parseResult := &ParseResult{
		HttpData: HttpRequestData{
			Path:      req.URL.Path,
			ApiKey:    apiKey,
			UserAgent: req.UserAgent(),
			Referrer:  req.Referer(),
		},
	}

	t.Format = m["fmt"]
	if contentType, ok = mp.mimeMap[t.Format]; !ok {
		return parseResult, &ParseError{
			MimeError: &MimeParseError{
				BadFormat: t.Format,
			},
		}
	}
	parseResult.ContentType = contentType
	parseResult.HttpData.Format = t.Format

	var coordError CoordParseError
	z := m["z"]
	t.Z, err = strconv.Atoi(z)
	if err != nil {
		coordError.BadZ = z
	}

	x := m["x"]
	t.X, err = strconv.Atoi(x)
	if err != nil {
		coordError.BadX = x
	}

	y := m["y"]
	t.Y, err = strconv.Atoi(y)
	if err != nil {
		coordError.BadY = y
	}

	if coordError.IsError() {
		return parseResult, &ParseError{
			CoordError: &coordError,
		}
	}

	parseResult.Coord = t

	ifNoneMatch := req.Header.Get("If-None-Match")
	if ifNoneMatch != "" {
		parseResult.Cond.IfNoneMatch = &ifNoneMatch
	}

	ifModifiedSince := req.Header.Get("If-Modified-Since")
	if ifModifiedSince != "" {
		parseResult.Cond.IfModifiedSince, err = parseHTTPDates(ifModifiedSince)
		if err != nil {
			return parseResult, &ParseError{
				CondError: &CondParseError{
					IfModifiedSinceError: err,
				},
			}
		}
	}

	return parseResult, nil
}

type OnDemandBufferManager struct{}

func (bm *OnDemandBufferManager) Get() *bytes.Buffer {
	return &bytes.Buffer{}
}

func (bm *OnDemandBufferManager) Put(buf *bytes.Buffer) {
}

func logFatalCfgErr(logger JsonLogger, msg string, xs ...interface{}) {
	logger.Error(LogCategory_ConfigError, msg, xs...)
	os.Exit(1)
}

func main() {
	var listen, healthcheck, debugHost string
	var poolNumEntries, poolEntrySize int
	var metricsStatsdAddr, metricsStatsdPrefix string
	var expVarsServe bool
	var expVarsLogIntervalSeconds int

	hc := handlerConfig{}

	systemLogger := log.New(os.Stdout, "", log.LstdFlags|log.LUTC|log.Lmicroseconds)
	hostname, err := os.Hostname()
	if err != nil {
		// NOTE: if there are legitimate cases when this can fail, we
		// can leave off the hostname in the logger.
		// But for now we prefer to get notified of it.
		systemLogger.Fatalf("ERROR: Cannot find hostname to use for logger")
	}
	// use this logger everywhere.
	logger := NewJsonLogger(systemLogger, hostname)

	f := flag.NewFlagSetWithEnvPrefix(os.Args[0], "TAPALCATL", 0)
	f.Var(&hc, "handler",
		`JSON object defining how request patterns will be handled.
	 Aws { Object present when Aws-wide configuration is needed, eg session config.
     Region string Name of aws region
   }
   Storage { key -> storage definition mapping
     storage name string -> {
        Type string storage type, can be "s3" or "file
        MetatileSize int      Number of 256px tiles in each dimension of the metatile.
        TileSize int        Size of tile in 256px tile units.

       (s3 storage)
        Layer      string   Name of layer to use in this bucket. Only relevant for s3.
        Bucket     string   Name of S3 bucket to fetch from.
        KeyPattern string   Pattern to fill with variables from the main pattern to make the S3 key.
        Healthcheck string Name of S3 key to use when querying health of S3 system.

       (file storage)
        BaseDir    string   Base directory to look for files under.
        Healthcheck string  Path to a file (inside BaseDir) when querying health of system.
     }
   }
   Pattern { request pattern -> storage configuration mapping
     request pattern string -> {
       storage string Name of storage defintion to use
       list of optional storage configuration to use:
       prefix is required for s3, others are optional overrides of relevant definition
     	 Prefix string  Prefix to use in this bucket.
     }
   }
   Mime { extension -> content-type used in http response
   }
`)
	f.StringVar(&listen, "listen", ":8080", "interface and port to listen on")
	f.String("config", "", "Config file to read values from.")
	f.StringVar(&healthcheck, "healthcheck", "", "A URL path for healthcheck. Intended for use by load balancer health checks.")
	f.StringVar(&debugHost, "debugHost", "", "IP address of remote debug host allowed to read expvars at /debug/vars.")

	f.IntVar(&poolNumEntries, "poolnumentries", 0, "Number of buffers to pool.")
	f.IntVar(&poolEntrySize, "poolentrysize", 0, "Size of each buffer in pool.")

	f.StringVar(&metricsStatsdAddr, "metrics-statsd-addr", "", "host:port to use to send data to statsd")
	f.StringVar(&metricsStatsdPrefix, "metrics-statsd-prefix", "", "prefix to prepend to metrics")

	f.BoolVar(&expVarsServe, "expvar-serve", false, "whether to serve expvars at /debug/vars")
	f.IntVar(&expVarsLogIntervalSeconds, "expvar-log-interval", 0, "seconds to log expvars, 0 disables")

	err = f.Parse(os.Args[1:])
	if err == flag.ErrHelp {
		return
	} else if err != nil {
		logFatalCfgErr(logger, "Unable to parse input command line, environment or config: %s", err.Error())
	}

	if len(hc.Pattern) == 0 {
		logFatalCfgErr(logger, "You must provide at least one pattern.")
	}
	if len(hc.Storage) == 0 {
		logFatalCfgErr(logger, "You must provide at least one storage.")
	}

	r := mux.NewRouter()

	// buffer manager shared by all handlers
	var bufferManager BufferManager

	if poolNumEntries > 0 && poolEntrySize > 0 {
		bufferManager = bpool.NewSizedBufferPool(poolNumEntries, poolEntrySize)
	} else {
		bufferManager = &OnDemandBufferManager{}
	}

	// metrics writer configuration
	var mw metricsWriter
	if metricsStatsdAddr != "" {
		udpAddr, err := net.ResolveUDPAddr("udp4", metricsStatsdAddr)
		if err != nil {
			logFatalCfgErr(logger, "Invalid metricsstatsdaddr %s: %s", metricsStatsdAddr, err)
		}
		mw = NewStatsdMetricsWriter(udpAddr, metricsStatsdPrefix, logger)
	} else {
		mw = &nilMetricsWriter{}
	}

	// set if we have s3 storage configured, and shared across all s3 sessions
	var awsSession *session.Session

	for _, sd := range hc.Storage {
		t := sd.Type
		switch t {
		case "s3":
		case "file":
		default:
			logFatalCfgErr(logger, "Unknown storage type: %s", t)
		}
	}

	// keep track of the storages so we can healthcheck them
	var storages []Storage

	// create the storage implementations and handler routes for patterns
	var storage Storage
	for reqPattern, sc := range hc.Pattern {

		storageDefinitionName := sc.Storage
		sd, ok := hc.Storage[storageDefinitionName]
		if !ok {
			logFatalCfgErr(logger, "Unknown storage definition: %s", storageDefinitionName)
		}
		metatileSize := sd.MetatileSize
		if sc.MetatileSize != nil {
			metatileSize = *sc.MetatileSize
		}
		if !tapalcatl.IsPowerOfTwo(metatileSize) {
			logFatalCfgErr(logger, "Metatile size must be power of two, but %d is not", metatileSize)
		}
		tileSize := 1
		if sd.TileSize != nil {
			tileSize = *sd.TileSize
		}
		if sc.TileSize != nil {
			tileSize = *sc.TileSize
		}
		if !tapalcatl.IsPowerOfTwo(tileSize) {
			logFatalCfgErr(logger, "Tile size must be power of two, but %d is not", tileSize)
		}
		layer := sd.Layer
		if sc.Layer != nil {
			layer = *sc.Layer
		}
		if layer == "" {
			logFatalCfgErr(logger, "Missing layer for storage: %s", storageDefinitionName)
		}

		switch sd.Type {
		case "s3":
			if sc.Prefix == nil {
				logFatalCfgErr(logger, "S3 configuration requires prefix")
			}
			prefix := *sc.Prefix

			if awsSession == nil {
				if hc.Aws != nil && hc.Aws.Region != nil {
					awsSession, err = session.NewSessionWithOptions(session.Options{
						Config: aws.Config{Region: hc.Aws.Region},
					})
				} else {
					awsSession, err = session.NewSession()
				}
			}
			if err != nil {
				logFatalCfgErr(logger, "Unable to set up AWS session: %s", err.Error())
			}
			keyPattern := sd.KeyPattern
			if sc.KeyPattern != nil {
				keyPattern = *sc.KeyPattern
			}

			if sd.Bucket == "" {
				logFatalCfgErr(logger, "S3 storage missing bucket configuration")
			}
			if keyPattern == "" {
				logFatalCfgErr(logger, "S3 storage missing key pattern")
			}

			if sd.Healthcheck == "" {
				logger.Warning(LogCategory_ConfigError, "Missing healthcheck for storage s3")
			}

			s3Client := s3.New(awsSession)
			storage = NewS3Storage(s3Client, sd.Bucket, keyPattern, prefix, layer, sd.Healthcheck)

		case "file":
			if sd.BaseDir == "" {
				logFatalCfgErr(logger, "File storage missing base dir")
			}

			if sd.Healthcheck == "" {
				logger.Warning(LogCategory_ConfigError, "Missing healthcheck for storage file")
			}

			storage = NewFileStorage(sd.BaseDir, layer, sd.Healthcheck)

		default:
			logFatalCfgErr(logger, "Unknown storage type: %s", sd.Type)
		}

		storage_err := storage.HealthCheck()
		if storage_err != nil {
			logger.Warning(LogCategory_ConfigError, "Healthcheck failed on storage: %s", storage_err)
		}

		storages = append(storages, storage)

		parser := &MuxParser{
			mimeMap: hc.Mime,
		}

		h := MetatileHandler(parser, metatileSize, tileSize, hc.Mime, storage, bufferManager, mw, logger)
		gzipped := gziphandler.GzipHandler(h)

		r.Handle(reqPattern, gzipped).Methods("GET")
	}

	if len(healthcheck) > 0 {
		hc := HealthCheckHandler(storages, logger)
		r.Handle(healthcheck, hc).Methods("GET")
	}

	if expVarsServe {
		// serve expvar stats to localhost and debugHost
		expvar_func, err := stats.HandlerFunc(debugHost)
		if err != nil {
			logFatalCfgErr(logger, "Failed to initialize stats.HandlerFunc: %s", err.Error())
		}
		r.HandleFunc("/debug/vars", expvar_func).Methods("GET")
	}

	if expVarsLogIntervalSeconds > 0 {
		// log the expvar stats periodically
		ticker := time.NewTicker(time.Second * time.Duration(expVarsLogIntervalSeconds))
		go func(c <-chan time.Time, l JsonLogger) {
			for _ = range c {
				logger.ExpVars()
			}
		}(ticker.C, logger)
	}

	corsHandler := handlers.CORS()(r)

	logger.Info("Server started and listening on %s\n", listen)

	systemLogger.Fatal(http.ListenAndServe(listen, corsHandler))
}
