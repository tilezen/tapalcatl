package main

import (
	"encoding/json"
	"fmt"
	"github.com/NYTimes/gziphandler"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/namsral/flag"
	//	"github.com/oxtoacart/bpool"
	"github.com/tilezen/tapalcatl"
	"github.com/whosonfirst/go-httpony/stats"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

// the handler config is the container for the json configuration
// storageDefinition contains the base options for a particular storage
// storageConfig contains the specific options for a particular pattern
// pattern ties together request patterns with storageConfig
// the storageConfig "Type_" needs to match the key mapping names in Storage
// awsConfig contains session-wide options for aws backed storage

// "s3" and "file" are possible storage definitions

type storageDefinition struct {
	// common fields across all storage types
	// these can be overridden in specific storage configuration
	MetatileSize int

	// s3 specific fields
	Layer      string
	Bucket     string
	KeyPattern string

	// file specific fields
	BaseDir string
}

// generic aws configuration applied to whole session
type awsConfig struct {
	Region *string
}

// storage configuration, specific to a pattern
type storageConfig struct {
	// should match storage definition name, "s3" or "file"
	Type_ string `json:"type"`

	MetatileSize *int

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
	_, err = time.Parse(time.RFC1123, date)
	return nil, err
}

// MuxParser parses the tile coordinate from the captured arguments from
// the gorilla mux router.
type MuxParser struct{}

// Parse ignores its argument and uses values from the capture.
func (_ *MuxParser) Parse(req *http.Request) (t tapalcatl.TileCoord, c Condition, err error) {
	m := mux.Vars(req)

	t.Z, err = strconv.Atoi(m["z"])
	if err != nil {
		return
	}

	t.X, err = strconv.Atoi(m["x"])
	if err != nil {
		return
	}

	t.Y, err = strconv.Atoi(m["y"])
	if err != nil {
		return
	}

	t.Format = m["fmt"]

	if_modified_since := req.Header.Get("If-Modified-Since")
	if if_modified_since != "" {
		c.IfModifiedSince, err = parseHTTPDates(if_modified_since)
		if err != nil {
			return
		}
	}

	if_none_match := req.Header.Get("If-None-Match")
	if if_none_match != "" {
		c.IfNoneMatch = &if_none_match
	}

	return
}

func main() {
	var listen, healthcheck, debugHost string
	var poolSize, poolWidth int
	hc := handlerConfig{}

	// use this logger everywhere.
	logger := log.New(os.Stdout, "tapalcatl ", log.LstdFlags)

	f := flag.NewFlagSetWithEnvPrefix(os.Args[0], "TAPALCATL", 0)
	f.Var(&hc, "handler",
		`JSON object defining how request patterns will be handled.
	 Aws { Object present when Aws-wide configuration is needed, eg session config.
     Region string Name of aws region
   }
   Storage { key -> storage definition mapping
     storage name (type) string -> {
     	 MetatileSize int      Number of tiles in each dimension of the metatile.

       (s3 storage)
    	 Layer      string   Name of layer to use in this bucket. Only relevant for s3.
    	 Bucket     string   Name of S3 bucket to fetch from.
       KeyPattern string   Pattern to fill with variables from the main pattern to make the S3 key.

       (file storage)
       BaseDir    string   Base directory to look for files under.
     }
   }
   Pattern { request pattern -> storage configuration mapping
     request pattern string -> {
       type string Name of storage defintion to use
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
	f.StringVar(&healthcheck, "healthcheck", "", "A path to respond to with a blank 200 OK. Intended for use by load balancer health checks.")
	f.StringVar(&debugHost, "debugHost", "", "IP address of remote debug host allowed to read expvars at /debug/vars.")
	f.IntVar(&poolSize, "poolsize", 0, "Number of byte buffers to cache in pool between requests.")
	f.IntVar(&poolWidth, "poolwidth", 1, "Size of new byte buffers to create in the pool.")

	err := f.Parse(os.Args[1:])
	if err == flag.ErrHelp {
		return
	} else if err != nil {
		logger.Fatalf("Unable to parse input command line, environment or config: %s", err.Error())
	}

	if len(hc.Pattern) == 0 {
		logger.Fatalf("You must provide at least one pattern.")
	}
	if len(hc.Storage) == 0 {
		logger.Fatalf("You must provide at least one storage.")
	}

	r := mux.NewRouter()

	// set if we have s3 storage configured, and shared across all s3 sessions
	var awsSession *session.Session

	// create the storage implementations and handler routes for patterns
	var storage Storage
	for reqPattern, sc := range hc.Pattern {

		t := sc.Type_
		sd, ok := hc.Storage[t]
		if !ok {
			logger.Fatalf("Missing s3 storage definition: %s", t)
		}
		metatileSize := sd.MetatileSize
		if sc.MetatileSize != nil {
			metatileSize = *sc.MetatileSize
		}
		layer := sd.Layer
		if sc.Layer != nil {
			layer = *sc.Layer
		}

		switch t {
		case "s3":
			if sc.Prefix == nil {
				logger.Fatalf("S3 configuration requires prefix")
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
				logger.Fatalf("Unable to set up AWS session: %s", err.Error())
			}
			keyPattern := sd.KeyPattern
			if sc.KeyPattern != nil {
				keyPattern = *sc.KeyPattern
			}

			s3Client := s3.New(awsSession)
			storage = NewS3Storage(s3Client, sd.Bucket, keyPattern, prefix, layer)

		case "file":
			sd, ok := hc.Storage[t]
			if !ok {
				logger.Fatalf("Missing file storage definition")
			}
			storage = NewFileStorage(sd.BaseDir, layer)

		default:
			logger.Fatalf("Unknown storage %s", t)
		}

		parser := &MuxParser{}

		h := MetatileHandler(parser, metatileSize, hc.Mime, storage, logger)
		gzipped := gziphandler.GzipHandler(h)

		r.Handle(reqPattern, gzipped).Methods("GET")
	}

	// bufferPool := bpool.NewBytePool(poolSize, poolWidth)

	if len(healthcheck) > 0 {
		r.HandleFunc(healthcheck, getHealth).Methods("GET")
	}

	// serve expvar stats to localhost and debugHost
	expvar_func, err := stats.HandlerFunc(debugHost)
	if err != nil {
		logger.Fatalf("Error initializing stats.HandlerFunc: %s", err.Error())
	}
	r.HandleFunc("/debug/vars", expvar_func).Methods("GET")

	corsHandler := handlers.CORS()(r)
	logHandler := handlers.CombinedLoggingHandler(os.Stdout, corsHandler)

	logger.Printf("Server started and listening on %s\n", listen)

	logger.Fatal(http.ListenAndServe(listen, logHandler))
}

func getHealth(rw http.ResponseWriter, _ *http.Request) {
	rw.WriteHeader(200)
}
