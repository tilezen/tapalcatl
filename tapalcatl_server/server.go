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

func getHealth(rw http.ResponseWriter, _ *http.Request) {
	rw.WriteHeader(200)
}

type s3Config struct {
	Bucket     string
	KeyPattern string
	Prefix     string
	Region     *string
}

type fileConfig struct {
	BaseDir string
}

type patternConfig struct {
	S3           *s3Config
	File         *fileConfig
	Layer        string
	MetatileSize int
}

type patternsOption struct {
	patterns map[string]*patternConfig
}

func (p *patternsOption) String() string {
	return fmt.Sprintf("%#v", p.patterns)
}

func (p *patternsOption) Set(line string) error {
	err := json.Unmarshal([]byte(line), &p.patterns)
	if err != nil {
		return fmt.Errorf("Unable to parse value as a JSON object: %s", err.Error())
	}
	return nil
}

type mimeMapOption struct {
	mimes map[string]string
}

func (m *mimeMapOption) String() string {
	return fmt.Sprintf("%#v", m.mimes)
}

func (m *mimeMapOption) Set(line string) error {
	err := json.Unmarshal([]byte(line), &m.mimes)
	if err != nil {
		return fmt.Errorf("Unable to parse JSON MIMEs map from string: %s", err.Error())
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

func copyHeader(new_headers http.Header, key string, old_headers http.Header) {
	val := old_headers.Get(key)
	if val != "" {
		new_headers.Set(key, val)
	}
}

func main() {
	var listen, healthcheck, debug_host string
	var poolSize, poolWidth int
	patterns := patternsOption{patterns: make(map[string]*patternConfig)}
	mime_map := mimeMapOption{mimes: make(map[string]string)}

	// use this logger everywhere.
	logger := log.New(os.Stdout, "tapalcatl", log.LstdFlags)

	f := flag.NewFlagSetWithEnvPrefix(os.Args[0], "TAPALCATL", 0)
	f.Var(&patterns, "patterns",
		`JSON object of patterns to use when matching incoming tile requests, each pattern should map to an object containing:
	S3 {                  Object present when the storage should be from S3.
	  Bucket     string   Name of S3 bucket to fetch from.
	  KeyPattern string   Pattern to fill with variables from the main pattern to make the S3 key.
	  Prefix     string   Prefix to use in this bucket.
	  Region     string   AWS region to connect to.
	}
	File {                Object present when the storage should be from disk.
	  BaseDir    string   Base directory to look for files under.
	}
	Layer        string   Name of layer to use in this bucket.
	MetatileSize int      Number of tiles in each dimension of the metatile.
`)
	f.StringVar(&listen, "listen", ":8080", "interface and port to listen on")
	f.String("config", "", "Config file to read values from.")
	f.StringVar(&healthcheck, "healthcheck", "", "A path to respond to with a blank 200 OK. Intended for use by load balancer health checks.")
	f.StringVar(&debug_host, "debugHost", "", "IP address of remote debug host allowed to read expvars at /debug/vars.")
	f.Var(&mime_map, "mime", "JSON object mapping file suffixes to MIME types.")
	f.IntVar(&poolSize, "poolsize", 0, "Number of byte buffers to cache in pool between requests.")
	f.IntVar(&poolWidth, "poolwidth", 1, "Size of new byte buffers to create in the pool.")

	err := f.Parse(os.Args[1:])
	if err == flag.ErrHelp {
		return
	} else if err != nil {
		logger.Fatalf("Unable to parse input command line, environment or config: %s", err.Error())
	}

	if len(patterns.patterns) == 0 {
		logger.Fatalf("You must provide at least one pattern.")
	}

	r := mux.NewRouter()

	needS3 := false
	for pattern, cfg := range patterns.patterns {
		if cfg.S3 != nil {
			if cfg.File != nil {
				logger.Fatalf("Only one storage may be configured for each Pattern, but %#v has both S3 and File.", pattern)
			}
			needS3 = true
		} else if cfg.File == nil {
			logger.Fatalf("No storage configured for pattern %#v.", pattern)
		}
	}

	var sess *session.Session
	if needS3 {

		// ensure that the region configured is the same across all s3 configuration
		var region *string
		for _, cfg := range patterns.patterns {
			if cfg.S3 != nil && cfg.S3.Region != nil {
				if region != nil && *region != *cfg.S3.Region {
					logger.Fatalf("Multiple s3 regions configured: %s and %s", *region, *cfg.S3.Region)
				}
				region = cfg.S3.Region
			}
		}

		// start up the AWS config session. this is safe to share amongst request threads
		if region == nil {
			sess, err = session.NewSession()
		} else {
			sess, err = session.NewSessionWithOptions(session.Options{
				Config: aws.Config{Region: region},
			})
		}
		if err != nil {
			logger.Fatalf("Unable to set up AWS session: %s", err.Error())
		}
	}

	// bufferPool := bpool.NewBytePool(poolSize, poolWidth)

	for pattern, cfg := range patterns.patterns {
		parser := &MuxParser{}

		var storage Getter
		if cfg.S3 != nil {
			storage = NewS3Storage(s3.New(sess), cfg.S3.Bucket, cfg.S3.KeyPattern, cfg.S3.Prefix, cfg.Layer)
		} else {
			storage = NewFileStorage(cfg.File.BaseDir, cfg.Layer)
		}

		h := MetatileHandler(parser, cfg.MetatileSize, mime_map.mimes, storage, logger)
		gzipped := gziphandler.GzipHandler(h)

		r.Handle(pattern, gzipped).Methods("GET")
	}

	if len(healthcheck) > 0 {
		r.HandleFunc(healthcheck, getHealth).Methods("GET")
	}

	// serve expvar stats to localhost and debugHost
	expvar_func, err := stats.HandlerFunc(debug_host)
	if err != nil {
		logger.Fatalf("Error initializing stats.HandlerFunc: %s", err.Error())
	}
	r.HandleFunc("/debug/vars", expvar_func).Methods("GET")

	corsHandler := handlers.CORS()(r)
	logHandler := handlers.CombinedLoggingHandler(os.Stdout, corsHandler)

	logger.Printf("Server started and listening on %s\n", listen)

	logger.Fatal(http.ListenAndServe(listen, logHandler))
}
