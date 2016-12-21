package main

import (
	"encoding/json"
	"fmt"
	"github.com/NYTimes/gziphandler"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/imkira/go-interpol"
	"github.com/namsral/flag"
	"github.com/oxtoacart/bpool"
	"github.com/tilezen/tapalcatl"
	"github.com/whosonfirst/go-httpony/stats"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
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
}

type fileConfig struct {
	BaseDir string
}

type proxyURL struct {
	url *url.URL
}

type patternConfig struct {
	S3           *s3Config   `json:"omitempty"`
	File         *fileConfig `json:"omitempty"`
	Layer        string
	ProxyURL     *proxyURL
	MetatileSize int
}

func (u *proxyURL) UnmarshalJSON(j []byte) error {
	var str string
	err := json.Unmarshal(j, &str)
	if err != nil {
		return err
	}
	local, err := url.Parse(str)
	if err != nil {
		return err
	}
	u.url = local
	return nil
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
	}
	File {                Object present when the storage should be from disk.
	  BaseDir    string   Base directory to look for files under.
	}
	Layer        string   Name of layer to use in this bucket.
	ProxyURL     url.URL  URL to proxy requests to. The path part is ignored.
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
		logger.Fatalf("You must provide at least one pattern to proxy.")
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
		var err error
		// start up the AWS config session. this is safe to share amongst request threads
		sess, err = session.NewSession()
		if err != nil {
			logger.Fatalf("Unable to set up AWS session: %s", err.Error())
		}
	}

	bufferPool := bpool.NewBytePool(poolSize, poolWidth)

	for pattern, cfg := range patterns.patterns {
		parser := &MuxParser{}

		var storage Getter
		if cfg.S3 != nil {
			storage = NewS3Storage(s3.New(sess), cfg.S3.Bucket, cfg.S3.KeyPattern, cfg.S3.Prefix, cfg.Layer)
		} else {
			storage = NewFileStorage(cfg.File.BaseDir, cfg.Layer)
		}

		proxy := &httputil.ReverseProxy{
			Director: func(req *http.Request) {
				// clear out most of the headers, particularly Host:
				// but we want to keep the if-modified-since and if-none-match
				new_headers := make(http.Header)
				copyHeader(new_headers, "If-Modified-Since", req.Header)
				copyHeader(new_headers, "If-None-Match", req.Header)
				copyHeader(new_headers, "X-Forwarded-For", req.Header)
				copyHeader(new_headers, "X-Real-IP", req.Header)
				req.Header = new_headers

				// overwrite scheme, user and host. leave query params and fragment as they are in the incoming request. interpolate path.
				req.URL.Scheme = cfg.ProxyURL.url.Scheme
				req.URL.Opaque = cfg.ProxyURL.url.Opaque
				req.URL.User = cfg.ProxyURL.url.User
				req.URL.Host = cfg.ProxyURL.url.Host
				req.Host = cfg.ProxyURL.url.Host

				var err error
				vars := mux.Vars(req)
				vars["layer"] = cfg.Layer
				req.URL.Path, err = interpol.WithMap(cfg.ProxyURL.url.Path, vars)
				if err != nil {
					// can't return error, can only log.
					logger.Printf("ERROR: Unable to interpolate string %#v with vars %#v: %s", cfg.ProxyURL.url.Path, vars, err.Error())
				}
			},
			ErrorLog:   logger,
			BufferPool: bufferPool,
		}

		h := MetatileHandler(parser, cfg.MetatileSize, mime_map.mimes, storage, proxy, logger)
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

	logger.Fatal(http.ListenAndServe(listen, logHandler))
}
