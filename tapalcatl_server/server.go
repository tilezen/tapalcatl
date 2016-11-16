package main

import (
	"encoding/json"
	"fmt"
	"github.com/NYTimes/gziphandler"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
	"github.com/namsral/flag"
	"github.com/tilezen/tapalcatl"
	"github.com/whosonfirst/go-httpony/stats"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
)

func getHealth(rw http.ResponseWriter, _ *http.Request) {
	rw.WriteHeader(200)
}

type patternConfig struct {
	Bucket string
	KeyPattern string
	Prefix string
	Layer string
	ProxyURL url.URL
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

// MuxParser parses the tile coordinate from the captured arguments from
// the gorilla mux router.
type MuxParser struct{}

// Parse ignores its argument and uses values from the capture.
func (_ *MuxParser) Parse(req *http.Request) (t tapalcatl.TileCoord, err error) {
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
	return
}

func main() {
	var listen, healthcheck, debug_host string
	patterns := patternsOption{patterns: make(map[string]*patternConfig)}
	mime_map := mimeMapOption{mimes: make(map[string]string)}

	f := flag.NewFlagSetWithEnvPrefix(os.Args[0], "TAPALCATL", 0)
	f.Var(&patterns, "patterns", "JSON object of patterns to use when matching incoming tile requests.")
	f.StringVar(&listen, "listen", ":8080", "interface and port to listen on")
	f.String("config", "", "Config file to read values from.")
	f.StringVar(&healthcheck, "healthcheck", "", "A path to respond to with a blank 200 OK. Intended for use by load balancer health checks.")
	f.StringVar(&debug_host, "debugHost", "", "IP address of remote debug host allowed to read expvars at /debug/vars.")
	f.Var(&mime_map, "mime", "JSON object mapping file suffixes to MIME types.")
	err := f.Parse(os.Args[1:])
	if err == flag.ErrHelp {
		return
	} else if err != nil {
		log.Fatalf("Unable to parse input command line, environment or config: %s", err.Error())
	}

	if len(patterns.patterns) == 0 {
		log.Fatalf("You must provide at least one pattern to proxy.")
	}

	r := mux.NewRouter()

	// start up the AWS config session. this is safe to share amongst request threads
	sess, err := session.NewSession()
	if err != nil {
		log.Fatalf("Unable to set up AWS session: %s", err.Error())
	}

	for pattern, cfg := range patterns.patterns {
		parser := &MuxParser{}
		storage := NewS3Storage(s3.New(sess), cfg.Bucket, cfg.KeyPattern, cfg.Prefix, cfg.Layer)
		proxy := &httputil.ReverseProxy{
			Director: func(req *http.Request) {
				// overwrite scheme, user, host and path. leave path, query params and fragment as they are in the incoming request.
				req.URL.Scheme = cfg.ProxyURL.Scheme
				req.URL.Opaque = cfg.ProxyURL.Opaque
				req.URL.User = cfg.ProxyURL.User
				req.URL.Host = cfg.ProxyURL.Host
				req.URL.Path = cfg.ProxyURL.Path
			},
		}

		h := MetatileHandler(parser, cfg.MetatileSize, mime_map.mimes, storage, proxy)
		gzipped := gziphandler.GzipHandler(h)

		r.Handle(pattern, gzipped).Methods("GET")
	}

	if len(healthcheck) > 0 {
		r.HandleFunc(healthcheck, getHealth).Methods("GET")
	}

	// serve expvar stats to localhost and debugHost
	expvar_func, err := stats.HandlerFunc(debug_host)
	if err != nil {
		log.Fatalf("Error initializing stats.HandlerFunc: %s", err.Error())
	}
	r.HandleFunc("/debug/vars", expvar_func).Methods("GET")

	http.Handle("/", r)

	log.Fatal(http.ListenAndServe(listen, r))
}
