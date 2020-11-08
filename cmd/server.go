package main

import (
	golog "log"
	"net"
	"net/http"
	"os"

	"github.com/NYTimes/gziphandler"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/namsral/flag"
	"github.com/oxtoacart/bpool"

	"github.com/tilezen/tapalcatl/pkg/buffer"
	"github.com/tilezen/tapalcatl/pkg/config"
	"github.com/tilezen/tapalcatl/pkg/handler"
	"github.com/tilezen/tapalcatl/pkg/log"
	"github.com/tilezen/tapalcatl/pkg/metrics"
	"github.com/tilezen/tapalcatl/pkg/storage"
	"github.com/tilezen/tapalcatl/pkg/tile"
)

func main() {
	var listen, healthcheck string
	var poolNumEntries, poolEntrySize int
	var metricsStatsdAddr, metricsStatsdPrefix string

	hc := config.HandlerConfig{}

	systemLogger := golog.New(os.Stdout, "", golog.LstdFlags|golog.LUTC|golog.Lmicroseconds)
	hostname, err := os.Hostname()
	if err != nil {
		// NOTE: if there are legitimate cases when this can fail, we
		// can leave off the hostname in the logger.
		// But for now we prefer to get notified of it.
		systemLogger.Fatalf("ERROR: Cannot find hostname to use for logger")
	}
	// use this logger everywhere.
	logger := log.NewJsonLogger(systemLogger, hostname)

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

	f.IntVar(&poolNumEntries, "poolnumentries", 0, "Number of buffers to pool.")
	f.IntVar(&poolEntrySize, "poolentrysize", 0, "Size of each buffer in pool.")

	f.StringVar(&metricsStatsdAddr, "metrics-statsd-addr", "", "host:port to use to send data to statsd")
	f.StringVar(&metricsStatsdPrefix, "metrics-statsd-prefix", "", "prefix to prepend to metrics")

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
	var bufferManager buffer.BufferManager

	if poolNumEntries > 0 && poolEntrySize > 0 {
		bufferManager = bpool.NewSizedBufferPool(poolNumEntries, poolEntrySize)
	} else {
		bufferManager = &buffer.OnDemandBufferManager{}
	}

	// metrics writer configuration
	var mw metrics.MetricsWriter
	if metricsStatsdAddr != "" {
		udpAddr, err := net.ResolveUDPAddr("udp4", metricsStatsdAddr)
		if err != nil {
			logFatalCfgErr(logger, "Invalid metricsstatsdaddr %s: %s", metricsStatsdAddr, err)
		}
		mw = metrics.NewStatsdMetricsWriter(udpAddr, metricsStatsdPrefix, logger)
	} else {
		mw = &metrics.NilMetricsWriter{}
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
	// we only need to check unique type/healthcheck configurations
	healthCheckStorages := make(map[config.HealthCheckConfig]storage.Storage)

	// create the storage implementations and handler routes for patterns
	var stg storage.Storage
	for reqPattern, rhc := range hc.Pattern {

		storageDefinitionName := rhc.Storage
		sd, ok := hc.Storage[storageDefinitionName]
		if !ok {
			logFatalCfgErr(logger, "Unknown storage definition: %s", storageDefinitionName)
		}
		metatileSize := sd.MetatileSize
		if rhc.MetatileSize != nil {
			metatileSize = *rhc.MetatileSize
		}
		if !tile.IsPowerOfTwo(metatileSize) {
			logFatalCfgErr(logger, "Metatile size must be power of two, but %d is not", metatileSize)
		}
		tileSize := 1
		if sd.TileSize != nil {
			tileSize = *sd.TileSize
		}
		if rhc.TileSize != nil {
			tileSize = *rhc.TileSize
		}
		if !tile.IsPowerOfTwo(tileSize) {
			logFatalCfgErr(logger, "Tile size must be power of two, but %d is not", tileSize)
		}
		layer := sd.Layer
		if rhc.Layer != nil {
			layer = *rhc.Layer
		}
		if layer == "" {
			logFatalCfgErr(logger, "Missing layer for storage: %s", storageDefinitionName)
		}

		var healthcheck string

		switch sd.Type {
		case "s3":
			if rhc.Prefix == nil {
				logFatalCfgErr(logger, "S3 configuration requires prefix")
			}
			prefix := *rhc.Prefix

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
			if rhc.KeyPattern != nil {
				keyPattern = *rhc.KeyPattern
			}

			if sd.Bucket == "" {
				logFatalCfgErr(logger, "S3 storage missing bucket configuration")
			}
			if keyPattern == "" {
				logFatalCfgErr(logger, "S3 storage missing key pattern")
			}

			if sd.Healthcheck == "" {
				logger.Warning(log.LogCategory_ConfigError, "Missing healthcheck for storage s3")
			}

			s3Client := s3.New(awsSession)
			healthcheck = sd.Healthcheck
			stg = storage.NewS3Storage(s3Client, sd.Bucket, keyPattern, prefix, layer, healthcheck)

		case "file":
			if sd.BaseDir == "" {
				logFatalCfgErr(logger, "File storage missing base dir")
			}

			if sd.Healthcheck == "" {
				logger.Warning(log.LogCategory_ConfigError, "Missing healthcheck for storage file")
			}

			healthcheck = sd.Healthcheck
			stg = storage.NewFileStorage(sd.BaseDir, layer, healthcheck)

		default:
			logFatalCfgErr(logger, "Unknown storage type: %s", sd.Type)
		}

		if healthcheck != "" {
			storageErr := stg.HealthCheck()
			if storageErr != nil {
				logger.Warning(log.LogCategory_ConfigError, "Healthcheck failed on storage: %s", storageErr)
			}

			hcc := config.HealthCheckConfig{
				Type:        sd.Type,
				Healthcheck: healthcheck,
			}

			if _, ok := healthCheckStorages[hcc]; !ok {
				healthCheckStorages[hcc] = stg
			}
		}

		if rhc.Type == nil || *rhc.Type == "metatile" {
			parser := &handler.MetatileMuxParser{
				MimeMap: hc.Mime,
			}

			h := handler.MetatileHandler(parser, metatileSize, tileSize, hc.Mime, stg, bufferManager, mw, logger)
			gzipped := gziphandler.GzipHandler(h)

			r.Handle(reqPattern, gzipped).Methods("GET")

		} else if rhc.Type != nil && *rhc.Type == "tilejson" {
			parser := &handler.TileJsonParser{}
			h := handler.TileJsonHandler(parser, stg, mw, logger)
			gzipped := gziphandler.GzipHandler(h)
			r.Handle(reqPattern, gzipped).Methods("GET")
		} else {
			systemLogger.Fatalf("ERROR: Invalid route handler type: %s\n", *rhc.Type)
		}

	}

	if len(healthcheck) > 0 {
		storagesToCheck := make([]storage.Storage, len(healthCheckStorages))
		i := 0
		for _, storage := range healthCheckStorages {
			storagesToCheck[i] = storage
			i++
		}
		hc := handler.HealthCheckHandler(storagesToCheck, logger)
		r.Handle(healthcheck, hc).Methods("GET")
	}

	corsHandler := handlers.CORS()(r)

	logger.Info("Server started and listening on %s\n", listen)

	systemLogger.Fatal(http.ListenAndServe(listen, corsHandler))
}

func logFatalCfgErr(logger log.JsonLogger, msg string, xs ...interface{}) {
	logger.Error(log.LogCategory_ConfigError, msg, xs...)
	os.Exit(1)
}
