package handler

import (
	"net/http"

	"github.com/tilezen/tapalcatl/pkg/log"
	"github.com/tilezen/tapalcatl/pkg/storage"
)

func HealthCheckHandler(storages []storage.Storage, logger log.JsonLogger) http.Handler {

	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		healthy := true

		for _, storage := range storages {
			storageErr := storage.HealthCheck()

			if storageErr != nil {
				logger.Error(log.LogCategory_StorageError, "Healthcheck on storage %s failed: %s", storage, storageErr.Error())
				healthy = false
				break
			}
		}

		if healthy {
			rw.WriteHeader(http.StatusOK)
		} else {
			rw.WriteHeader(http.StatusInternalServerError)
		}
	})
}
