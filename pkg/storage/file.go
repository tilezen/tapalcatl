package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/tilezen/tapalcatl/pkg/cache"
	"github.com/tilezen/tapalcatl/pkg/state"
	"github.com/tilezen/tapalcatl/pkg/tile"
)

type FileStorage struct {
	baseDir     string
	layer       string
	healthcheck string
}

func NewFileStorage(baseDir string, tileCache cache.Cache, layer, healthcheck string) *FileStorage {
	return &FileStorage{
		baseDir:     baseDir,
		layer:       layer,
		healthcheck: healthcheck,
	}
}

func respondWithPath(path string) (*StorageResponse, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			resp := &StorageResponse{
				NotFound: true,
			}
			return resp, nil

		} else {
			return nil, err
		}
	} else {
		resp := &StorageResponse{
			Response: &SuccessfulResponse{
				Body: bytes,
			},
		}
		return resp, nil
	}
}

func (f *FileStorage) Fetch(t tile.TileCoord, c state.Condition, prefix string) (*StorageResponse, error) {
	tilepath := filepath.Join(f.baseDir, f.layer, filepath.FromSlash(t.FileName()))
	return respondWithPath(tilepath)
}

func (s *FileStorage) TileJson(f state.TileJsonFormat, c state.Condition, prefix string) (*StorageResponse, error) {
	dirpath := "tilejson"
	tileJsonExt := "json"
	filename := fmt.Sprintf("%s.%s", f.Name(), tileJsonExt)
	tilejsonPath := filepath.Join(s.baseDir, dirpath, filename)
	return respondWithPath(tilejsonPath)
}

func (s *FileStorage) HealthCheck() error {
	tilepath := filepath.Join(s.baseDir, s.healthcheck)
	f, err := os.Open(tilepath)
	if err != nil {
		err = f.Close()
	}
	return err
}
