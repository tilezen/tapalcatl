package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/tilezen/tapalcatl/pkg/tile"
)

type FileStorage struct {
	baseDir     string
	layer       string
	healthcheck string
}

func NewFileStorage(baseDir, layer string, healthcheck string) *FileStorage {
	return &FileStorage{
		baseDir:     baseDir,
		layer:       layer,
		healthcheck: healthcheck,
	}
}

func respondWithPath(path string) (*StorageResponse, error) {
	file, err := os.Open(path)
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
				Body: file,
			},
		}
		return resp, nil
	}
}

func (f *FileStorage) Fetch(t tile.TileCoord, c Condition, prefix string) (*StorageResponse, error) {
	tilepath := filepath.Join(f.baseDir, f.layer, filepath.FromSlash(t.FileName()))
	return respondWithPath(tilepath)
}

func (s *FileStorage) TileJson(f TileJsonFormat, c Condition, prefix string) (*StorageResponse, error) {
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
