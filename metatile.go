package tapalcatl

import (
	"archive/zip"
	"fmt"
	"io"
)

type TileCoord struct {
	Z, X, Y int
	Format  string
}

func (t TileCoord) FileName() string {
	return fmt.Sprintf("%d/%d/%d.%s", t.Z, t.X, t.Y, t.Format)
}

func (t TileCoord) MetaAndOffset(size int) (meta, offset TileCoord) {
	meta.Z = t.Z
	meta.X = size * (t.X / size)
	meta.Y = size * (t.Y / size)
	meta.Format = "zip"

	offset.Z = t.Z - meta.Z
	offset.X = t.X - meta.X
	offset.Y = t.Y - meta.Y
	offset.Format = t.Format

	return
}

func NewMetatileReader(t TileCoord, r io.ReaderAt, size int64) (io.ReadCloser, uint64, error) {
	z, err := zip.NewReader(r, size)
	if err != nil {
		return nil, 0, err
	}

	target := t.FileName()

	for _, f := range z.File {
		if f.Name == target {
			result, err := f.Open()
			return result, f.UncompressedSize64, err
		}
	}

	return nil, 0, fmt.Errorf("Unable to find relative tile offset %#v in metatile.", target)
}
