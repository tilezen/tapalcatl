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

// IsPowerOfTwo return true when the given integer is a power of two.
// See https://graphics.stanford.edu/~seander/bithacks.html#DetermineIfPowerOf2
// for details.
func IsPowerOfTwo(i int) bool {
	if i > 0 {
		return (i & (i - 1)) == 0
	}
	return false
}

// sizeToZoom returns the zoom equivalent to a metatile size, meaning that
// there are size * size tiles at this zoom level. Input size must be a
// power of two.
//
// Algorithm from: https://graphics.stanford.edu/~seander/bithacks.html#IntegerLog
// as Go seems to lack an integer Log2 in its math library.
func sizeToZoom(v uint) uint {
	var b = [...]uint{0x2, 0xC, 0xF0, 0xFF00, 0xFFFF0000}
	var S = [...]uint{1, 2, 3, 4, 16}
	var r uint = 0
	var i int

	for i = 4; i >= 0; i-- {
		if (v & b[i]) != 0 {
			v >>= S[i]
			r |= S[i]
		}
	}

	return r
}

// MetaAndOffset returns the metatile coordinate and the offset within it for
// this TileCoord object. The argument metaSize indicates the size of the
// metatile and tileSize indicates the size of the tile within the metatile
// that you want to extract, both in units of "standard" 256px tiles.
//
// For example, to extract a 1x1 regular 256px tile from a 2x2 metatile, one
// would call MetaAndOffset(2, 1). To extract the 512px tile from the same,
// call MetaAndOffset(2, 2).
func (t TileCoord) MetaAndOffset(metaSize, tileSize int) (meta, offset TileCoord, err error) {
	// check that sizes are powers of two before proceeding.
	if !IsPowerOfTwo(metaSize) {
		err = fmt.Errorf("Metatile size is required to be a power of two, but %d is not.", metaSize)
		return
	}
	if !IsPowerOfTwo(tileSize) {
		err = fmt.Errorf("Tile size is required to be a power of two, but %d is not.", tileSize)
		return
	}

	// now we can calculate the delta in zoom level, knowing that both must be
	// powers of two, and hence positive.
	metaZoom := sizeToZoom(uint(metaSize))
	tileZoom := sizeToZoom(uint(tileSize))
	if tileZoom > metaZoom {
		err = fmt.Errorf("Tile size must not be greater than metatile size, but %d > %d.", tileSize, metaSize)
		return
	}
	deltaZ := metaZoom - tileZoom

	// note that the uint->int conversion is technically a narrowing, but cannot
	// overflow because we know it contains the difference of two Log2s, which
	// cannot be larger than 32.
	iDeltaZ := int(deltaZ)

	// if the reduction in zoom due to the metatile size would take us "outside
	// the world" and leave meta.Z < 0, then we just clamp to zero.
	if t.Z < iDeltaZ {
		meta.Z = 0
		meta.X = 0
		meta.Y = 0
		meta.Format = "zip"

		offset.Z = 0
		offset.X = 0
		offset.Y = 0
		offset.Format = t.Format

	} else {
		meta.Z = t.Z - iDeltaZ
		meta.X = t.X >> deltaZ
		meta.Y = t.Y >> deltaZ
		meta.Format = "zip"

		offset.Z = t.Z - meta.Z
		offset.X = t.X - (meta.X << deltaZ)
		offset.Y = t.Y - (meta.Y << deltaZ)
		offset.Format = t.Format
	}

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
