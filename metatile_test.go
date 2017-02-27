package tapalcatl

import (
	"archive/zip"
	"bytes"
	"fmt"
	"testing"
)

func makeTestZip(t *testing.T, tile TileCoord, content string) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	w := zip.NewWriter(buf)
	f, err := w.Create(tile.FileName())
	if err != nil {
		return nil, fmt.Errorf("Unable to create file %#v in zip: %s", tile.FileName(), err.Error())
	}
	_, err = f.Write([]byte("{}"))
	if err != nil {
		return nil, fmt.Errorf("Unable to write JSON file to zip: %s", err.Error())
	}
	err = w.Close()
	if err != nil {
		return nil, fmt.Errorf("Error while finalizing zip file: %s", err.Error())
	}
	return buf, nil
}

func TestReadZip(t *testing.T) {
	tile := TileCoord{Z: 0, X: 0, Y: 0, Format: "json"}
	json := "{}"
	buf, err := makeTestZip(t, tile, json)
	if err != nil {
		t.Fatalf("Unable to make test zip: %s", err.Error())
	}
	readerAt := bytes.NewReader(buf.Bytes())
	jsonReader, size, err := NewMetatileReader(tile, readerAt, int64(buf.Len()))
	if err != nil {
		t.Fatalf("Unable to read test zip: %s", err.Error())
	}
	if size <= 0 {
		t.Fatalf("Bad size in test zip: %d", size)
	}
	tileBuf := new(bytes.Buffer)
	tileBuf.ReadFrom(jsonReader)
	if tileBuf.String() != json {
		t.Fatalf("Expected tile content to be %#v, but was %#v.", json, tileBuf.String())
	}
}

func TestReadZipMissing(t *testing.T) {
	tile := TileCoord{Z: 0, X: 0, Y: 0, Format: "json"}
	otherTile := TileCoord{Z: 0, X: 1, Y: 0, Format: "json"}
	json := "{}"
	buf, err := makeTestZip(t, tile, json)
	if err != nil {
		t.Fatalf("Unable to make test zip: %s", err.Error())
	}
	readerAt := bytes.NewReader(buf.Bytes())
	_, _, err = NewMetatileReader(otherTile, readerAt, int64(buf.Len()))
	if err == nil {
		t.Fatalf("Expected not to find tile in zip, but no error was returned.")
	}
}

func coordEquals(t *testing.T, name string, exp, act TileCoord) {
	if exp.Z != act.Z {
		t.Fatalf("Expected %s Z to be %d but was %d.", name, exp.Z, act.Z)
	}
	if exp.X != act.X {
		t.Fatalf("Expected %s X to be %d but was %d.", name, exp.X, act.X)
	}
	if exp.Y != act.Y {
		t.Fatalf("Expected %s Y to be %d but was %d.", name, exp.Y, act.Y)
	}
	if exp.Format != act.Format {
		t.Fatalf("Expected %s Format to be %s but was %s.", name, exp.Format, act.Format)
	}
}

func checkMetaOffset(t *testing.T, size int, coord, exp_meta, exp_offset TileCoord) {
	meta, offset := coord.MetaAndOffset(size)
	coordEquals(t, "meta", exp_meta, meta)
	coordEquals(t, "offset", exp_offset, offset)
}

func TestMetaOffset(t *testing.T) {
	checkMetaOffset(t, 1,
		TileCoord{Z: 0, X: 0, Y: 0, Format: "json"},
		TileCoord{Z: 0, X: 0, Y: 0, Format: "zip"},
		TileCoord{Z: 0, X: 0, Y: 0, Format: "json"})

	checkMetaOffset(t, 1,
		TileCoord{Z: 12, X: 637, Y: 936, Format: "json"},
		TileCoord{Z: 12, X: 637, Y: 936, Format: "zip"},
		TileCoord{Z: 0, X: 0, Y: 0, Format: "json"})

	checkMetaOffset(t, 2,
		TileCoord{Z: 12, X: 637, Y: 936, Format: "json"},
		TileCoord{Z: 12, X: 636, Y: 936, Format: "zip"},
		TileCoord{Z: 0, X: 1, Y: 0, Format: "json"})

	checkMetaOffset(t, 8,
		TileCoord{Z: 12, X: 637, Y: 935, Format: "json"},
		TileCoord{Z: 12, X: 632, Y: 928, Format: "zip"},
		TileCoord{Z: 0, X: 5, Y: 7, Format: "json"})
}
