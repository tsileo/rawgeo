package rawgeo

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/TomiHiltunen/geohash-golang"
	"github.com/cznic/kv"
)

// TODO(tsileo): implement remove

var (
	ErrNotFound       = errors.New("key does not exist")
	ErrMissingID      = errors.New("missing ID")
	ErrInvalidLatLong = errors.New("invalid lat/long")
)

var (
	indexKeyFmt = "%s:%s"   // {geohash}:{id}
	earthRadius = 6378137.0 // As defined by WGS 84
)

type Point struct {
	ID      string
	Geohash string

	// These fields are computed query time and will be ignored when indexing
	Distance float64
	Lat, Lng float64
}

func decodeLatLong(p *Point) {
	latlng := geohash.Decode(p.Geohash).Center()
	p.Lat, p.Lng = latlng.Lat(), latlng.Lng()
}

func NewPointFromGeohash(id, geohash string) *Point {
	p := &Point{
		ID:      id,
		Geohash: geohash,
	}
	decodeLatLong(p)
	return p
}

// Implements the equirectangular approximation from www.movable-type.co.uk/scripts/latlong.html
// (Pythagoras’ theorem on an equirectangular projection)
// var x = (λ2-λ1) * Math.cos((φ1+φ2)/2);
// var y = (φ2-φ1);
// var d = Math.sqrt(x*x + y*y) * R;
//
// Returns the approximate distance in meters
func (p *Point) DistanceFrom(point *Point) float64 {
	x := (point.Lng - p.Lng) * math.Pi / 180 * math.Cos((point.Lat+p.Lat)*math.Pi/360)
	y := (point.Lat - p.Lat) * math.Pi / 180
	return math.Sqrt(x*x+y*y) * earthRadius
}

type byDistance []*Point

func (s byDistance) Len() int           { return len(s) }
func (s byDistance) Less(i, j int) bool { return s[i].Distance < s[j].Distance }
func (s byDistance) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type RawGeo struct {
	db   *kv.DB
	path string
	mu   sync.Mutex
}

// New initializes/loads a `RawGeo` index at the given path
func New(path string) (*RawGeo, error) {
	createOpen := kv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	kvdb, err := createOpen(path, &kv.Options{})
	if err != nil {
		return nil, err
	}
	return &RawGeo{
		db:   kvdb,
		path: path,
	}, nil
}

// Close gracefully closes the opened index
func (rg *RawGeo) Close() error {
	return rg.db.Close()
}

// Destroy will try to remove the index file
func (rg *RawGeo) Destroy() error {
	if rg.path != "" {
		rg.Close()
		return os.RemoveAll(rg.path)
	}
	return nil
}

// Put index the given entry in the given index
// (you have to cared about duplicate IDs)
func (rg *RawGeo) Index(point *Point) error {
	// Ensure the entry contains a latitude and a longitude
	if point.Lat == 0 || point.Lng == 0 {
		return ErrInvalidLatLong
	}
	// Compute the Geohash if needed
	if point.Geohash == "" {
		point.Geohash = geohash.Encode(point.Lat, point.Lng)
	}
	if point.ID == "" {
		return ErrMissingID
	}
	if err := rg.db.Set([]byte(fmt.Sprintf(indexKeyFmt, point.Geohash, point.ID)), nil); err != nil {
		return err
	}
	return nil
}

// Query will returns all the points found sorted by distance to the query
func (rg *RawGeo) Query(lat, lng float64, precision int) ([]*Point, error) {
	// XXX(tsileo): make precision optional or
	// handle a radius parameters (radius to precision func needed)
	refPoint := &Point{
		Lat: lat,
		Lng: lng,
	}
	gh := geohash.EncodeWithPrecision(lat, lng, precision)

	res := []*Point{}

	// Search in the given geohash box, along with all the adjacent boxes
	for _, geohash := range append(geohash.CalculateAllAdjacent(gh), gh) {
		subres, err := rg.findPrefix(geohash)
		// fmt.Printf("adj=%s / %d\n", geohash, len(subres))
		if err != nil {
			return nil, err
		}
		for _, point := range subres {
			// Compute the distance from the query reference
			point.Distance = refPoint.DistanceFrom(point)
			res = append(res, point)
		}
	}
	sort.Sort(byDistance(res))
	return res, nil
}

func (rg *RawGeo) findPrefix(geoPrefix string) ([]*Point, error) {
	var limit int
	points := []*Point{}
	enum, _, err := rg.db.Seek([]byte(geoPrefix))
	if err != nil {
		return nil, err
	}
	// TODO(tsileo): check/fix the endBytes
	endBytes := []byte("\xff")
	i := 0
	for {
		k, _, err := enum.Next()
		sk := string(k)
		fmt.Printf("iter: k=%s err=%v\n", k, err)
		if err == io.EOF {
			break
		}
		if !strings.HasPrefix(sk, geoPrefix) {
			fmt.Printf("nope k=%s, start=%s\n", k, geoPrefix)
			break
		}
		if bytes.Compare(k, endBytes) > 0 || (limit != 0 && i > limit) {
			fmt.Printf("greater")
			return points, nil
		}
		parts := strings.SplitN(sk, ":", 2)
		p := NewPointFromGeohash(parts[1], parts[0])
		fmt.Printf("iter2=%+v\n", p)
		points = append(points, p)
		i++
	}
	return points, nil
}
