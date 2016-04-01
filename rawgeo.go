package rawgeo

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/TomiHiltunen/geohash-golang"
	"github.com/cznic/kv"
	log "gopkg.in/inconshreveable/log15.v2"
)

// TODO(tsileo): implement remove

var (
	Log               = log.New()
	ErrNotFound       = errors.New("key does not exist")
	ErrMissingID      = errors.New("missing ID")
	ErrInvalidLatLong = errors.New("invalid lat/long")
)

var (
	indexKeyFmt = "%s:%s"   // {geohash}:{id}
	earthRadius = 6378137.0 // As defined by WGS 84
)

func init() {
	Log.SetHandler(log.DiscardHandler())
}

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
	log  log.Logger
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
		log:  Log.New("path", filepath.Base(path)),
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
	rg.log.Debug("indexed new point", "id", point.ID, "lat", point.Lat, "lng", point.Lng, "geohash", point.Geohash)
	return nil
}

// Query will returns all the points found in the given radius (in meters) sorted by distance to the query
func (rg *RawGeo) Query(lat, lng, radius float64) ([]*Point, error) {
	start := time.Now()
	qlog := rg.log.New("lat", lat, "lng", lng, "radius", radius)
	qlog.Debug("new query")
	refPoint := &Point{
		Lat: lat,
		Lng: lng,
	}
	precision := radiusToPrecision(radius)
	qlog.Debug("converted radius to precision", "radius", radius, "precision", precision)
	gh := geohash.EncodeWithPrecision(lat, lng, precision)
	qlog.Debug("query geohash", "geohash", gh)

	res := []*Point{}

	// Search in the given geohash box, along with all the 8 adjacent boxes
	for _, geohash := range append(geohash.CalculateAllAdjacent(gh), gh) {
		subres, err := rg.findPrefix(geohash)
		qlog.Debug("new subquery", "geohash", geohash, "cnt", len(subres))
		if err != nil {
			return nil, err
		}
		for _, point := range subres {
			// Compute the distance from the reference point (i.e. the query)
			point.Distance = refPoint.DistanceFrom(point)
			// Since the geohashes window is not a radius, we still need to check
			// if the point is located within the given radius.
			if point.Distance > radius {
				continue
			}
			res = append(res, point)
		}
	}
	sort.Sort(byDistance(res))
	qlog.Debug("query took", "duration", time.Since(start))
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
		if err == io.EOF {
			break
		}
		if !strings.HasPrefix(sk, geoPrefix) {
			break
		}
		if bytes.Compare(k, endBytes) > 0 || (limit != 0 && i > limit) {
			return points, nil
		}
		parts := strings.SplitN(sk, ":", 2)
		p := NewPointFromGeohash(parts[1], parts[0])
		points = append(points, p)
		i++
	}
	return points, nil
}

// Data comes from http://www.movable-type.co.uk/scripts/geohash.html
var geoPrecision = []struct {
	radius    float64 // in meters
	precision int     // Geohash precision
}{
	{0.074, 11},
	{0.6, 10},
	{2.4, 9},
	{19, 8},
	{76, 7},
	{610, 6},
	{2400, 5},
	{20000, 4},
	{78000, 3},
	{630000, 2},
	{2500000, 1},
}

// Convert radius meters to Geohash precision
func radiusToPrecision(r float64) int {
	for _, gp := range geoPrecision {
		if r <= gp.radius {
			return gp.precision
		}
	}
	return 2
}
