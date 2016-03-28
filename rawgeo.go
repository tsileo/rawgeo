package rawgeo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/TomiHiltunen/geohash-golang"
	"github.com/cznic/kv"
	"github.com/paulmach/go.geo"
)

// XXX(tsileo): encode the index entry (lat/long) as binary instead of JSON?

var (
	ErrNotFound       = errors.New("key does not exist")
	ErrMissingID      = errors.New("missing ID")
	ErrInvalidLatLong = errors.New("invalid lat/long")
)
var IndexEntryFmt = "index:%s:%s:%s" // index:{name}:{geohash}:{id}

type IndexEntry struct {
	// Geohash enough? or we still store the exact data inputed?
	Lat  float64 `json:"lat"`
	Long float64 `json:"long"`

	ID      string `json:"id"`
	Geohash string `json:"-"` // Don't store it as we can recompute it from `Lat`/`Long`

	// Optional user-supplied meta data about the entry
	Data map[string]interface{} `json:"data,omitempty"`

	Distance float64 `json"-"` // Computed query time
}

func (ie *IndexEntry) Point() *geo.Point {
	return geo.NewPointFromLatLng(ie.Lat, ie.Long)
}

type byDistance []*IndexEntry

func (s byDistance) Len() int           { return len(s) }
func (s byDistance) Less(i, j int) bool { return s[i].Distance < s[j].Distance }
func (s byDistance) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type DB struct {
	db   *kv.DB
	path string
	mu   *sync.Mutex
}

// New creates a new database.
func New(path string) (*DB, error) {
	createOpen := kv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	kvdb, err := createOpen(path, &kv.Options{})
	if err != nil {
		return nil, err
	}
	return &DB{
		db:   kvdb,
		path: path,
		mu:   new(sync.Mutex),
	}, nil
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Destroy() error {
	if db.path != "" {
		db.Close()
		return os.RemoveAll(db.path)
	}
	return nil
}

// Put index the given entry in the given index
func (db *DB) Put(name string, entry *IndexEntry) error {
	// Compute the geohash if needed
	if entry.Geohash == "" {
		entry.Geohash = geohash.Encode(entry.Lat, entry.Long)
	}
	if entry.ID == "" {
		return ErrMissingID
	}
	if entry.Lat == 0 || entry.Long == 0 {
		return ErrInvalidLatLong
	}
	js, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if err := db.db.Set([]byte(fmt.Sprintf(IndexEntryFmt, name, entry.Geohash, entry.ID)), js); err != nil {
		return err
	}
	return nil
}

func (db *DB) get(key []byte) (*IndexEntry, error) {
	entry := &IndexEntry{}
	data, err := db.db.Get(nil, key)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (db *DB) Find(name string, lat, long float64, precision int) ([]*IndexEntry, error) {
	// XXX(tsileo): make precision optional
	refPoint := geo.NewPointFromLatLng(lat, long)
	gh := geohash.EncodeWithPrecision(lat, long, precision)

	res := []*IndexEntry{}
	for _, geohash := range append(geohash.CalculateAllAdjacent(gh), gh) {
		subres, err := db.findPrefix(name, geohash)
		fmt.Printf("adj=%s / %d\n", geohash, len(subres))
		if err != nil {
			return nil, err
		}
		for _, entry := range subres {
			// Compute the distance from the query reference using the Haversine formula
			entry.Distance = entry.Point().GeoDistanceFrom(refPoint)
			res = append(res, entry)
		}
	}
	sort.Sort(byDistance(res))
	return res, nil
}

func (db *DB) findPrefix(name, geoPrefix string) ([]*IndexEntry, error) {
	var limit int
	res := []*IndexEntry{}
	start := fmt.Sprintf("index:%s:%s", name, geoPrefix)
	enum, _, err := db.db.Seek([]byte(start))
	if err != nil {
		return nil, err
	}
	// TODO(tsileo): fix the endBytes
	endBytes := []byte(fmt.Sprintf("index:%s:%s", name, "\xff"))
	i := 0
	for {
		k, _, err := enum.Next()
		fmt.Printf("iter: k=%s err=%v\n", k, err)
		if err == io.EOF {
			break
		}
		if !strings.HasPrefix(string(k), start) {
			break
		}
		if bytes.Compare(k, endBytes) > 0 || (limit != 0 && i > limit) {
			return res, nil
		}
		kv, err := db.get(k)
		if err != nil {
			return nil, err
		}
		res = append(res, kv)
		i++
	}
	return res, nil
}
