package rawgeo

import (
	log "gopkg.in/inconshreveable/log15.v2"

	"testing"
)

var Paris = &Point{Lat: 48.864716, Lng: 2.349014, ID: "paris"}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func TestRawGeoQueryInsertedPoint(t *testing.T) {
	Log.SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StdoutHandler))
	tdb, err := New("test_1.db")
	check(err)
	defer tdb.Destroy()

	check(tdb.Index(Paris))

	// Check with a 10meters radius that we can retrieve the previously inserted point
	res, err := tdb.Query(Paris.Lat, Paris.Lng, 1)
	check(err)

	if len(res) != 1 {
		t.Errorf("Query should return 1 point, got %d", len(res))
	}

	if len(res) == 0 {
		t.Error("no results found, expected 1")
	} else {
		if int(res[0].Distance) != 0 {
			t.Errorf("Expected distance 0 from query, got %f", res[0].Distance)
		}
	}
}

func TestRawGeo(t *testing.T) {
	Log.SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StdoutHandler))
	log.Root().SetHandler(log.StdoutHandler)
	tdb, err := New("test_2.db")
	check(err)
	defer tdb.Destroy()

	farFromParis := &Point{
		ID:  "500km",
		Lat: 44.36810797040634,
		Lng: 2.349014000000026,
	}

	p2 := &Point{
		ID:  "40km",
		Lat: 48.50498735763251,
		Lng: 2.349014000000026,
	}

	p3 := &Point{
		ID:  "50km",
		Lat: 48.41505519704064,
		Lng: 2.349014000000026,
	}

	for _, p := range []*Point{farFromParis, p2, p3} {
		check(tdb.Index(p))
	}

	res, err := tdb.Query(Paris.Lat, Paris.Lng, 45000)
	check(err)

	if len(res) != 1 {
		t.Errorf("expected 1 results, got %d", len(res))
	}

	res, err = tdb.Query(Paris.Lat, Paris.Lng, 60000)
	check(err)

	if len(res) != 2 {
		t.Errorf("expected 2 results, got %d", len(res))
	}

	res, err = tdb.Query(Paris.Lat, Paris.Lng, 600000)
	check(err)

	if len(res) != 3 {
		t.Errorf("expected 3 results, got %d", len(res))
	}
}
