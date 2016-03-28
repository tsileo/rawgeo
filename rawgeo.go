package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"

	"github.com/TomiHiltunen/geohash-golang"
	"github.com/paulmach/go.geo"
)

type City struct {
	Country, City, AccentCity, Region string
	// ,Population,
	Latitude, Longitude float64
	Geohash             string
}

func (c *City) Point() *geo.Point {
	return geo.NewPointFromLatLng(c.Latitude, c.Longitude)
}

var ps = geo.NewPointSet()

func parseLocation(file string) (map[string]*City, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	csvr := csv.NewReader(f)
	csvr.LazyQuotes = true

	locations := map[string]*City{}
	for {
		row, err := csvr.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return locations, err
		}
		// fmt.Printf("%q\n", row)
		// p := &Point{}
		if row[5] == "Latitude" {
			continue
		}
		lat, err := strconv.ParseFloat(row[5], 64)
		if err != nil {
			return nil, err
		}
		lon, err := strconv.ParseFloat(row[6], 64)
		if err != nil {
			return nil, err
		}
		gh := geohash.Encode(lat, lon)
		city := &City{
			Country:    row[0],
			City:       row[1],
			AccentCity: row[2],
			Region:     row[3],
			Latitude:   lat,
			Longitude:  lon,
			Geohash:    gh,
		}
		// ps.Push(city.Point())
		locations[row[1]] = city
		// fmt.Printf("%s %s\n", row[1], gh)
		// locations[row[0]] = p
	}
	return locations, nil
}

func search(lat, lon float64, data map[string]*City) *City {
	gh := geohash.Encode(lat, lon)
	fmt.Printf("query geohahs=%s\n", gh)
	rpoint := geo.NewPointFromLatLng(lat, lon)
	dist := math.Inf(1)
	var res *City
	for _, c := range data {
		if d := c.Point().GeoDistanceFrom(rpoint); d < dist {
			dist = d
			res = c
		}
	}
	return res
	mostMatched := 0
	matched := 0
	for _, c := range data {
		// check first two characters to reduce the number of loops
		if c.Geohash[0] == gh[0] && c.Geohash[1] == gh[1] {
			matched = 2
			for i := 2; i <= len(gh); i++ {
				//log.Println(gh[0:i])
				if c.Geohash[0:i] == gh[0:i] {
					matched++
				}
			}
			if matched > mostMatched {
				res = c
				mostMatched = matched
			}
		}
	}
	return res
}

// HOW TO SEARCH:
// Choose a precision, get the geohash, find all cities in Ajacent, and find the nearest using lat/long coordiantes
// indexed in a kv file

func main() {
	locs, err := parseLocation("worldcitiespop.txt")
	if err != nil {
		panic(err)
	}
	fmt.Printf("loaded")
	// res := search(37.46, -122.14, locs)
	fmt.Printf("res=%+v", res)
}
