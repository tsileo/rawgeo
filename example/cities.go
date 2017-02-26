package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pariz/gountries"

	"a4.io/rawgeo"
)

var data = gountries.New()

type Location struct {
	ID                 string  `json:"id"`          // 0
	CityName           string  `json:"city_name"`   // 1
	Lat                float64 `json:"lat"`         // 4
	Lon                float64 `json:"lon"`         // 5
	CountryCode        string  `json:"cc"`          // 8
	AdminCode          string  `json:"admin"`       // 10
	CountrySubdivision string  `json:"subdivision"` // 11

	CountryName     string `json:"country_name"`
	SubdivisionName string `json:"subdivison_name"`
	Name            string `json:"name"`
}

// https://github.com/hexorx/countries/tree/master/lib
// source => https://anonscm.debian.org/cgit/pkg-isocodes/iso-codes.git/tree/iso_3166-2

// XXX(tsileo): filter by feature class/feature code to only restrict to cities

var locations = map[string]*Location{}

func parseLocation(db *rawgeo.RawGeo) error {
	file, err := os.Open("cities1000.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	scan := bufio.NewScanner(file)
	for scan.Scan() {
		line := scan.Text()
		s := strings.Split(line, "\t")
		if len(s) < 19 {
			continue
		}
		lat, err := strconv.ParseFloat(s[4], 64)
		if err != nil {
			return err
		}
		lon, err := strconv.ParseFloat(s[5], 64)
		if err != nil {
			return err
		}
		loc := &Location{
			ID:                 s[0],
			CityName:           s[1],
			Lat:                lat,
			Lon:                lon,
			CountryCode:        s[8],
			AdminCode:          s[10], // admin
			CountrySubdivision: s[11], // admin2
		}
		country, _ := data.FindCountryByAlpha(loc.CountryCode)

		loc.CountryName = country.Name.Common
		for _, subdiv := range country.SubDivisions() {
			if subdiv.Code == loc.CountrySubdivision {
				loc.SubdivisionName = subdiv.Name
			}
		}

		if loc.SubdivisionName != "" {
			loc.Name = fmt.Sprintf("%s, %s, %s", loc.CityName, loc.SubdivisionName, loc.CountryName)
		} else {
			loc.Name = fmt.Sprintf("%s, %s", loc.CityName, loc.CountryName)
		}

		locations[loc.ID] = loc
		p := &rawgeo.Point{
			ID:  loc.ID,
			Lat: lat,
			Lng: lon,
		}
		if err := db.Index(p); err != nil {
			if err == rawgeo.ErrInvalidLatLong {
				continue
			}
			return err
		}
	}
	return nil
}

func main() {
	n := time.Now()
	db, err := rawgeo.New("cities2.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	if err := parseLocation(db); err != nil {
		panic(err)
	}
	took := time.Since(n)
	fmt.Printf("loading took %v\n", took)

	n = time.Now()
	// Query should return Austin TX
	res, err := db.Query(48.26127189, 4.0871129, 1000)
	// res, err := db.Query(30.26715, -97.74306, 40) // 40m
	took = time.Since(n)
	fmt.Printf("query took %v\n", took)
	if res != nil && len(res) > 0 {
		fmt.Printf("res=%+v", locations[res[0].ID])
	} else {
		fmt.Printf("res=%q\nerr=%v", res, err)
	}
}
