package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tsileo/rawgeo"
)

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
		p := &rawgeo.Point{
			ID:  fmt.Sprintf("%s-%s", s[8], s[1]),
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
	db, err := rawgeo.New("cities.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	if err := parseLocation(db); err != nil {
		panic(err)
	}
	n := time.Now()
	// Query should return Austin TX
	res, err := db.Query(30.26715, -97.74306, 40) // 40m
	took := time.Since(n)
	fmt.Printf("query took %v\n", took)
	if res != nil && len(res) > 0 {
		fmt.Printf("res=%+v", res[0])
	} else {
		fmt.Printf("res=%q\nerr=%v", res, err)
	}
}
