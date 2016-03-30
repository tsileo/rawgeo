package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/tsileo/rawgeo"
)

func parseLocation(file string, db *rawgeo.RawGeo) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	csvr := csv.NewReader(f)
	csvr.LazyQuotes = true

	// Skip the header
	if _, err := csvr.Read(); err != nil {
		return err
	}
	for {
		row, err := csvr.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		lat, err := strconv.ParseFloat(row[5], 64)
		if err != nil {
			return err
		}
		lon, err := strconv.ParseFloat(row[6], 64)
		if err != nil {
			return err
		}
		p := &rawgeo.Point{
			ID:  fmt.Sprintf("%s-%s", row[0], row[1]),
			Lat: lat,
			Lng: lon,
			// Data: map[string]interface{}{
			// 	"city":    row[1],
			// 	"country": row[0],
			// 	"region":  row[3],
			// },
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

func search(lat, lon float64, db *rawgeo.RawGeo) ([]*rawgeo.Point, error) {
	return db.Query(lat, lon, 8)
}

func main() {
	db, err := rawgeo.New("cities.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// if err := parseLocation("worldcitiespop.txt", db); err != nil {
	// 	panic(err)
	// }
	n := time.Now()
	res, err := search(30.26715, -97.74306, db)
	took := time.Since(n)
	fmt.Printf("took %v\n", took)
	if res != nil && len(res) > 0 {
		fmt.Printf("res=%+v | %+v\nerr=%s", res[0], res[len(res)-1], err)
	} else {
		fmt.Printf("res=%q\nerr=%v", res, err)
	}
}
