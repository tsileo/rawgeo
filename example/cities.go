package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/tsileo/rawgeo"
)

func parseLocation(file string, db *rawgeo.DB) error {
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
		ie := &rawgeo.IndexEntry{
			ID:   fmt.Sprintf("%s-%s", row[0], row[1]),
			Lat:  lat,
			Long: lon,
			Data: map[string]interface{}{
				"city":    row[1],
				"country": row[0],
				"region":  row[3],
			},
		}
		if err := db.Put("cities", ie); err != nil {
			if err == rawgeo.ErrInvalidLatLong {
				continue
			}
			return err
		}
	}
	return nil

}

func search(lat, lon float64, db *rawgeo.DB) ([]*rawgeo.IndexEntry, error) {
	return db.Find("cities", lat, lon, 6)
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
	res, err := search(30.26715, -97.74306, db)
	if res != nil && len(res) > 0 {
		fmt.Printf("res=%+v | %+v\nerr=%s", res[0], res[len(res)-1], err)
	} else {
		fmt.Printf("res=%q\nerr=%v", res, err)
	}
}
