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

type City struct {
	GeonameID      int
	Name           string   //  name of geographical point (utf8) varchar(200)
	AsciiName      string   //  name of geographical point in plain ascii characters, varchar(200)
	AlternateNames []string //  alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
	Latitude       float64  //  latitude in decimal degrees (wgs84)
	Longitude      float64  //  longitude in decimal degrees (wgs84)
	FeatureClass   string   //  see httpstring // //www.geonames.org/export/codes.html, char(1)
	FeatureCode    string   //  see httpstring // //www.geonames.org/export/codes.html, varchar(10)
	CountryCode    string   //  ISO-3166 2-letter country code, 2 characters
	CC2            string   //  alternate country codes, comma separated, ISO-3166 2-letter country code, 60 characters
	/* Skip 4 Admin */
	Population int       //  bigint (8 byte int)
	Elevation  int       //  in meters, integer
	Dem        string    //  digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
	Timezone   string    //  the timezone id (see file timeZone.txt) varchar(40)
	Modified   time.Time //  date of last modification in yyyy-MM-dd format
}

func parseLocation(db *rawgeo.RawGeo) error {
	file, err := os.Open("cities1000.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	scan := bufio.NewScanner(file)
	t := time.Now()

	n := 0
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
		n++
	}
	d := time.Since(t)
	fmt.Printf("Processed %d entries in %v, %.1f entries/sec.", n, d, float64(n)/(float64(d)/float64(time.Second)))
	return nil
}

func search(lat, lon, radius float64, db *rawgeo.RawGeo) ([]*rawgeo.Point, error) {
	return db.Query(lat, lon, radius)
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
	res, err := search(30.26715, -97.74306, 40, db)
	took := time.Since(n)
	fmt.Printf("took %v\n", took)
	if res != nil && len(res) > 0 {
		fmt.Printf("res=%+v", res[0])
	} else {
		fmt.Printf("res=%q\nerr=%v", res, err)
	}
}
