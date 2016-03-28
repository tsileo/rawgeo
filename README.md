# Rawgeo

[![Godoc Reference](https://godoc.org/github.com/tsileo/rawgeo?status.png)](https://godoc.org/github.com/tsileo/rawgeo)

Building block for building geospatial indexes using geohashes.

## Features

- Index is stored in a [kv](https://github.com/cznic/kv) file.
- Find nearest neighbors from `lag,lng` sorted by distance.
- Don't designed to store you data, but you attach a `map[string]interface{}` for lightweight use case.

