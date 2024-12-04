# Tiempo

## Overview

Tiempo (or 'Time' in English) is a simple, fast and lightweight time-series database designed to handle efficient write and query operations. It uses a Write-Ahead Log (WAL) for data durability and periodic compaction to optimize storage. It provides basic functionality to store time-series data, query it by time range, and delete or compact data as needed.

Note that Tiempo is not intended for production use. If you're looking for a mature time-series database or a wrapper, take a look at [InfluxDB](https://www.influxdata.com/) and their official wrapper, [Influx Go Client](https://github.com/influxdata/influxdb-client-go), or the alternative [Prometheus](https://prometheus.io/), or others. I just coded Tiempo on my Sunday morning XD.

However, Tiempo is fully functional for your hobby projects, as it offers basic features for a time-series database.

## Key Features

- **Write-Ahead Log (WAL)**: Ensures data durability by appending records to a log.
- **Snappy Compression**: Compresses data to save storage space.
- **Compaction**: Periodically compacts the database to optimize storage.
- **Querying**: Allows querying by timestamp range with filtering support.
- **Metrics**: Provides database metrics such as write counts, read counts, and storage size.
- **Soft and Hard Deletion**: Supports soft (marking for deletion) and hard (removing from storage) deletions.

## Quick Start

### Creating a Database

To create a new `Tiempo` instance, use the `NewTiempoDB` function. You can specify the path to the WAL file and optional configuration options like the maximum WAL size or compaction period.

```go
import (
	"log"
	"github.com/ezrantn/tiempo"
)

func main() {
	db, err := tiempo.NewTiempoDB("/path/to/walfile")
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()
}
```

You can also configure the database with options like maximum WAL size and compaction period:

```go
db, err := tiempo.NewTiempoDB(
	"/path/to/walfile",
	tiempo.WithMaxWALSize(200 * 1024 * 1024),
	tiempo.WithCompactionPeriod(30 * time.Minute),
)
```

### Writing Data

To write data to the database, use the `Write` method. A `Record` consists of a timestamp, value, and optional tags.

```go
record := tiempo.Record{
	Timestamp: time.Now().Unix(),
	Value:     123.45,
	Tags:      map[string]string{"sensor": "temperature", "location": "office"},
}

err := db.Write(record)
if err != nil {
	log.Fatalf("Failed to write record: %v", err)
}
```

### Querying Data

To query data, use the `Query` method. You can specify a time range (`start` and `end`) and apply filters. The method returns a slice of records that match the query.

```go
start := time.Now().Add(-1 * time.Hour).Unix()
end := time.Now().Unix()

results, err := db.Query(start, end, func(r tiempo.Record) bool {
	return r.Tags["sensor"] == "temperature"
})
if err != nil {
	log.Fatalf("Failed to query records: %v", err)
}

for _, record := range results {
	fmt.Printf("Timestamp: %d, Value: %f, Tags: %v\n", record.Timestamp, record.Value, record.Tags)
}
```

### Deleting Data

You can delete data by specifying a time range and whether to perform a hard or soft delete.

```go
start := time.Now().Add(-1 * time.Hour).Unix()
end := time.Now().Unix()

// Soft delete records in the given time range
err := db.Delete(start, end, false)
if err != nil {
	log.Fatalf("Failed to delete records: %v", err)
}

// Hard delete records in the given time range
err = db.Delete(start, end, true)
if err != nil {
	log.Fatalf("Failed to hard delete records: %v", err)
}
```

### Compaction

Tiempo automatically performs compaction periodically to optimize storage usage. You can also trigger a manual compaction:

```go
// Trigger a manual compaction
err := db.Compact()
if err != nil {
	log.Fatalf("Failed to compact database: %v", err)
}
```

Compaction helps keep the database efficient by removing deleted records and reducing the overall storage footprint.

### Metrics

Tiempo provides metrics about database usage, including total writes, queries, compactions, storage, and write/query latency.

You can retrieve these metrics using the `GetMetrics` method:

```go
metrics := db.GetMetrics()
fmt.Printf("Total Writes: %d, Total Queries: %d\n", metrics.WritesTotal, metrics.QueriesTotal)
```

This can help you monitor the health and performance of the database.

### Closing the Database

To safely close the database and ensure all data is flushed, use the `Close` method:

```go
err := db.Close()
if err != nil {
	log.Fatalf("Failed to close database: %v", err)
}
```

## Installation

You can install Tiempo by running:

```bash
go get github.com/ezrantn/tiempo
```

## License

This tool is open-source and available under the [MIT](https://github.com/ezrantn/tiempo/blob/main/LICENSE) License.

## Contributions

Contributions are welcome! Please feel free to submit a pull request.