package tiempo

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
)

const (
	defaultMaxWALSize       = 100 * 1024 * 1024
	defaultCompactionPeriod = 1 * time.Hour
	walFileMode             = 0644
	maxRecordSize           = 1 * 1024 * 1024
)

type Metrics struct {
	WritesTotal     uint64
	WriteErrorTotal uint64
	WriteLatencyNs  uint64

	QueriesTotal    uint64
	QueryErrorTotal uint64
	QueryLatencyNs  uint64

	CompactionsTotal     uint64
	LastCompactionTimeNs int64

	RecordsStored   uint64
	CurrentStorageB uint64
	PeakStorageB    uint64
}

// Record represents a single time-series data point.
type Record struct {
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags"`
	DeletedAt int64             `json:"deleted_at:omitempty"`
}

type TiempoDB struct {
	mu      sync.RWMutex
	data    []Record
	index   map[int64]int // time-to-index for fast lookup
	walFile *os.File
	walLock sync.Mutex

	maxWALSize       int64
	compactionPeriod time.Duration
	path             string

	metrics     Metrics
	metricsLock sync.RWMutex

	lastCompaction time.Time
	compactChan    chan struct{}
	stopCompaction chan struct{}
}

type DatabaseOption func(*TiempoDB)

func WithMaxWALSize(maxSize int64) DatabaseOption {
	return func(db *TiempoDB) {
		if maxSize > 0 {
			db.maxWALSize = maxSize
		}
	}
}

func WithCompactionPeriod(period time.Duration) DatabaseOption {
	return func(db *TiempoDB) {
		if period > 0 {
			db.compactionPeriod = period
		}
	}
}

func NewTiempoDB(walPath string, opts ...DatabaseOption) (*TiempoDB, error) {
	if err := os.MkdirAll(filepath.Dir(walPath), os.ModePerm); err != nil {
		return nil, fmt.Errorf("could not create WAL directory: %v", err)
	}

	walFile, err := os.OpenFile(walPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, walFileMode)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %v", err)
	}

	db := &TiempoDB{
		data:             make([]Record, 0, 1000),
		index:            make(map[int64]int),
		walFile:          walFile,
		path:             walPath,
		maxWALSize:       defaultMaxWALSize,
		compactionPeriod: defaultCompactionPeriod,
		compactChan:      make(chan struct{}, 1),
		stopCompaction:   make(chan struct{}),
	}

	for _, opt := range opts {
		opt(db)
	}

	if err := db.Recover(); err != nil {
		return nil, fmt.Errorf("failed to recover database: %v", err)
	}

	go db.compactionWorker()

	return db, nil
}

// writeWAL writes the record to the Write-Ahead Log (WAL) with Snappy compression.
func (db *TiempoDB) writeWAL(record Record) error {
	db.walLock.Lock()
	defer db.walLock.Unlock()

	// Check WAL size and potentially rotate
	if err := db.rotateWALIfNeeded(); err != nil {
		return err
	}

	var buffer bytes.Buffer
	// Write Timestamp
	if err := binary.Write(&buffer, binary.LittleEndian, record.Timestamp); err != nil {
		return fmt.Errorf("failed to write timestamp: %v", err)
	}

	// Write Value
	if err := binary.Write(&buffer, binary.LittleEndian, record.Value); err != nil {
		return fmt.Errorf("failed to write value: %v", err)
	}

	// Compress data
	compressed := snappy.Encode(nil, buffer.Bytes())

	// Validate compressed data size
	if len(compressed) > maxRecordSize {
		return fmt.Errorf("record exceeds maximum size of %d bytes", maxRecordSize)
	}

	// Write record length
	if err := binary.Write(db.walFile, binary.LittleEndian, uint32(len(compressed))); err != nil {
		return fmt.Errorf("failed to write record length: %v", err)
	}

	// Write compressed data
	if _, err := db.walFile.Write(compressed); err != nil {
		return fmt.Errorf("failed to write to WAL: %v", err)
	}

	return db.walFile.Sync()
}

// compactionWorker runs periodic compaction
func (db *TiempoDB) compactionWorker() {
	ticker := time.NewTicker(db.compactionPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Trigger compaction
			if err := db.Compact(); err != nil {
				log.Printf("Compaction error: %v", err)
			}
		case <-db.compactChan:
			// Manual compaction request
			if err := db.Compact(); err != nil {
				log.Printf("Manual compaction error: %v", err)
			}
		case <-db.stopCompaction:
			return
		}
	}
}

// Write appends a new record to the database
func (db *TiempoDB) Write(record Record) error {
	start := time.Now()

	// Increment total writes
	atomic.AddUint64(&db.metrics.WritesTotal, 1)

	// Write to WAL
	if err := db.writeWAL(record); err != nil {
		// Track write errors
		atomic.AddUint64(&db.metrics.WriteErrorTotal, 1)
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	db.data = append(db.data, record)
	db.index[record.Timestamp] = len(db.data) - 1

	// Update metrics
	writeLatency := uint64(time.Since(start).Nanoseconds())
	atomic.StoreUint64(&db.metrics.WriteLatencyNs, writeLatency)
	atomic.AddUint64(&db.metrics.RecordsStored, 1)

	return nil
}

func (db *TiempoDB) Query(start, end int64, filters ...func(Record) bool) ([]Record, error) {
	queryStart := time.Now()
	atomic.AddUint64(&db.metrics.QueriesTotal, 1)

	db.mu.RLock()
	defer db.mu.RUnlock()

	var results []Record
	for _, record := range db.data {
		// Skip deleted or soft-deleted records
		if record.DeletedAt > 0 {
			continue
		}

		if record.Timestamp >= start && record.Timestamp <= end {
			// Apply additional filters
			matches := true
			for _, filter := range filters {
				if !filter(record) {
					matches = false
					break
				}
			}
			if matches {
				results = append(results, record)
			}
		}
	}

	// Sort results by timestamp
	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp < results[j].Timestamp
	})

	// Update query metrics
	queryLatency := uint64(time.Since(queryStart).Nanoseconds())
	atomic.StoreUint64(&db.metrics.QueryLatencyNs, queryLatency)

	return results, nil
}

// Compact performs database compaction
func (db *TiempoDB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	compactionStart := time.Now()

	// Create a new slice to hold non-deleted records
	var compactedData []Record

	// Track bytes before and after compaction
	_ = uint64(len(db.data) * (8 + 8 + 8 + 24)) // Rough estimate

	for _, record := range db.data {
		// Only keep non-deleted records within a reasonable time window
		if record.DeletedAt == 0 {
			compactedData = append(compactedData, record)
		}
	}

	// Replace existing data
	db.data = compactedData

	// Rebuild index
	db.rebuildIndex()

	// Update compaction metrics
	atomic.AddUint64(&db.metrics.CompactionsTotal, 1)
	atomic.StoreInt64(&db.metrics.LastCompactionTimeNs, time.Since(compactionStart).Nanoseconds())

	// Update storage metrics
	afterSize := uint64(len(db.data) * (8 + 8 + 8 + 24)) // Rough estimate
	atomic.StoreUint64(&db.metrics.CurrentStorageB, afterSize)

	if afterSize > atomic.LoadUint64(&db.metrics.PeakStorageB) {
		atomic.StoreUint64(&db.metrics.PeakStorageB, afterSize)
	}

	return nil
}

// GetMetrics returns a snapshot of current database metrics
func (db *TiempoDB) GetMetrics() Metrics {
	return Metrics{
		WritesTotal:     atomic.LoadUint64(&db.metrics.WritesTotal),
		WriteErrorTotal: atomic.LoadUint64(&db.metrics.WriteErrorTotal),
		WriteLatencyNs:  atomic.LoadUint64(&db.metrics.WriteLatencyNs),

		QueriesTotal:    atomic.LoadUint64(&db.metrics.QueriesTotal),
		QueryErrorTotal: atomic.LoadUint64(&db.metrics.QueryErrorTotal),
		QueryLatencyNs:  atomic.LoadUint64(&db.metrics.QueryLatencyNs),

		CompactionsTotal:     atomic.LoadUint64(&db.metrics.CompactionsTotal),
		LastCompactionTimeNs: atomic.LoadInt64(&db.metrics.LastCompactionTimeNs),

		RecordsStored:   atomic.LoadUint64(&db.metrics.RecordsStored),
		CurrentStorageB: atomic.LoadUint64(&db.metrics.CurrentStorageB),
		PeakStorageB:    atomic.LoadUint64(&db.metrics.PeakStorageB),
	}
}

// Delete removes records within the specified time range
func (db *TiempoDB) Delete(start, end int64, hardDelete bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	now := time.Now().UnixMilli()

	if hardDelete {
		// Create a new slice to hold non-deleted records
		newData := make([]Record, 0, len(db.data))
		for _, record := range db.data {
			if record.Timestamp < start || record.Timestamp > end {
				newData = append(newData, record)
			}
		}
		db.data = newData
	} else {
		// Soft delete
		for i := range db.data {
			if db.data[i].Timestamp >= start && db.data[i].Timestamp <= end {
				db.data[i].DeletedAt = now
			}
		}
	}

	// Rebuild index
	db.rebuildIndex()

	return nil
}

func (db *TiempoDB) Close() error {
	// Stop compaction worker
	close(db.stopCompaction)

	db.mu.Lock()
	defer db.mu.Unlock()
	return db.walFile.Close()
}

// Recover replays the WAL file to restore the database state
func (db *TiempoDB) Recover() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, err := db.walFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to start of WAL: %v", err)
	}

	bufferedReader := bufio.NewReader(db.walFile)
	for {
		// Read record length
		var recordLength uint32
		err := binary.Read(bufferedReader, binary.LittleEndian, &recordLength)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read record length: %v", err)
		}

		// Validate record size
		if recordLength > maxRecordSize {
			return fmt.Errorf("invalid record size: %d", recordLength)
		}

		// Read compressed data
		compressed := make([]byte, recordLength)
		_, err = io.ReadFull(bufferedReader, compressed)
		if err != nil {
			return fmt.Errorf("failed to read compressed data: %v", err)
		}

		// Decompress
		decompressed, err := snappy.Decode(nil, compressed)
		if err != nil {
			return fmt.Errorf("failed to decompress record: %v", err)
		}

		// Deserialize
		var record Record
		buffer := bytes.NewBuffer(decompressed)
		if err := binary.Read(buffer, binary.LittleEndian, &record.Timestamp); err != nil {
			return fmt.Errorf("failed to read timestamp: %v", err)
		}
		if err := binary.Read(buffer, binary.LittleEndian, &record.Value); err != nil {
			return fmt.Errorf("failed to read value: %v", err)
		}

		db.data = append(db.data, record)
	}

	// Rebuild index
	db.rebuildIndex()
	return nil
}

// rebuildIndex reconstructs the timestamp-to-index mapping
func (db *TiempoDB) rebuildIndex() {
	db.index = make(map[int64]int)
	for i, record := range db.data {
		db.index[record.Timestamp] = i
	}
}

// rotateWALIfNeeded checks and rotates WAL if it exceeds maximum size
func (db *TiempoDB) rotateWALIfNeeded() error {
	stat, err := db.walFile.Stat()
	if err != nil {
		return fmt.Errorf("could not get WAL file stats: %v", err)
	}

	if stat.Size() > db.maxWALSize {
		// Close current WAL
		if err := db.walFile.Close(); err != nil {
			return fmt.Errorf("failed to close current WAL: %v", err)
		}

		// Generate new WAL filename with timestamp
		newWALPath := fmt.Sprintf("%s.%d", db.path, time.Now().UnixNano())

		// Open new WAL file
		newWALFile, err := os.OpenFile(newWALPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, walFileMode)
		if err != nil {
			return fmt.Errorf("failed to create new WAL file: %v", err)
		}

		db.walFile = newWALFile
	}

	return nil
}
