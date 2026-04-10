package test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"fv-sec-001/ad-aggregator/adcsv"
)

func measurePeakMem(run func() error) (elapsed time.Duration, peakAlloc, peakHeapInuse uint64, err error) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	peakAlloc = ms.Alloc
	peakHeapInuse = ms.HeapInuse

	done := make(chan struct{})
	go func() {
		t := time.NewTicker(2 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				runtime.ReadMemStats(&ms)
				if ms.Alloc > peakAlloc {
					peakAlloc = ms.Alloc
				}
				if ms.HeapInuse > peakHeapInuse {
					peakHeapInuse = ms.HeapInuse
				}
			case <-done:
				return
			}
		}
	}()

	start := time.Now()
	err = run()
	elapsed = time.Since(start)
	close(done)

	runtime.ReadMemStats(&ms)
	if ms.Alloc > peakAlloc {
		peakAlloc = ms.Alloc
	}
	if ms.HeapInuse > peakHeapInuse {
		peakHeapInuse = ms.HeapInuse
	}
	return elapsed, peakAlloc, peakHeapInuse, err
}

func benchmarkCSVPath(tb testing.TB) string {
	tb.Helper()
	_, file, _, ok := runtime.Caller(1)
	if !ok {
		tb.Fatal("runtime.Caller failed")
	}
	// src/test -> .. -> .. = challenge root
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "ad_data_csv", "ad_data.csv"))
}

// BenchmarkReadParseFullFile measures time and allocations to read and parse the entire CSV (once per b.N).
// Large file: go test -bench=BenchmarkReadParseFullFile -benchtime=1x -benchmem ./test
func BenchmarkReadParseFullFile(b *testing.B) {
	b.Skip("deprecated: removed ReadFileRows")
}

type benchAgg struct {
	rows int64
	sink int64
}

func (a *benchAgg) Add(_ string, _ string, impressions, clicks int64, _ int64, conversions int64) {
	a.rows++
	a.sink += impressions + clicks + conversions
}

func BenchmarkReadParseFullFileAggregate(b *testing.B) {
	b.Skip("deprecated: removed ReadFileAggregate")
}

func BenchmarkAggregateFileFast(b *testing.B) {
	path := benchmarkCSVPath(b)
	st, err := os.Stat(path)
	if err != nil {
		b.Fatalf("stat %s: %v", path, err)
	}
	b.SetBytes(st.Size())

	var totalRows int64
	var totalNs int64
	var totalMs float64
	var peakAlloc uint64
	var peakHeapInuse uint64
	var a adcsv.FastAggregator
	var sink int64

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		a = adcsv.FastAggregator{}
		elapsed, pAlloc, pHeapInuse, err := measurePeakMem(func() error {
			return adcsv.AggregateFileFast(path, &a)
		})
		totalNs += elapsed.Nanoseconds()
		totalMs += float64(elapsed.Nanoseconds()) / 1e6
		totalRows += a.RowsProcessed
		if pAlloc > peakAlloc {
			peakAlloc = pAlloc
		}
		if pHeapInuse > peakHeapInuse {
			peakHeapInuse = pHeapInuse
		}
		if err != nil {
			b.Fatalf("AggregateFileFast: %v", err)
		}
		if a.RowsProcessed == 0 {
			b.Fatal("no data rows (only header or empty file)")
		}
		for i := 0; i < len(a.Campaigns); i++ {
			sink += a.Campaigns[i].Impressions + a.Campaigns[i].Clicks + a.Campaigns[i].Conversions
		}
	}
	if b.N > 0 && totalRows > 0 {
		b.ReportMetric(float64(totalRows)/float64(b.N), "rows/op")
		b.ReportMetric(float64(totalNs)/float64(totalRows), "ns/row")
		b.ReportMetric(totalMs/float64(b.N), "ms/op_wall")
		b.ReportMetric(float64(peakAlloc), "peak_alloc_bytes")
		b.ReportMetric(float64(peakHeapInuse), "peak_heap_inuse_bytes")
	}
	_ = sink
}

func BenchmarkAggregateFileFastParallel(b *testing.B) {
	path := benchmarkCSVPath(b)
	st, err := os.Stat(path)
	if err != nil {
		b.Fatalf("stat %s: %v", path, err)
	}
	b.SetBytes(st.Size())

	var totalRows int64
	var totalNs int64
	var totalMs float64
	var peakAlloc uint64
	var peakHeapInuse uint64
	var a adcsv.FastAggregator
	var sink int64

	workers := runtime.GOMAXPROCS(0)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		a = adcsv.FastAggregator{}
		elapsed, pAlloc, pHeapInuse, err := measurePeakMem(func() error {
			return adcsv.AggregateFileFastParallel(path, workers, &a)
		})
		totalNs += elapsed.Nanoseconds()
		totalMs += float64(elapsed.Nanoseconds()) / 1e6
		totalRows += a.RowsProcessed
		if pAlloc > peakAlloc {
			peakAlloc = pAlloc
		}
		if pHeapInuse > peakHeapInuse {
			peakHeapInuse = pHeapInuse
		}
		if err != nil {
			b.Fatalf("AggregateFileFastParallel: %v", err)
		}
		if a.RowsProcessed == 0 {
			b.Fatal("no data rows (only header or empty file)")
		}
		for i := 0; i < len(a.Campaigns); i++ {
			sink += a.Campaigns[i].Impressions + a.Campaigns[i].Clicks + a.Campaigns[i].Conversions
		}
	}
	if b.N > 0 && totalRows > 0 {
		b.ReportMetric(float64(totalRows)/float64(b.N), "rows/op")
		b.ReportMetric(float64(totalNs)/float64(totalRows), "ns/row")
		b.ReportMetric(totalMs/float64(b.N), "ms/op_wall")
		b.ReportMetric(float64(peakAlloc), "peak_alloc_bytes")
		b.ReportMetric(float64(peakHeapInuse), "peak_heap_inuse_bytes")
	}
	_ = sink
}
