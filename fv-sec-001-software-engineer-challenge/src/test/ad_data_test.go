package test

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// adDataCSV resolves path to challenge ad_data.csv relative to this test file.
func adDataCSV(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(1)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// src/test -> .. -> .. = challenge root
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "ad_data_csv", "ad_data.csv"))
}

func TestPrintFirstFiveLinesOfAdData(t *testing.T) {
	path := adDataCSV(t)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { _ = f.Close() })

	sc := bufio.NewScanner(f)
	const n = 5
	for i := 0; i < n && sc.Scan(); i++ {
		t.Logf("%s", sc.Text())
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("read: %v", err)
	}
}
