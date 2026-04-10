//go:build unix && !wasip1

package adcsv

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Row is one data line from ad_data.csv (after the header).
type Row struct {
	CampaignID  string
	Date        string
	Impressions int64
	Clicks      int64
	Spend       int64 // cents (*10^2)
	Conversions int64
}

const expectedFields = 6
const maxBatchRows = 65536

var (
	errParseField  = errors.New("invalid field")
	errParseDigit  = errors.New("invalid digit")
	errParseHeader = errors.New("invalid header")
)

// ParseRecord maps one CSV record to Row.
func ParseRecord(record []string) (Row, error) {
	if len(record) != expectedFields {
		return Row{}, fmt.Errorf("expected %d fields, got %d", expectedFields, len(record))
	}
	impressions, err := strconv.ParseInt(strings.TrimSpace(record[2]), 10, 64)
	if err != nil {
		return Row{}, fmt.Errorf("impressions: %w", err)
	}
	clicks, err := strconv.ParseInt(strings.TrimSpace(record[3]), 10, 64)
	if err != nil {
		return Row{}, fmt.Errorf("clicks: %w", err)
	}
	spend, err := parseCentsString(strings.TrimSpace(record[4]))
	if err != nil {
		return Row{}, fmt.Errorf("spend: %w", err)
	}
	conversions, err := strconv.ParseInt(strings.TrimSpace(record[5]), 10, 64)
	if err != nil {
		return Row{}, fmt.Errorf("conversions: %w", err)
	}
	return Row{
		CampaignID:  strings.TrimSpace(record[0]),
		Date:        strings.TrimSpace(record[1]),
		Impressions: impressions,
		Clicks:      clicks,
		Spend:       spend,
		Conversions: conversions,
	}, nil
}

func bytesToStr(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func parseCentsString(s string) (int64, error) {
	if len(s) == 0 {
		return 0, errParseField
	}
	b := unsafe.Slice(unsafe.StringData(s), len(s))
	var intPart int64
	i := 0
	for i < len(b) && b[i] != '.' {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, errParseDigit
		}
		intPart = intPart*10 + int64(c-'0')
		i++
	}
	cents := intPart * 100
	if i < len(b) && b[i] == '.' {
		i++
		if i < len(b) {
			c := b[i]
			if c < '0' || c > '9' {
				return 0, errParseDigit
			}
			cents += int64(c-'0') * 10
			i++
		}
		if i < len(b) {
			c := b[i]
			if c < '0' || c > '9' {
				return 0, errParseDigit
			}
			cents += int64(c - '0')
			i++
		}
		for i < len(b) {
			c := b[i]
			if c < '0' || c > '9' {
				return 0, errParseDigit
			}
			i++
		}
	}
	return cents, nil
}

// split6 splits a single CSV line into exactly six comma-separated fields (no quoted commas).
func split6(line []byte) ([6][]byte, bool) {
	var out [6][]byte
	start := 0
	for i := 0; i < 5; i++ {
		idx := bytes.IndexByte(line[start:], ',')
		if idx < 0 {
			return out, false
		}
		out[i] = line[start : start+idx]
		start += idx + 1
	}
	out[5] = line[start:]
	return out, true
}

// parseLineSinglePassTrusted scans one row with minimal checks.
// Assumes data rows are valid and follow expected CSV format.
func parseLineSinglePassTrusted(line []byte, row *Row) {
	n := len(line)
	i := 0

	j := i
	for i < n && line[i] != ',' {
		i++
	}
	row.CampaignID = bytesToStr(line[j:i])
	i++

	j = i
	for i < n && line[i] != ',' {
		i++
	}
	row.Date = bytesToStr(line[j:i])
	i++

	var v int64
	for i < n && line[i] != ',' {
		v = v*10 + int64(line[i]-'0')
		i++
	}
	row.Impressions = v
	i++

	v = 0
	for i < n && line[i] != ',' {
		v = v*10 + int64(line[i]-'0')
		i++
	}
	row.Clicks = v
	i++

	var intPart int64
	var fracPart int64
	fracDigits := 0
	for i < n && line[i] != ',' && line[i] != '.' {
		intPart = intPart*10 + int64(line[i]-'0')
		i++
	}
	if i < n && line[i] == '.' {
		i++
		for i < n && line[i] != ',' {
			fracPart = fracPart*10 + int64(line[i]-'0')
			fracDigits++
			i++
		}
	}
	cents := intPart * 100
	switch fracDigits {
	case 0:
	case 1:
		cents += fracPart * 10
	default:
		for fracDigits > 2 {
			fracPart /= 10
			fracDigits--
		}
		cents += fracPart
	}
	row.Spend = cents
	i++

	v = 0
	for ; i < n; i++ {
		v = v*10 + int64(line[i]-'0')
	}
	row.Conversions = v
}

func withMmap(path string, work func([]byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return err
	}
	size := st.Size()
	if size == 0 {
		return errors.New("empty csv")
	}
	if size > int64(int(^uint(0)>>1)) {
		return errors.New("file too large")
	}
	n := int(size)
	data, err := unix.Mmap(int(f.Fd()), 0, n, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return err
	}
	defer unix.Munmap(data)

	return work(data)
}

// CompactRow keeps only numeric fields for highest-throughput parsing.
type CompactRow struct {
	CampaignID  uint16 // CMP000-CMP999 -> 0-999
	Date        uint32 // YYYY-MM-DD -> YYYYMMDD
	Impressions int64
	Clicks      int64
	SpendCents  int64
	Conversions int64
}

// FastAggregator is a fixed-size aggregator for max throughput with clean input.
type FastAggregator struct {
	Campaigns [1000]struct {
		Impressions int64
		Clicks      int64
		SpendCents  int64
		Conversions int64
	}
	RowsProcessed int64
}

func skipHeaderFast(data []byte) int {
	p := 0
	for data[p] != '\n' && data[p] != '\r' {
		p++
	}
	for p < len(data) && (data[p] == '\n' || data[p] == '\r') {
		p++
	}
	return p
}

// parseLineToCompactTrusted parses one clean row to CompactRow with minimal branching.
func parseLineToCompactTrusted(line []byte, row *CompactRow) {
	i := 0

	// campaign_id: CMPddd
	row.CampaignID = uint16(line[3]-'0')*100 + uint16(line[4]-'0')*10 + uint16(line[5]-'0')
	for line[i] != ',' {
		i++
	}
	i++ // date start

	// date: YYYY-MM-DD
	row.Date = uint32(line[i]-'0')*10000000 +
		uint32(line[i+1]-'0')*1000000 +
		uint32(line[i+2]-'0')*100000 +
		uint32(line[i+3]-'0')*10000 +
		uint32(line[i+5]-'0')*1000 +
		uint32(line[i+6]-'0')*100 +
		uint32(line[i+8]-'0')*10 +
		uint32(line[i+9]-'0')
	i += 10
	for line[i] != ',' {
		i++
	}
	i++

	// impressions
	v := int64(0)
	for line[i] != ',' {
		v = v*10 + int64(line[i]-'0')
		i++
	}
	row.Impressions = v
	i++

	// clicks
	v = 0
	for line[i] != ',' {
		v = v*10 + int64(line[i]-'0')
		i++
	}
	row.Clicks = v
	i++

	// spend cents (assume xx.yy)
	dollars := int64(0)
	for line[i] != '.' {
		dollars = dollars*10 + int64(line[i]-'0')
		i++
	}
	i++ // dot
	cents := int64(line[i]-'0') * 10
	i++
	// allow 1 or 2 fractional digits before comma
	if line[i] != ',' {
		cents += int64(line[i] - '0')
		i++
	}
	row.SpendCents = dollars*100 + cents
	for line[i] != ',' {
		i++
	}
	i++

	// conversions
	v = 0
	for i < len(line) {
		v = v*10 + int64(line[i]-'0')
		i++
	}
	row.Conversions = v
}

type fastAggLocal struct {
	Campaigns     [1000]struct{ Impressions, Clicks, SpendCents, Conversions int64 }
	RowsProcessed int64
}

func alignStartToNextLine(data []byte, p int) int {
	for p < len(data) && data[p] != '\n' && data[p] != '\r' {
		p++
	}
	for p < len(data) && (data[p] == '\n' || data[p] == '\r') {
		p++
	}
	return p
}

func alignEndToLineEnd(data []byte, p int) int {
	for p < len(data) && data[p] != '\n' && data[p] != '\r' {
		p++
	}
	return p
}

// AggregateFileFastParallel parses and aggregates per-campaign totals using multiple goroutines.
// Assumes clean input format. workers <= 0 uses GOMAXPROCS.
func AggregateFileFastParallel(path string, workers int, agg *FastAggregator) error {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers < 1 {
		workers = 1
	}
	return withMmap(path, func(data []byte) error {
		if len(data) == 0 {
			return errors.New("empty csv")
		}
		start := skipHeaderFast(data)
		if start >= len(data) {
			return nil
		}

		remain := len(data) - start
		if workers > remain {
			workers = 1
		}
		if workers == 1 {
			var row CompactRow
			for p := start; p < len(data); {
				lineStart := p
				for p < len(data) && data[p] != '\n' && data[p] != '\r' {
					p++
				}
				line := data[lineStart:p]
				for p < len(data) && (data[p] == '\n' || data[p] == '\r') {
					p++
				}
				if len(line) == 0 {
					continue
				}
				parseLineToCompactTrusted(line, &row)
				c := &agg.Campaigns[row.CampaignID]
				c.Impressions += row.Impressions
				c.Clicks += row.Clicks
				c.SpendCents += row.SpendCents
				c.Conversions += row.Conversions
				agg.RowsProcessed++
			}
			return nil
		}

		locals := make([]fastAggLocal, workers)
		var wg sync.WaitGroup
		wg.Add(workers)

		chunk := remain / workers
		for w := 0; w < workers; w++ {
			w := w
			s := start + w*chunk
			e := start + (w+1)*chunk
			if w == 0 {
				s = start
			} else {
				s = alignStartToNextLine(data, s)
			}
			if w == workers-1 {
				e = len(data)
			} else {
				e = alignEndToLineEnd(data, e)
			}

			go func() {
				defer wg.Done()
				var row CompactRow
				p := s
				for p < e {
					lineStart := p
					for p < e && data[p] != '\n' && data[p] != '\r' {
						p++
					}
					line := data[lineStart:p]
					for p < e && (data[p] == '\n' || data[p] == '\r') {
						p++
					}
					if len(line) == 0 {
						continue
					}
					parseLineToCompactTrusted(line, &row)
					c := &locals[w].Campaigns[row.CampaignID]
					c.Impressions += row.Impressions
					c.Clicks += row.Clicks
					c.SpendCents += row.SpendCents
					c.Conversions += row.Conversions
					locals[w].RowsProcessed++
				}
			}()
		}
		wg.Wait()

		for w := 0; w < workers; w++ {
			agg.RowsProcessed += locals[w].RowsProcessed
			for i := 0; i < len(agg.Campaigns); i++ {
				agg.Campaigns[i].Impressions += locals[w].Campaigns[i].Impressions
				agg.Campaigns[i].Clicks += locals[w].Campaigns[i].Clicks
				agg.Campaigns[i].SpendCents += locals[w].Campaigns[i].SpendCents
				agg.Campaigns[i].Conversions += locals[w].Campaigns[i].Conversions
			}
		}
		return nil
	})
}

// AggregateFileFast aggregates using all available CPU cores.
func AggregateFileFast(path string, agg *FastAggregator) error {
	return AggregateFileFastParallel(path, runtime.GOMAXPROCS(0), agg)
}

// CampaignStats is the aggregated output row for top-10.
type CampaignStats struct {
	ID              uint16
	TotalImp        int64
	TotalClicks     int64
	TotalSpendCents int64
	TotalConv       int64
	CTR             float64
	CPA             float64
}

func CTR(clicks, imps int64) float64 {
	if imps <= 0 {
		return 0
	}
	return float64(clicks) / float64(imps)
}

func CPA(spendCents, conv int64) float64 {
	if conv <= 0 {
		return 0
	}
	return (float64(spendCents) / 100.0) / float64(conv)
}

func insertTop10(dst *[10]CampaignStats, n *int, cur CampaignStats, better func(a, b CampaignStats) bool) {
	// keep dst sorted best->worst according to better(a,b) == true means a should be before b.
	pos := -1
	for i := 0; i < *n; i++ {
		if dst[i].ID == cur.ID {
			pos = i
			dst[i] = cur
			break
		}
	}
	if pos == -1 {
		if *n < 10 {
			dst[*n] = cur
			pos = *n
			*n++
		} else if better(cur, dst[9]) {
			dst[9] = cur
			pos = 9
		} else {
			return
		}
	}
	for pos > 0 && better(dst[pos], dst[pos-1]) {
		dst[pos], dst[pos-1] = dst[pos-1], dst[pos]
		pos--
	}
	for pos+1 < *n && better(dst[pos+1], dst[pos]) {
		dst[pos], dst[pos+1] = dst[pos+1], dst[pos]
		pos++
	}
}

// Top10 aggregates per-campaign totals, then returns top 10 by CTR and top 10 by lowest CPA (excluding zero conversions).
func Top10(path string) (topCTR []CampaignStats, topCPA []CampaignStats, err error) {
	var agg FastAggregator
	if err := AggregateFileFast(path, &agg); err != nil {
		return nil, nil, err
	}

	var topC [10]CampaignStats
	var topA [10]CampaignStats
	nC, nA := 0, 0

	for id := 0; id < len(agg.Campaigns); id++ {
		c := agg.Campaigns[id]
		stats := CampaignStats{
			ID:              uint16(id),
			TotalImp:        c.Impressions,
			TotalClicks:     c.Clicks,
			TotalSpendCents: c.SpendCents,
			TotalConv:       c.Conversions,
		}
		stats.CTR = CTR(stats.TotalClicks, stats.TotalImp)
		stats.CPA = CPA(stats.TotalSpendCents, stats.TotalConv)

		insertTop10(&topC, &nC, stats, func(a, b CampaignStats) bool { return a.CTR > b.CTR })
		if stats.TotalConv > 0 {
			insertTop10(&topA, &nA, stats, func(a, b CampaignStats) bool { return a.CPA < b.CPA })
		}
	}

	topCTR = make([]CampaignStats, nC)
	copy(topCTR, topC[:nC])
	topCPA = make([]CampaignStats, nA)
	copy(topCPA, topA[:nA])
	return topCTR, topCPA, nil
}
