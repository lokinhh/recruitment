package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"syscall"
	"time"

	"fv-sec-001/ad-aggregator/adcsv"
)

func main() {
	var input string
	var output string
	var runs int
	var warmup int
	flag.StringVar(&input, "input", "", "path to ad_data.csv")
	flag.StringVar(&output, "output", ".", "output directory")
	flag.IntVar(&runs, "runs", 3, "number of benchmark runs")
	flag.IntVar(&warmup, "warmup", 1, "number of warm-up runs")
	flag.Parse()

	if input == "" {
		// Default for running from src/ as in the README.
		input = "../ad_data_csv/ad_data.csv"
	}
	if runs < 1 {
		runs = 1
	}
	if warmup < 0 {
		warmup = 0
	}

	printRuntimeInfo(input, output, runs, warmup)

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	peakAlloc := ms.Alloc
	peakHeapInuse := ms.HeapInuse

	done := make(chan struct{})
	go func() {
		t := time.NewTicker(5 * time.Millisecond)
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

	runDurations := make([]time.Duration, 0, runs)
	runCPUUsages := make([]float64, 0, runs)
	var topCTR []adcsv.CampaignStats
	var topCPA []adcsv.CampaignStats
	var err error

	for i := 0; i < warmup; i++ {
		_, _ = fmt.Fprintf(os.Stderr, "[warmup %d/%d] start\n", i+1, warmup)
		start := time.Now()
		_, _, err = adcsv.Top10(input)
		d := time.Since(start)
		if err != nil {
			break
		}
		_, _ = fmt.Fprintf(os.Stderr, "[warmup %d/%d] done in %s\n", i+1, warmup, d.Truncate(time.Millisecond))
	}

	if err == nil {
		for i := 0; i < runs; i++ {
			_, _ = fmt.Fprintf(os.Stderr, "[run %d/%d] start\n", i+1, runs)
			cpuStart, cpuOK := getProcessCPUTime()
			start := time.Now()
			topCTR, topCPA, err = adcsv.Top10(input)
			d := time.Since(start)
			if err != nil {
				break
			}
			runDurations = append(runDurations, d)
			cpuUsageText := "n/a"
			if cpuOK {
				if cpuEnd, ok := getProcessCPUTime(); ok {
					cpuDelta := cpuEnd - cpuStart
					if d > 0 && cpuDelta > 0 {
						cpuPct := (float64(cpuDelta) / float64(d)) * 100.0
						runCPUUsages = append(runCPUUsages, cpuPct)
						cpuUsageText = fmt.Sprintf("%.1f%%", cpuPct)
					}
				}
			}
			_, _ = fmt.Fprintf(os.Stderr, "[run %d/%d] done in %s | cpu=%s\n", i+1, runs, d.Truncate(time.Millisecond), cpuUsageText)
		}
	}

	close(done)

	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if err := os.MkdirAll(output, 0o755); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if err := WriteTop10CSV(filepath.Join(output, "top10_ctr.csv"), topCTR); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	if err := WriteTop10CSV(filepath.Join(output, "top10_cpa.csv"), topCPA); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	printBenchmarkSummary(runDurations, runCPUUsages, peakAlloc, peakHeapInuse)
}

func printRuntimeInfo(input, output string, runs, warmup int) {
	info, ok := debug.ReadBuildInfo()
	goVersion := runtime.Version()
	if ok && info.GoVersion != "" {
		goVersion = info.GoVersion
	}
	cpuModel := detectCPUModel()
	_, _ = fmt.Fprintf(os.Stderr,
		"runtime: go=%s os=%s arch=%s cpus=%d gomaxprocs=%d cpu_model=%s\n",
		goVersion,
		runtime.GOOS,
		runtime.GOARCH,
		runtime.NumCPU(),
		runtime.GOMAXPROCS(0),
		cpuModel,
	)
	_, _ = fmt.Fprintf(os.Stderr,
		"input=%s output=%s warmup=%d runs=%d\n",
		input,
		output,
		warmup,
		runs,
	)
}

func detectCPUModel() string {
	switch runtime.GOOS {
	case "darwin":
		out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output()
		if err == nil {
			s := strings.TrimSpace(string(out))
			if s != "" {
				return s
			}
		}
		out, err = exec.Command("system_profiler", "SPHardwareDataType").Output()
		if err == nil {
			lines := strings.Split(string(out), "\n")
			for _, line := range lines {
				s := strings.TrimSpace(line)
				if strings.HasPrefix(s, "Chip:") || strings.HasPrefix(s, "Processor Name:") {
					parts := strings.SplitN(s, ":", 2)
					if len(parts) == 2 {
						v := strings.TrimSpace(parts[1])
						if v != "" {
							return v
						}
					}
				}
			}
		}
		out, err = exec.Command("sysctl", "-n", "hw.model").Output()
		if err == nil {
			s := strings.TrimSpace(string(out))
			if s != "" {
				return s
			}
		}
	case "linux":
		b, err := os.ReadFile("/proc/cpuinfo")
		if err == nil {
			lines := strings.Split(string(b), "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "model name") {
					parts := strings.SplitN(line, ":", 2)
					if len(parts) == 2 {
						s := strings.TrimSpace(parts[1])
						if s != "" {
							return s
						}
					}
				}
			}
		}
	case "windows":
		out, err := exec.Command("wmic", "cpu", "get", "Name").Output()
		if err == nil {
			lines := strings.Split(strings.TrimSpace(string(out)), "\n")
			for _, line := range lines {
				s := strings.TrimSpace(line)
				if s != "" && !strings.EqualFold(s, "Name") {
					return s
				}
			}
		}
	}
	return "unknown"
}

func printBenchmarkSummary(runDurations []time.Duration, runCPUUsages []float64, peakAlloc uint64, peakHeapInuse uint64) {
	if len(runDurations) == 0 {
		_, _ = fmt.Fprintf(os.Stderr, "no successful benchmark runs | peak_alloc=%.2f MiB | peak_heap_inuse=%.2f MiB\n",
			float64(peakAlloc)/(1024*1024),
			float64(peakHeapInuse)/(1024*1024),
		)
		return
	}

	total := time.Duration(0)
	best := runDurations[0]
	worst := runDurations[0]
	for _, d := range runDurations {
		total += d
		if d < best {
			best = d
		}
		if d > worst {
			worst = d
		}
	}
	avg := total / time.Duration(len(runDurations))
	sorted := append([]time.Duration(nil), runDurations...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	median := sorted[len(sorted)/2]
	if len(sorted)%2 == 0 {
		median = (sorted[len(sorted)/2-1] + sorted[len(sorted)/2]) / 2
	}
	p95 := sorted[(len(sorted)-1)*95/100]
	jitter := float64(worst-best) / float64(avg) * 100.0

	cpuSummary := "cpu=n/a"
	if len(runCPUUsages) > 0 {
		cpuSorted := append([]float64(nil), runCPUUsages...)
		sort.Float64s(cpuSorted)
		cpuTotal := 0.0
		for _, v := range cpuSorted {
			cpuTotal += v
		}
		cpuAvg := cpuTotal / float64(len(cpuSorted))
		cpuMedian := cpuSorted[len(cpuSorted)/2]
		if len(cpuSorted)%2 == 0 {
			cpuMedian = (cpuSorted[len(cpuSorted)/2-1] + cpuSorted[len(cpuSorted)/2]) / 2
		}
		cpuP95 := cpuSorted[(len(cpuSorted)-1)*95/100]
		cpuMax := cpuSorted[len(cpuSorted)-1]
		cpuPerCore := cpuAvg / float64(runtime.NumCPU())
		cpuSummary = fmt.Sprintf("cpu_avg=%.1f%% cpu_median=%.1f%% cpu_p95=%.1f%% cpu_max=%.1f%% cpu_per_core_avg=%.1f%%",
			cpuAvg, cpuMedian, cpuP95, cpuMax, cpuPerCore)
	}

	_, _ = fmt.Fprintf(os.Stderr,
		"benchmark: runs=%d best=%s avg=%s median=%s p95=%s worst=%s jitter=%.1f%% | %s | peak_alloc=%.2f MiB | peak_heap_inuse=%.2f MiB\n",
		len(runDurations),
		best.Truncate(time.Millisecond),
		avg.Truncate(time.Millisecond),
		median.Truncate(time.Millisecond),
		p95.Truncate(time.Millisecond),
		worst.Truncate(time.Millisecond),
		jitter,
		cpuSummary,
		float64(peakAlloc)/(1024*1024),
		float64(peakHeapInuse)/(1024*1024),
	)
}

func getProcessCPUTime() (time.Duration, bool) {
	switch runtime.GOOS {
	case "darwin", "linux":
		var ru syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
			return 0, false
		}
		user := time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond
		sys := time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond
		return user + sys, true
	default:
		return 0, false
	}
}

func WriteTop10CSV(path string, rows []adcsv.CampaignStats) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write([]string{
		"campaign_id",
		"total_impressions",
		"total_clicks",
		"total_spend",
		"total_conversions",
		"CTR",
		"CPA",
	}); err != nil {
		return err
	}

	for _, r := range rows {
		totalSpend := fmt.Sprintf("%.2f", float64(r.TotalSpendCents)/100.0)
		ctrS := fmt.Sprintf("%.4f", r.CTR)
		cpaS := ""
		if r.TotalConv > 0 {
			cpaS = fmt.Sprintf("%.2f", r.CPA)
		}
		rec := []string{
			fmt.Sprintf("CMP%03d", r.ID),
			fmt.Sprintf("%d", r.TotalImp),
			fmt.Sprintf("%d", r.TotalClicks),
			totalSpend,
			fmt.Sprintf("%d", r.TotalConv),
			ctrS,
			cpaS,
		}
		if err := w.Write(rec); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}
