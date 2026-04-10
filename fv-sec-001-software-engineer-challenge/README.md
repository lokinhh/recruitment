# FV-SEC001 - Ad Performance Aggregator (Go)

CLI application to process a ~1GB `ad_data.csv`, aggregate by `campaign_id`, and output:

- `top10_ctr.csv`: top 10 campaigns with the highest CTR
- `top10_cpa.csv`: top 10 campaigns with the lowest CPA (excluding campaigns with `conversions = 0`)

## Setup

Requirements:

- Go 1.25+
- Dataset at `ad_data_csv/ad_data.csv` (actual file size is about 995MB)

## How To Run

Run with default input (reads `../ad_data_csv/ad_data.csv` when running from `src`):

```bash
cd src
go run .
```

Run with custom input/output:

```bash
cd src
go run . -input ../ad_data_csv/ad_data.csv -output ./results
```

Run benchmark multiple times (with warm-up):

```bash
cd src
go run . -runs=10 -warmup=10
```

## Output Files

Generated output files:

- `src/results/top10_ctr.csv`
- `src/results/top10_cpa.csv`

## Libraries Used

Only Go standard library is used:

- `encoding/csv`
- `flag`
- `os`, `filepath`
- `runtime`, `syscall`, `time`
- `sort`, `strings`

## Processing Strategy

Main processing flow:

1. Stream-read the CSV file line by line without loading the full file into memory.
2. Parse each record and normalize values on the fly.
3. Aggregate directly into campaign-level state during parsing.
4. After reading finishes, compute CTR/CPA and select top 10.
5. Write CSV outputs.

## Bottleneck And Optimization Decisions

The biggest bottleneck in this problem is **reading and parsing a large CSV file**.  
If parsing and aggregation are separated into multiple passes, CPU cycles increase and memory pressure grows because intermediate records must be retained.

Therefore, this implementation uses:

- **Fast file access with memory-mapped I/O (`mmap`)** to reduce syscall/read-loop overhead.
- **Chunked parallel parsing across CPU cores** (`workers ~= GOMAXPROCS`), where each worker handles an independent byte range aligned to line boundaries.
- **Byte-level parsing** (`parseLineToCompactTrusted`) instead of a generic CSV parser, since the input format is fixed and trusted.
- **Money stored as cents (`int64`)** to avoid float overhead during parsing and accumulation.
- **Single-pass aggregation during parsing**, without creating an intermediate record slice.
- **Local aggregator per worker, then final merge** to reduce contention/locking on the hot path.

Benefits:

- Higher read+parse throughput on large files.
- Lower CPU cycle cost by removing unnecessary parse/convert layers.
- Fewer allocations, lower GC pressure, and lower peak memory.

## Benchmark Report (1GB dataset)

Command:

```bash
cd src
go run . -runs=10 -warmup=10
```

Machine/runtime:

- `go=go1.25.7`
- `os=darwin`, `arch=arm64`
- `cpu_model=Apple M1 Max`
- `cpus=10`, `gomaxprocs=10`

Measured result:

- Time: `best=199ms`, `avg=207ms`, `median=207ms`, `p95=213ms`, `worst=214ms`, `jitter=7.1%`
- CPU: `cpu_avg=881.6%`, `cpu_median=876.5%`, `cpu_p95=919.5%`, `cpu_max=919.9%`, `cpu_per_core_avg=88.2%`
- Memory: `peak_alloc=4.04 MiB`, `peak_heap_inuse=4.54 MiB`

## Notes

- The app prints detailed per-run benchmark logs and a final summary.
- You can tune `-runs` and `-warmup` for more stable measurements on your machine.
