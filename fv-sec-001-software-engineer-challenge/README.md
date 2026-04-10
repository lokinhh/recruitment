# FV-SEC001 - Ad Performance Aggregator (Go)

CLI application để xử lý file `ad_data.csv` ~1GB, aggregate theo `campaign_id`, và xuất:

- `top10_ctr.csv`: top 10 campaign có CTR cao nhất
- `top10_cpa.csv`: top 10 campaign có CPA thấp nhất (loại campaign có conversions = 0)

## Setup

Yêu cầu:

- Go 1.25+
- Dataset tại `ad_data_csv/ad_data.csv` (kích thước thực tế khoảng 995MB)

## How To Run

Chạy mặc định (đọc `../ad_data_csv/ad_data.csv` khi đứng trong `src`):

```bash
cd src
go run .
```

Chạy chỉ định input/output:

```bash
cd src
go run . -input ../ad_data_csv/ad_data.csv -output ./results
```

Chạy benchmark nhiều lần (có warm-up):

```bash
cd src
go run . -runs=10 -warmup=10
```

## Output Files

Kết quả được ghi ra:

- `src/results/top10_ctr.csv`
- `src/results/top10_cpa.csv`

## Libraries Used

Chỉ dùng Go standard library:

- `encoding/csv`
- `flag`
- `os`, `filepath`
- `runtime`, `syscall`, `time`
- `sort`, `strings`

## Processing Strategy

Luồng xử lý chính:

1. Đọc file CSV theo streaming (line by line), không load toàn bộ file vào memory.
2. Parse từng record và chuẩn hóa số liệu ngay lúc đọc.
3. Aggregate trực tiếp vào map theo `campaign_id` trong lúc parsing.
4. Sau khi đọc xong mới tính CTR/CPA và lấy top 10.
5. Ghi CSV output.

## Bottleneck Và Quyết Định Tối Ưu

Bottleneck lớn nhất của bài toán là **đọc + parse file CSV dung lượng lớn**.  
Nếu parse xong mới gom nhóm ở một pass khác thì sẽ tốn thêm CPU cycles và tăng memory pressure do phải giữ dữ liệu trung gian.

Vì vậy mình chọn:

- **Đọc file nhanh bằng memory-mapped I/O (`mmap`)** để giảm overhead syscall/read loop so với đọc từng chunk nhỏ.
- **Chia dữ liệu theo chunk và parse song song theo số core** (`workers ~= GOMAXPROCS`), mỗi worker xử lý một vùng byte độc lập theo ranh giới dòng.
- **Parsing nhanh theo byte-level scanner** (`parseLineToCompactTrusted`) thay vì parser CSV tổng quát, vì input format cố định và sạch.
- **Lưu tiền ở đơn vị cents (`int64`)** để tránh chi phí float khi parse và cộng dồn.
- **Aggregate ngay trong lúc parsing** (single pass), không tạo slice record trung gian.
- **Dùng local aggregator per worker rồi merge cuối** để giảm contention/lock trong hot path.

Lợi ích:

- Tăng throughput đọc + parse trên file lớn.
- Giảm CPU cycles do bỏ bớt lớp parse/convert dư thừa.
- Giảm allocation, giảm GC pressure, giữ peak memory thấp.

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

- App có log benchmark chi tiết theo từng run và summary cuối.
- Có thể thay đổi `-runs`/`-warmup` để lấy số đo ổn định hơn tùy môi trường máy.
