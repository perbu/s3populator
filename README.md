# S3 Populator

CLI tool to create N files in S3 or S3-compatible storage with optional rate limiting.

## Usage

```bash
go run main.go -bucket BUCKET [options]
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-bucket` | **required** | S3 bucket name |
| `-count` | `100` | Number of files to create |
| `-rate` | `0` | Rate limit (requests/sec, 0 = unlimited) |
| `-file-size` | `1024` | Size of each file in bytes |
| `-depth` | `0` | Folder depth (number of slashes in object key) |
| `-log-level` | `info` | Log level (debug, info, warn, error) |
| `-endpoint` | | S3 endpoint URL for compatible storage |
| `-region` | `us-east-1` | AWS region |

## Examples

```bash
# Basic usage
go run main.go -bucket my-bucket -count 1000

# With rate limiting
go run main.go -bucket my-bucket -count 5000 -rate 50

# S3-compatible storage (MinIO)
go run main.go -bucket my-bucket -endpoint https://minio.example.com -count 100

# Debug logging
go run main.go -bucket my-bucket -count 10 -log-level debug

# Create folder structure (dir0-dir3 at each level)
go run main.go -bucket my-bucket -count 100 -depth 3
```

## Output

- Progress logged every 1% of completion
- Final summary includes upload count, errors, duration, and rate
- Exits with code 1 on failure

## Testing

```bash
go test -v
go test -short  # Skip integration tests
```