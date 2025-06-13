package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/time/rate"
)

type Config struct {
	Bucket   string
	Count    int
	Rate     int
	LogLevel string
	Endpoint string
	Region   string
	FileSize int64
	Depth    int
}

func main() {
	if err := run(); err != nil {
		slog.Error("Application failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := parseFlags()

	if err := setupLogging(cfg.LogLevel); err != nil {
		return fmt.Errorf("failed to setup logging: %w", err)
	}

	slog.Debug("Starting S3 populator", "config", cfg)

	ctx := context.Background()
	s3Client, err := createS3Client(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	return populateS3(ctx, s3Client, cfg)
}

func parseFlags() Config {
	var cfg Config

	flag.StringVar(&cfg.Bucket, "bucket", "", "S3 bucket name (required)")
	flag.IntVar(&cfg.Count, "count", 100, "Number of files to create")
	flag.IntVar(&cfg.Rate, "rate", 0, "Rate limit (requests per second, 0 = unlimited)")
	flag.StringVar(&cfg.LogLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	flag.StringVar(&cfg.Endpoint, "endpoint", "", "S3 endpoint URL (for S3-compatible storage)")
	flag.StringVar(&cfg.Region, "region", "us-east-1", "AWS region")
	flag.Int64Var(&cfg.FileSize, "file-size", 1024, "Size of each file in bytes")
	flag.IntVar(&cfg.Depth, "depth", 0, "Folder depth (number of slashes in object key, 0 = no folders)")

	flag.Parse()

	if cfg.Bucket == "" {
		fmt.Fprintf(os.Stderr, "Error: -bucket is required\n")
		flag.Usage()
		os.Exit(1)
	}

	return cfg
}

func setupLogging(level string) error {
	var logLevel slog.Level

	switch strings.ToLower(level) {
	case "debug":
		logLevel = slog.LevelDebug
	case "info":
		logLevel = slog.LevelInfo
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		return fmt.Errorf("invalid log level: %s", level)
	}

	opts := &slog.HandlerOptions{
		Level: logLevel,
	}

	handler := slog.NewTextHandler(os.Stdout, opts)
	logger := slog.New(handler)
	slog.SetDefault(logger)

	return nil
}

func createS3Client(ctx context.Context, cfg Config) (*s3.Client, error) {
	slog.Debug("Creating S3 client", "region", cfg.Region, "endpoint", cfg.Endpoint)

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	var s3Client *s3.Client
	if cfg.Endpoint != "" {
		s3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		})
	} else {
		s3Client = s3.NewFromConfig(awsCfg)
	}

	return s3Client, nil
}

func populateS3(ctx context.Context, s3Client *s3.Client, cfg Config) error {
	slog.Info("Starting S3 population",
		"bucket", cfg.Bucket,
		"count", cfg.Count,
		"rate", cfg.Rate,
		"file_size", cfg.FileSize)

	var limiter *rate.Limiter
	if cfg.Rate > 0 {
		limiter = rate.NewLimiter(rate.Limit(cfg.Rate), 1)
		slog.Debug("Rate limiter configured", "rate", cfg.Rate)
	}

	var uploaded, errors int
	progressStep := cfg.Count / 100
	if progressStep == 0 {
		progressStep = 1
	}

	fileData := make([]byte, cfg.FileSize)
	if _, err := rand.Read(fileData); err != nil {
		return fmt.Errorf("failed to generate random data: %w", err)
	}

	start := time.Now()

	for i := 0; i < cfg.Count; i++ {
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return fmt.Errorf("rate limiter error: %w", err)
			}
		}

		key := generateFileKey(i+1, cfg.Depth)

		slog.Debug("Uploading file", "key", key, "size", cfg.FileSize)

		if err := uploadFile(ctx, s3Client, cfg.Bucket, key, fileData); err != nil {
			slog.Error("Failed to upload file", "key", key, "error", err)
			errors++
		} else {
			uploaded++
			slog.Debug("Successfully uploaded file", "key", key)
		}

		if (i+1)%progressStep == 0 || i+1 == cfg.Count {
			progress := float64(i+1) / float64(cfg.Count) * 100
			elapsed := time.Since(start)
			slog.Info("Progress update",
				"progress", fmt.Sprintf("%.1f%%", progress),
				"uploaded", uploaded,
				"errors", errors,
				"elapsed", elapsed.Round(time.Second))
		}
	}

	duration := time.Since(start)
	slog.Info("S3 population completed",
		"total_files", cfg.Count,
		"uploaded", uploaded,
		"errors", errors,
		"duration", duration.Round(time.Second),
		"rate", fmt.Sprintf("%.2f files/sec", float64(cfg.Count)/duration.Seconds()))

	if errors > 0 {
		return fmt.Errorf("completed with %d errors out of %d files", errors, cfg.Count)
	}

	return nil
}

func generateFileKey(index, depth int) string {
	if depth == 0 {
		return fmt.Sprintf("file-%06d.dat", index)
	}

	var pathParts []string
	seed := index
	for i := 0; i < depth; i++ {
		dirNum := (seed*31 + i) % 4
		pathParts = append(pathParts, fmt.Sprintf("dir%d", dirNum))
		seed = seed*7 + dirNum
	}
	pathParts = append(pathParts, fmt.Sprintf("file-%06d.dat", index))

	return strings.Join(pathParts, "/")
}

func uploadFile(ctx context.Context, s3Client *s3.Client, bucket, key string, data []byte) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(string(data)),
	}

	_, err := s3Client.PutObject(ctx, input)
	return err
}
