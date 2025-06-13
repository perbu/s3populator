package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/time/rate"
)

type Config struct {
	Bucket       string
	Count        int
	Rate         int
	LogLevel     string
	Endpoint     string
	Region       string
	FileSize     int64
	Depth        int
	KeyID        string
	SecretKey    string
	SessionToken string
	Concurrency  int
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
	flag.StringVar(&cfg.KeyID, "key-id", "", "AWS Access Key ID (optional, uses AWS config if not provided)")
	flag.StringVar(&cfg.SecretKey, "secret-key", "", "AWS Secret Access Key (optional, uses AWS config if not provided)")
	flag.StringVar(&cfg.SessionToken, "session-token", "", "AWS Session Token (optional, for temporary credentials)")
	flag.IntVar(&cfg.Concurrency, "concurrency", 1, "Number of concurrent uploads (default: 1, no concurrency)")

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

	var awsCfg aws.Config
	var err error

	if cfg.KeyID != "" && cfg.SecretKey != "" {
		slog.Debug("Using provided AWS credentials")
		creds := credentials.NewStaticCredentialsProvider(cfg.KeyID, cfg.SecretKey, cfg.SessionToken)
		awsCfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(cfg.Region),
			config.WithCredentialsProvider(creds),
		)
	} else {
		slog.Debug("Using default AWS configuration")
		awsCfg, err = config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
	}

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
		"file_size", cfg.FileSize,
		"concurrency", cfg.Concurrency)

	var limiter *rate.Limiter
	if cfg.Rate > 0 {
		// With concurrency, we need to adjust the rate limiter
		// The burst should accommodate concurrent requests
		burst := cfg.Concurrency
		if burst < 1 {
			burst = 1
		}
		limiter = rate.NewLimiter(rate.Limit(cfg.Rate), burst)
		slog.Debug("Rate limiter configured", "rate", cfg.Rate, "burst", burst)
	}

	// Thread-safe counters
	var uploaded, errors int64
	var processed int64

	lastProgressReport := time.Now()
	progressInterval := 10 * time.Second
	var progressMutex sync.Mutex

	fileData := make([]byte, cfg.FileSize)
	if _, err := rand.Read(fileData); err != nil {
		return fmt.Errorf("failed to generate random data: %w", err)
	}

	start := time.Now()

	// Create work channel
	workChan := make(chan int, cfg.Count)

	// Fill work channel
	for i := 0; i < cfg.Count; i++ {
		workChan <- i
	}
	close(workChan)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for range workChan {
				if limiter != nil {
					if err := limiter.Wait(ctx); err != nil {
						slog.Error("Rate limiter error", "worker", workerID, "error", err)
						atomic.AddInt64(&errors, 1)
						continue
					}
				}

				key := generateRandomFileKey(cfg.Depth)
				slog.Debug("Uploading file", "worker", workerID, "key", key, "size", cfg.FileSize)

				if err := uploadFile(ctx, s3Client, cfg.Bucket, key, fileData); err != nil {
					slog.Error("Failed to upload file", "worker", workerID, "key", key, "error", err)
					atomic.AddInt64(&errors, 1)
				} else {
					atomic.AddInt64(&uploaded, 1)
					slog.Debug("Successfully uploaded file", "worker", workerID, "key", key)
				}

				currentProcessed := atomic.AddInt64(&processed, 1)

				// Check if we need to report progress (thread-safe)
				progressMutex.Lock()
				if time.Since(lastProgressReport) >= progressInterval || int(currentProcessed) == cfg.Count {
					progress := float64(currentProcessed) / float64(cfg.Count) * 100
					elapsed := time.Since(start)
					currentUploaded := atomic.LoadInt64(&uploaded)
					currentErrors := atomic.LoadInt64(&errors)
					uploadRate := float64(currentUploaded) / elapsed.Seconds()

					slog.Info("Progress update",
						"progress", fmt.Sprintf("%.1f%%", progress),
						"uploaded", currentUploaded,
						"errors", currentErrors,
						"elapsed", elapsed.Round(time.Second),
						"rate", fmt.Sprintf("%.2f files/sec", uploadRate))
					lastProgressReport = time.Now()
				}
				progressMutex.Unlock()
			}
		}(i)
	}

	// Wait for all workers to complete
	wg.Wait()

	duration := time.Since(start)
	finalUploaded := atomic.LoadInt64(&uploaded)
	finalErrors := atomic.LoadInt64(&errors)

	slog.Info("S3 population completed",
		"total_files", cfg.Count,
		"uploaded", finalUploaded,
		"errors", finalErrors,
		"duration", duration.Round(time.Second),
		"rate", fmt.Sprintf("%.2f files/sec", float64(finalUploaded)/duration.Seconds()),
		"concurrency", cfg.Concurrency)

	if finalErrors > 0 {
		return fmt.Errorf("completed with %d errors out of %d files", finalErrors, cfg.Count)
	}

	return nil
}

func generateRandomFileKey(depth int) string {
	if depth == 0 {
		return generateRandomString(16) + ".dat"
	}

	var pathParts []string
	for i := 0; i < depth; i++ {
		pathParts = append(pathParts, generateRandomString(8))
	}
	pathParts = append(pathParts, generateRandomString(16)+".dat")

	return strings.Join(pathParts, "/")
}

func generateRandomString(length int) string {
	bytes := make([]byte, length/2)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
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
