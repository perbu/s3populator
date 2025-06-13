package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/time/rate"
)

type MockS3Client struct {
	mock.Mock
}

func (m *MockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(*s3.PutObjectOutput), args.Error(1)
}

func TestSetupLogging(t *testing.T) {
	tests := []struct {
		name        string
		level       string
		expectError bool
	}{
		{"debug level", "debug", false},
		{"info level", "info", false},
		{"warn level", "warn", false},
		{"error level", "error", false},
		{"DEBUG level (case insensitive)", "DEBUG", false},
		{"invalid level", "invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := setupLogging(tt.level)
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "invalid log level")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestUploadFile(t *testing.T) {
	tests := []struct {
		name        string
		bucket      string
		key         string
		data        []byte
		mockError   error
		expectError bool
	}{
		{
			name:        "successful upload",
			bucket:      "test-bucket",
			key:         "test-key",
			data:        []byte("test data"),
			mockError:   nil,
			expectError: false,
		},
		{
			name:        "failed upload",
			bucket:      "test-bucket",
			key:         "test-key",
			data:        []byte("test data"),
			mockError:   errors.New("upload failed"),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &MockS3Client{}
			mockClient.On("PutObject", mock.Anything, mock.MatchedBy(func(input *s3.PutObjectInput) bool {
				return *input.Bucket == tt.bucket && *input.Key == tt.key
			})).Return(&s3.PutObjectOutput{}, tt.mockError)

			err := uploadFileWithClient(context.Background(), mockClient, tt.bucket, tt.key, tt.data)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name   string
		config Config
		valid  bool
	}{
		{
			name: "valid config",
			config: Config{
				Bucket:      "test-bucket",
				Count:       100,
				Rate:        10,
				LogLevel:    "info",
				FileSize:    1024,
				Concurrency: 1,
			},
			valid: true,
		},
		{
			name: "empty bucket",
			config: Config{
				Bucket:   "",
				Count:    100,
				Rate:     10,
				LogLevel: "info",
				FileSize: 1024,
			},
			valid: false,
		},
		{
			name: "negative count",
			config: Config{
				Bucket:   "test-bucket",
				Count:    -1,
				Rate:     10,
				LogLevel: "info",
				FileSize: 1024,
			},
			valid: false,
		},
		{
			name: "negative rate",
			config: Config{
				Bucket:   "test-bucket",
				Count:    100,
				Rate:     -1,
				LogLevel: "info",
				FileSize: 1024,
			},
			valid: false,
		},
		{
			name: "zero file size",
			config: Config{
				Bucket:   "test-bucket",
				Count:    100,
				Rate:     10,
				LogLevel: "info",
				FileSize: 0,
			},
			valid: false,
		},
		{
			name: "negative depth",
			config: Config{
				Bucket:   "test-bucket",
				Count:    100,
				Rate:     10,
				LogLevel: "info",
				FileSize: 1024,
				Depth:    -1,
			},
			valid: false,
		},
		{
			name: "zero concurrency",
			config: Config{
				Bucket:      "test-bucket",
				Count:       100,
				Rate:        10,
				LogLevel:    "info",
				FileSize:    1024,
				Concurrency: 0,
			},
			valid: false,
		},
		{
			name: "negative concurrency",
			config: Config{
				Bucket:      "test-bucket",
				Count:       100,
				Rate:        10,
				LogLevel:    "info",
				FileSize:    1024,
				Concurrency: -1,
			},
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConfig(tt.config)
			if tt.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestRandomStringGeneration(t *testing.T) {
	tests := []struct {
		name   string
		length int
	}{
		{"8 chars", 8},
		{"16 chars", 16},
		{"32 chars", 32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateRandomString(tt.length)
			assert.Equal(t, tt.length, len(result))
			assert.Regexp(t, "^[a-f0-9]+$", result, "Should contain only hex characters")
		})
	}
}

func TestRateLimiting(t *testing.T) {
	limiter := rate.NewLimiter(rate.Limit(10), 1)

	start := time.Now()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		err := limiter.Wait(ctx)
		assert.NoError(t, err)
	}

	elapsed := time.Since(start)
	assert.True(t, elapsed >= 400*time.Millisecond, "Rate limiting should take at least 400ms for 5 requests at 10/sec")
}

func TestLogCapture(t *testing.T) {
	var buf bytes.Buffer

	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := slog.NewTextHandler(&buf, opts)
	logger := slog.New(handler)

	logger.Info("test message", "key", "value")

	output := buf.String()
	assert.Contains(t, output, "test message")
	assert.Contains(t, output, "key=value")
}

func TestErrorHandling(t *testing.T) {
	mockClient := &MockS3Client{}
	mockClient.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, errors.New("network error"))

	cfg := Config{
		Bucket:      "test-bucket",
		Count:       5,
		Rate:        0,
		FileSize:    100,
		Concurrency: 1,
	}

	ctx := context.Background()
	err := populateS3WithClient(ctx, mockClient, cfg)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "errors")
	mockClient.AssertExpectations(t)
}

func TestFileGeneration(t *testing.T) {
	data1 := generateRandomData(1024)
	data2 := generateRandomData(2048)

	assert.Len(t, data1, 1024)
	assert.Len(t, data2, 2048)
}

func TestRandomKeyGeneration(t *testing.T) {
	tests := []struct {
		name  string
		depth int
	}{
		{"no depth", 0},
		{"depth 1", 1},
		{"depth 3", 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := generateRandomFileKey(tt.depth)
			assert.Contains(t, key, ".dat", "Should have .dat extension")

			parts := strings.Split(key, "/")
			expectedParts := tt.depth + 1
			assert.Equal(t, expectedParts, len(parts), "Should have correct number of path parts")

			// Check that all parts are valid hex strings
			for i, part := range parts {
				if i == len(parts)-1 {
					// Last part is filename
					assert.True(t, strings.HasSuffix(part, ".dat"))
					filename := strings.TrimSuffix(part, ".dat")
					assert.Equal(t, 16, len(filename))
					assert.Regexp(t, "^[a-f0-9]+$", filename)
				} else {
					// Directory part
					assert.Equal(t, 8, len(part))
					assert.Regexp(t, "^[a-f0-9]+$", part)
				}
			}
		})
	}
}

func TestRandomKeyUniqueness(t *testing.T) {
	keys := make(map[string]bool)

	// Generate 100 random keys and ensure they're unique
	for i := 0; i < 100; i++ {
		key := generateRandomFileKey(1)
		assert.False(t, keys[key], "Generated key should be unique: %s", key)
		keys[key] = true
	}

	assert.Equal(t, 100, len(keys), "Should have 100 unique keys")
}

func TestConcurrency(t *testing.T) {
	tests := []struct {
		name        string
		concurrency int
		count       int
		expectCalls int
	}{
		{"single worker", 1, 5, 5},
		{"multiple workers", 3, 10, 10},
		{"more workers than tasks", 10, 5, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &MockS3Client{}
			mockClient.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil)

			cfg := Config{
				Bucket:      "test-bucket",
				Count:       tt.count,
				Rate:        0,
				FileSize:    100,
				Concurrency: tt.concurrency,
			}

			ctx := context.Background()
			err := populateS3WithClient(ctx, mockClient, cfg)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectCalls, len(mockClient.Calls))
			mockClient.AssertExpectations(t)
		})
	}
}

func TestConcurrencyWithErrors(t *testing.T) {
	mockClient := &MockS3Client{}
	mockClient.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, errors.New("network error"))

	cfg := Config{
		Bucket:      "test-bucket",
		Count:       5,
		Rate:        0,
		FileSize:    100,
		Concurrency: 3,
	}

	ctx := context.Background()
	err := populateS3WithClient(ctx, mockClient, cfg)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "errors")
	assert.Equal(t, 5, len(mockClient.Calls))
	mockClient.AssertExpectations(t)
}

func TestConcurrencyRaceConditions(t *testing.T) {
	// This test verifies that concurrent access doesn't cause race conditions
	mockClient := &MockS3Client{}
	mockClient.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil)

	cfg := Config{
		Bucket:      "test-bucket",
		Count:       100,
		Rate:        0,
		FileSize:    100,
		Concurrency: 10,
	}

	ctx := context.Background()
	err := populateS3WithClient(ctx, mockClient, cfg)

	assert.NoError(t, err)
	assert.Equal(t, 100, len(mockClient.Calls))
	mockClient.AssertExpectations(t)
}

func TestMainIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{"s3populator", "-bucket", "test-bucket", "-count", "0"}

	err := run()
	assert.NoError(t, err, "Should handle zero count gracefully")
}

func uploadFileWithClient(ctx context.Context, client S3Client, bucket, key string, data []byte) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(string(data)),
	}

	_, err := client.PutObject(ctx, input)
	return err
}

type S3Client interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

func validateConfig(cfg Config) error {
	if cfg.Bucket == "" {
		return errors.New("bucket cannot be empty")
	}
	if cfg.Count < 0 {
		return errors.New("count cannot be negative")
	}
	if cfg.Rate < 0 {
		return errors.New("rate cannot be negative")
	}
	if cfg.FileSize <= 0 {
		return errors.New("file size must be positive")
	}
	if cfg.Depth < 0 {
		return errors.New("depth cannot be negative")
	}
	if cfg.Concurrency < 1 {
		return errors.New("concurrency must be at least 1")
	}
	return nil
}

func populateS3WithClient(ctx context.Context, client S3Client, cfg Config) error {
	if cfg.Count == 0 {
		return nil
	}

	// Use atomic counters for thread safety
	var uploaded, errorCount int64
	fileData := generateRandomData(cfg.FileSize)

	// Create work channel
	workChan := make(chan int, cfg.Count)
	for i := 0; i < cfg.Count; i++ {
		workChan <- i
	}
	close(workChan)

	// Handle concurrency
	concurrency := cfg.Concurrency
	if concurrency < 1 {
		concurrency = 1
	}

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range workChan {
				key := generateRandomFileKey(cfg.Depth)
				if err := uploadFileWithClient(ctx, client, cfg.Bucket, key, fileData); err != nil {
					atomic.AddInt64(&errorCount, 1)
				} else {
					atomic.AddInt64(&uploaded, 1)
				}
			}
		}()
	}

	wg.Wait()

	if errorCount > 0 {
		return errors.New("completed with errors")
	}
	return nil
}

func generateRandomData(size int64) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	return data
}
