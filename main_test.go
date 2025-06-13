package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
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
				Bucket:   "test-bucket",
				Count:    100,
				Rate:     10,
				LogLevel: "info",
				FileSize: 1024,
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

func TestProgressCalculation(t *testing.T) {
	tests := []struct {
		name         string
		total        int
		current      int
		expectedStep int
	}{
		{"100 files", 100, 50, 1},
		{"1000 files", 1000, 500, 10},
		{"50 files", 50, 25, 1},
		{"10 files", 10, 5, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := calculateProgressStep(tt.total)
			assert.Equal(t, tt.expectedStep, step)
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
		Bucket:   "test-bucket",
		Count:    5,
		Rate:     0,
		FileSize: 100,
	}

	ctx := context.Background()
	err := populateS3WithClient(ctx, mockClient, cfg)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "errors")
	mockClient.AssertExpectations(t)
}

func TestFileGeneration(t *testing.T) {
	data1 := generateRandomData(1024)
	data2 := generateRandomData(1024)

	assert.Len(t, data1, 1024)
	assert.Len(t, data2, 1024)
	assert.NotEqual(t, data1, data2, "Random data should be different each time")
}

func TestKeyGeneration(t *testing.T) {
	tests := []struct {
		name     string
		index    int
		depth    int
		expected string
	}{
		{"no depth", 1, 0, "file-000001.dat"},
		{"no depth large", 42, 0, "file-000042.dat"},
		{"depth 1", 1, 1, "dir0/file-000001.dat"},
		{"depth 1 index 2", 2, 1, "dir3/file-000002.dat"},
		{"depth 1 index 3", 3, 1, "dir2/file-000003.dat"},
		{"depth 1 index 4", 4, 1, "dir1/file-000004.dat"},
		{"depth 3", 1, 3, "dir0/dir0/dir0/file-000001.dat"},
		{"depth 3 index 5", 5, 3, "dir0/dir3/dir2/file-000005.dat"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := generateFileKey(tt.index, tt.depth)
			assert.Equal(t, tt.expected, key)
		})
	}
}

func TestDirectoryDistribution(t *testing.T) {
	dirCounts := make(map[string]int)

	for i := 1; i <= 100; i++ {
		key := generateFileKey(i, 1)
		parts := strings.Split(key, "/")
		if len(parts) >= 1 {
			dirCounts[parts[0]]++
		}
	}

	assert.Len(t, dirCounts, 4, "Should have exactly 4 different directories")

	for dir, count := range dirCounts {
		assert.Contains(t, []string{"dir0", "dir1", "dir2", "dir3"}, dir)
		assert.Greater(t, count, 0, "Each directory should have at least one file")
	}
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
	return nil
}

func calculateProgressStep(total int) int {
	step := total / 100
	if step == 0 {
		step = 1
	}
	return step
}

func populateS3WithClient(ctx context.Context, client S3Client, cfg Config) error {
	if cfg.Count == 0 {
		return nil
	}

	var uploaded, errors int
	fileData := generateRandomData(cfg.FileSize)

	for i := 0; i < cfg.Count; i++ {
		key := generateFileKey(i+1, cfg.Depth)

		if err := uploadFileWithClient(ctx, client, cfg.Bucket, key, fileData); err != nil {
			errors++
		} else {
			uploaded++
		}
	}

	if errors > 0 {
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
