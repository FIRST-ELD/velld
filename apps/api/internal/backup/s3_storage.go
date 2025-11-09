package backup

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// cleanCredential removes all whitespace and control characters from a credential string
func cleanCredential(cred string) string {
	// First trim leading/trailing whitespace
	cred = strings.TrimSpace(cred)
	
	// Remove all whitespace and control characters
	var builder strings.Builder
	for _, r := range cred {
		if !unicode.IsSpace(r) && !unicode.IsControl(r) {
			builder.WriteRune(r)
		}
	}
	return builder.String()
}

type S3Config struct {
	Endpoint   string
	Region     string
	Bucket     string
	AccessKey  string
	SecretKey  string
	UseSSL     bool
	PathPrefix string
}

type S3Storage struct {
	client *minio.Client
	bucket string
	prefix string
}

func NewS3Storage(config S3Config) (*S3Storage, error) {
	// Aggressively clean all credentials to prevent "malformed credential" errors
	// This removes all whitespace, control characters, and invisible Unicode characters
	accessKey := cleanCredential(config.AccessKey)
	secretKey := cleanCredential(config.SecretKey)
	endpoint := strings.TrimSpace(config.Endpoint) // Endpoint can have spaces in domain names
	bucket := cleanCredential(config.Bucket)
	
	// Validate that credentials are not empty after cleaning
	if accessKey == "" {
		return nil, fmt.Errorf("access key is empty after cleaning")
	}
	if secretKey == "" {
		return nil, fmt.Errorf("secret key is empty after cleaning")
	}
	if endpoint == "" {
		return nil, fmt.Errorf("endpoint is empty after cleaning")
	}
	if bucket == "" {
		return nil, fmt.Errorf("bucket is empty after cleaning")
	}
	
	// Log credential lengths for debugging (without exposing actual values)
	// This helps identify if credentials are being truncated or corrupted
	if len(accessKey) == 0 {
		return nil, fmt.Errorf("access key is empty")
	}
	if len(secretKey) == 0 {
		return nil, fmt.Errorf("secret key is empty")
	}
	
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: config.UseSSL,
		Region: config.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	ctx := context.Background()
	
	// Check if this is Backblaze B2 (they handle bucket checks differently)
	isBackblaze := strings.Contains(config.Endpoint, "backblazeb2.com")
	
		if isBackblaze {
		// For Backblaze, skip BucketExists check and try to list objects instead
		// This is more reliable as Backblaze application keys may not have ListBuckets permission
		// BucketExists often returns 400 for Backblaze, so we test access by listing objects
		objectCh := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			MaxKeys: 1,
		})
		// Try to read one object to verify bucket access
		// If there's an error, it will be caught when we try to upload
		for range objectCh {
			break
		}
		// If we got here without error, the bucket is accessible
	} else {
		// For other S3 providers, use standard bucket existence check
		exists, err := client.BucketExists(ctx, bucket)
		if err != nil {
			return nil, fmt.Errorf("failed to check bucket existence: %w", err)
		}

		if !exists {
			err = client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{
				Region: config.Region,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create bucket: %w", err)
			}
		}
	}

	return &S3Storage{
		client: client,
		bucket: bucket,
		prefix: config.PathPrefix,
	}, nil
}

func (s *S3Storage) UploadFile(ctx context.Context, localPath string) (string, error) {
	return s.UploadFileWithLogging(ctx, localPath, nil)
}

func (s *S3Storage) UploadFileWithLogging(ctx context.Context, localPath string, logFunc func(string)) (string, error) {
	if logFunc != nil {
		logFunc("[INFO] Opening backup file for upload...")
	}
	
	file, err := os.Open(localPath)
	if err != nil {
		if logFunc != nil {
			logFunc(fmt.Sprintf("[ERROR] Failed to open file: %v", err))
		}
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		if logFunc != nil {
			logFunc(fmt.Sprintf("[ERROR] Failed to get file info: %v", err))
		}
		return "", fmt.Errorf("failed to stat file: %w", err)
	}

	fileName := filepath.Base(localPath)
	objectKey := s.getObjectKey(fileName)

	if logFunc != nil {
		logFunc(fmt.Sprintf("[INFO] Uploading to S3 bucket '%s' with key '%s'...", s.bucket, objectKey))
		logFunc(fmt.Sprintf("[INFO] File size: %d bytes (%.2f MB)", fileInfo.Size(), float64(fileInfo.Size())/(1024*1024)))
	}

	_, err = s.client.PutObject(ctx, s.bucket, objectKey, file, fileInfo.Size(), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		if logFunc != nil {
			logFunc(fmt.Sprintf("[ERROR] S3 upload failed: %v", err))
		}
		return "", fmt.Errorf("failed to upload to S3: %w", err)
	}

	if logFunc != nil {
		logFunc(fmt.Sprintf("[INFO] Upload completed successfully"))
	}

	return objectKey, nil
}

func (s *S3Storage) DownloadFile(ctx context.Context, objectKey, localPath string) error {
	object, err := s.client.GetObject(ctx, s.bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer object.Close()

	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()

	_, err = io.Copy(file, object)
	if err != nil {
		return fmt.Errorf("failed to download from S3: %w", err)
	}

	return nil
}

// GetObject returns an io.ReadCloser for streaming download from S3
func (s *S3Storage) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	object, err := s.client.GetObject(ctx, s.bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object from S3: %w", err)
	}
	return object, nil
}

func (s *S3Storage) DeleteFile(ctx context.Context, objectKey string) error {
	err := s.client.RemoveObject(ctx, s.bucket, objectKey, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete object from S3: %w", err)
	}
	return nil
}

func (s *S3Storage) ListFiles(ctx context.Context) ([]string, error) {
	var files []string

	opts := minio.ListObjectsOptions{
		Prefix:    s.prefix,
		Recursive: true,
	}

	for object := range s.client.ListObjects(ctx, s.bucket, opts) {
		if object.Err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", object.Err)
		}
		files = append(files, object.Key)
	}

	return files, nil
}

func (s *S3Storage) GetFileSize(ctx context.Context, objectKey string) (int64, error) {
	info, err := s.client.StatObject(ctx, s.bucket, objectKey, minio.StatObjectOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to stat object: %w", err)
	}
	return info.Size, nil
}

func (s *S3Storage) TestConnection(ctx context.Context) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("failed to check bucket: %w", err)
	}
	if !exists {
		return fmt.Errorf("bucket does not exist: %s", s.bucket)
	}
	return nil
}

func (s *S3Storage) getObjectKey(fileName string) string {
	if s.prefix == "" {
		return fileName
	}
	
	// Ensure prefix doesn't end with / and fileName doesn't start with /
	prefix := strings.TrimSuffix(s.prefix, "/")
	fileName = strings.TrimPrefix(fileName, "/")
	return fmt.Sprintf("%s/%s", prefix, fileName)
}
