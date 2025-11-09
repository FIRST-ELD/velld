package settings

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/dendianugerah/velld/internal/common"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type SettingsService struct {
	repo          *SettingsRepository
	cryptoService *common.EncryptionService
}

func NewSettingsService(repo *SettingsRepository, crypto *common.EncryptionService) *SettingsService {
	return &SettingsService{
		repo:          repo,
		cryptoService: crypto,
	}
}

func (s *SettingsService) GetUserSettings(userID uuid.UUID) (*UserSettings, error) {
	settings, err := s.repo.GetUserSettings(userID)
	if err != nil {
		return nil, err
	}

	// Apply environment variable overrides (env vars take precedence and are read-only in UI)
	s.applyDefaults(settings)

	// Remove sensitive data before returning
	settings.SMTPPassword = nil
	settings.S3SecretKey = nil
	return settings, nil
}

func (s *SettingsService) GetUserSettingsInternal(userID uuid.UUID) (*UserSettings, error) {
	settings, err := s.repo.GetUserSettings(userID)
	if err != nil {
		return nil, err
	}

	s.applyDefaults(settings)

	return settings, nil
}

func (s *SettingsService) applyDefaults(settings *UserSettings) {
	settings.EnvConfigured = make(map[string]bool)

	if smtpHost := os.Getenv("SMTP_HOST"); smtpHost != "" {
		settings.SMTPHost = &smtpHost
		settings.EnvConfigured["smtp_host"] = true
	}

	if smtpPortStr := os.Getenv("SMTP_PORT"); smtpPortStr != "" {
		if port, err := strconv.Atoi(smtpPortStr); err == nil {
			settings.SMTPPort = &port
			settings.EnvConfigured["smtp_port"] = true
		}
	}

	if smtpUser := os.Getenv("SMTP_USER"); smtpUser != "" {
		settings.SMTPUsername = &smtpUser
		settings.EnvConfigured["smtp_username"] = true
	}

	if smtpPass := os.Getenv("SMTP_PASSWORD"); smtpPass != "" {
		settings.SMTPPassword = &smtpPass
		settings.EnvConfigured["smtp_password"] = true
	}

	if smtpFrom := os.Getenv("SMTP_FROM"); smtpFrom != "" {
		settings.Email = &smtpFrom
		settings.EnvConfigured["email"] = true
	}
}

func (s *SettingsService) UpdateUserSettings(userID uuid.UUID, req *UpdateSettingsRequest) (*UserSettings, error) {
	settings, err := s.repo.GetUserSettings(userID)
	if err != nil {
		return nil, err
	}

	envSMTPHost := os.Getenv("SMTP_HOST") != ""
	envSMTPPort := os.Getenv("SMTP_PORT") != ""
	envSMTPUser := os.Getenv("SMTP_USER") != ""
	envSMTPPass := os.Getenv("SMTP_PASSWORD") != ""
	envSMTPFrom := os.Getenv("SMTP_FROM") != ""

	if req.NotifyDashboard != nil {
		settings.NotifyDashboard = *req.NotifyDashboard
	}
	if req.NotifyEmail != nil {
		settings.NotifyEmail = *req.NotifyEmail
	}
	if req.NotifyWebhook != nil {
		settings.NotifyWebhook = *req.NotifyWebhook
	}
	if req.WebhookURL != nil {
		settings.WebhookURL = req.WebhookURL
	}
	if req.Email != nil && !envSMTPFrom {
		settings.Email = req.Email
	}
	if req.SMTPHost != nil && !envSMTPHost {
		settings.SMTPHost = req.SMTPHost
	}
	if req.SMTPPort != nil && !envSMTPPort {
		settings.SMTPPort = req.SMTPPort
	}
	if req.SMTPUsername != nil && !envSMTPUser {
		settings.SMTPUsername = req.SMTPUsername
	}
	if req.SMTPPassword != nil && !envSMTPPass {
		// Encrypt SMTP password before storing
		encryptedPass, err := s.cryptoService.Encrypt(*req.SMTPPassword)
		if err != nil {
			return nil, err
		}
		settings.SMTPPassword = &encryptedPass
	}

	// Update S3 settings
	if req.S3Enabled != nil {
		settings.S3Enabled = *req.S3Enabled
	}
	if req.S3Endpoint != nil {
		settings.S3Endpoint = req.S3Endpoint
	}
	if req.S3Region != nil {
		settings.S3Region = req.S3Region
	}
	if req.S3Bucket != nil {
		settings.S3Bucket = req.S3Bucket
	}
	if req.S3AccessKey != nil {
		settings.S3AccessKey = req.S3AccessKey
	}
	if req.S3SecretKey != nil {
		// Only update secret key if a non-empty value is provided
		if *req.S3SecretKey != "" {
		// Encrypt S3 secret key before storing
		encryptedKey, err := s.cryptoService.Encrypt(*req.S3SecretKey)
		if err != nil {
			return nil, err
		}
		settings.S3SecretKey = &encryptedKey
		}
		// If empty string is provided, preserve existing secret key (don't clear it)
		// This allows users to update other settings without re-entering the secret key
		// settings.S3SecretKey already has the existing value from GetUserSettings, so we don't modify it
	}
	if req.S3UseSSL != nil {
		settings.S3UseSSL = *req.S3UseSSL
	}
	if req.S3PathPrefix != nil {
		settings.S3PathPrefix = req.S3PathPrefix
	}

	if err := s.repo.UpdateUserSettings(settings); err != nil {
		return nil, err
	}

	// Remove sensitive data before returning
	settings.SMTPPassword = nil
	settings.S3SecretKey = nil
	return settings, nil
}

// TestS3Connection tests the S3 connection with the provided credentials
func (s *SettingsService) TestS3Connection(req *TestS3ConnectionRequest) error {
	if req.Endpoint == "" {
		return fmt.Errorf("S3 endpoint is required")
	}
	if req.Bucket == "" {
		return fmt.Errorf("S3 bucket is required")
	}
	if req.AccessKey == "" {
		return fmt.Errorf("S3 access key is required")
	}
	if req.SecretKey == "" {
		return fmt.Errorf("S3 secret key is required")
	}

	// Trim whitespace from credentials (common issue with copy/paste)
	req.AccessKey = strings.TrimSpace(req.AccessKey)
	req.SecretKey = strings.TrimSpace(req.SecretKey)
	req.Endpoint = strings.TrimSpace(req.Endpoint)
	req.Bucket = strings.TrimSpace(req.Bucket)

	// Validate access key format for Backblaze
	if strings.Contains(req.Endpoint, "backblazeb2.com") {
		// Backblaze Application Key IDs are typically 24-25 characters, alphanumeric
		if len(req.AccessKey) < 20 || len(req.AccessKey) > 30 {
			return fmt.Errorf("Backblaze Application Key ID should be 20-30 characters. Current length: %d. Please check your Application Key ID", len(req.AccessKey))
		}
		// Check for common issues
		if strings.Contains(req.AccessKey, " ") {
			return fmt.Errorf("Access Key ID contains spaces. Please remove any spaces from your Application Key ID")
		}
		if strings.Contains(req.SecretKey, " ") {
			return fmt.Errorf("Secret Key contains spaces. Please remove any spaces from your Application Key")
		}
	}

	region := req.Region
	if region == "" {
		region = "us-east-1"
	}

	// Log credentials for debugging (mask secret key for security)
	maskedSecret := req.SecretKey
	if len(maskedSecret) > 8 {
		maskedSecret = maskedSecret[:4] + "****" + maskedSecret[len(maskedSecret)-4:]
	} else if len(maskedSecret) > 0 {
		maskedSecret = "****"
	}
	
	fmt.Printf("[S3 Test Connection] Testing with:\n")
	fmt.Printf("  Endpoint: %s\n", req.Endpoint)
	fmt.Printf("  Region: %s\n", region)
	fmt.Printf("  Bucket: %s\n", req.Bucket)
	fmt.Printf("  Access Key: %s (length: %d)\n", req.AccessKey, len(req.AccessKey))
	fmt.Printf("  Access Key (hex): %x\n", req.AccessKey) // Debug: show hex representation
	fmt.Printf("  Secret Key: %s (length: %d)\n", maskedSecret, len(req.SecretKey))
	fmt.Printf("  Use SSL: %t\n", req.UseSSL)
	fmt.Printf("  Path Prefix: %s\n", req.PathPrefix)

	// Check if this is Backblaze B2 and validate endpoint format
	isBackblaze := strings.Contains(req.Endpoint, "backblazeb2.com")
	if isBackblaze {
		// Backblaze B2 endpoint format: s3.<region>.backblazeb2.com
		// Region should match the endpoint region
		if !strings.HasPrefix(req.Endpoint, "s3.") || !strings.HasSuffix(req.Endpoint, ".backblazeb2.com") {
			fmt.Printf("[S3 Test Connection] Warning: Backblaze B2 endpoint format should be s3.<region>.backblazeb2.com\n")
		}
		// Extract region from endpoint if not provided
		if region == "us-east-1" && strings.Contains(req.Endpoint, "backblazeb2.com") {
			parts := strings.Split(req.Endpoint, ".")
			if len(parts) >= 2 {
				extractedRegion := parts[1]
				if extractedRegion != "" && extractedRegion != "backblazeb2" {
					region = extractedRegion
					fmt.Printf("[S3 Test Connection] Auto-detected Backblaze region: %s\n", region)
				}
			}
		}
		fmt.Printf("[S3 Test Connection] Using Backblaze B2 S3-Compatible API\n")
	}

	// Create S3 client directly to avoid import cycle
	// Backblaze B2 requires Signature Version 4, which we're using with StaticV4
	client, err := minio.New(req.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(req.AccessKey, req.SecretKey, ""),
		Secure: req.UseSSL,
		Region: region,
	})
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	ctx := context.Background()

	// For Backblaze, skip ListBuckets and go directly to bucket access test
	// Backblaze Application Keys may not have permission to list all buckets
	if isBackblaze {
		fmt.Printf("[S3 Test Connection] Skipping ListBuckets for Backblaze, testing bucket access directly\n")
	} else {
		// First, try to list buckets to verify credentials work
		_, err = client.ListBuckets(ctx)
		if err != nil {
			errStr := err.Error()
			fmt.Printf("[S3 Test Connection] ListBuckets error: %s\n", errStr)
			
			// Check for specific error types
			if strings.Contains(errStr, "Malformed Access Key Id") {
				return fmt.Errorf("authentication failed: Malformed Access Key ID. Please check your access key format")
			}
			if strings.Contains(errStr, "SignatureDoesNotMatch") || strings.Contains(errStr, "InvalidAccessKeyId") {
				return fmt.Errorf("authentication failed: Invalid credentials. Please check your access key and secret key: %w", err)
			}
			return fmt.Errorf("authentication failed: unable to list buckets. Please check your access key and secret key: %w", err)
		}
		fmt.Printf("[S3 Test Connection] ListBuckets succeeded\n")
	}

	// For Backblaze, test by listing objects directly (most reliable method)
	// This is what we actually need permission for
	fmt.Printf("[S3 Test Connection] Testing bucket access by listing objects...\n")
	opts := minio.ListObjectsOptions{
		MaxKeys: 1,
	}
	objectsCh := client.ListObjects(ctx, req.Bucket, opts)
	objectFound := false
	hasError := false
	var lastError error
	
	for object := range objectsCh {
		if object.Err != nil {
			hasError = true
			lastError = object.Err
			errStr := object.Err.Error()
			fmt.Printf("[S3 Test Connection] ListObjects error: %s\n", errStr)
			
			// Check for specific Backblaze errors
			if strings.Contains(errStr, "Malformed Access Key Id") {
				return fmt.Errorf("authentication failed: Malformed Access Key ID. For Backblaze B2, ensure you're using Application Key ID (not Master Key) from the 'App Keys' section. The key should be 24-25 characters. Current key length: %d", len(req.AccessKey))
			}
			if strings.Contains(errStr, "InvalidAccessKeyId") || strings.Contains(errStr, "SignatureDoesNotMatch") {
				return fmt.Errorf("authentication failed: Invalid credentials. For Backblaze B2, ensure you're using Application Key ID and Application Key (not Master Application Key) from the 'App Keys' section")
			}
			if strings.Contains(errStr, "Access Denied") || strings.Contains(errStr, "access denied") || strings.Contains(errStr, "AccessDenied") {
				return fmt.Errorf("credentials are valid, but you don't have permission to access bucket '%s'. Please ensure your Application Key has 'readFiles' and 'writeFiles' capabilities for this bucket", req.Bucket)
			}
			if strings.Contains(errStr, "NoSuchBucket") {
				return fmt.Errorf("bucket '%s' does not exist. Please check the bucket name", req.Bucket)
			}
			// Continue to see if we get more errors
		} else {
			objectFound = true
			fmt.Printf("[S3 Test Connection] Successfully accessed bucket (found object)\n")
			return nil
		}
	}
	
	if hasError && lastError != nil {
		return fmt.Errorf("failed to access bucket '%s': %w", req.Bucket, lastError)
	}
	
	// If we got here without errors, the bucket is accessible (even if empty)
	if !objectFound {
		fmt.Printf("[S3 Test Connection] Bucket is empty, but connection works\n")
	}
	
	fmt.Printf("[S3 Test Connection] Connection test successful\n")
	return nil
}
