package backup

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/dendianugerah/velld/internal/common"
	"github.com/dendianugerah/velld/internal/connection"
	"github.com/dendianugerah/velld/internal/notification"
	"github.com/dendianugerah/velld/internal/settings"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

// cleanS3Credential removes all whitespace and control characters from a credential string
func cleanS3Credential(cred string) string {
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

type BackupService struct {
	connStorage       *connection.ConnectionRepository
	backupDir         string
	backupRepo        *BackupRepository
	cronManager       *cron.Cron
	cronEntries       map[string]cron.EntryID // map[scheduleID]entryID
	settingsService   *settings.SettingsService
	notificationRepo  *notification.NotificationRepository
	cryptoService     *common.EncryptionService
	s3ProviderService *S3ProviderService
	logStreams        map[string]chan string // map[backupID]logChannel
	logStreamsMutex   sync.RWMutex
	logWriteQueue     map[string][]string // Queue logs for batched writes
	logWriteQueueMutex sync.Mutex
}

func NewBackupService(
	connStorage *connection.ConnectionRepository,
	backupDir string,
	backupRepo *BackupRepository,
	settingsService *settings.SettingsService,
	notificationRepo *notification.NotificationRepository,
	cryptoService *common.EncryptionService,
	s3ProviderService *S3ProviderService,
) *BackupService {
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		panic(err)
	}

	cronManager := cron.New(cron.WithSeconds())
	service := &BackupService{
		connStorage:       connStorage,
		backupDir:         backupDir,
		backupRepo:        backupRepo,
		settingsService:   settingsService,
		notificationRepo: notificationRepo,
		cryptoService:     cryptoService,
		s3ProviderService: s3ProviderService,
		cronManager:       cronManager,
		cronEntries:       make(map[string]cron.EntryID),
		logStreams:        make(map[string]chan string),
		logWriteQueue:     make(map[string][]string),
	}

	// Recover existing schedules before starting the cron manager
	if err := service.recoverSchedules(); err != nil {
		fmt.Printf("Error recovering schedules: %v\n", err)
	}

	cronManager.Start()
	return service
}

func (s *BackupService) recoverSchedules() error {
	schedules, err := s.backupRepo.GetAllActiveSchedules()
	if err != nil {
		return fmt.Errorf("failed to get active schedules: %v", err)
	}

	now := time.Now()
	for _, schedule := range schedules {
		scheduleID := schedule.ID.String()

		// Check if we missed any backups
		if schedule.NextRunTime != nil && schedule.NextRunTime.Before(now) {
			// Execute a backup immediately for missed schedule
			go s.executeCronBackup(schedule)
		}

		// Re-register the cron job
		entryID, err := s.cronManager.AddFunc(schedule.CronSchedule, func() {
			s.executeCronBackup(schedule)
		})
		if err != nil {
			fmt.Printf("Error re-registering schedule %s: %v\n", scheduleID, err)
			continue
		}

		s.cronEntries[scheduleID] = entryID
	}

	return nil
}

// StartBackup starts a backup asynchronously and returns the backup ID immediately
func (s *BackupService) StartBackup(connectionID string, s3ProviderIDs []string) (*Backup, error) {
	conn, err := s.connStorage.GetConnection(connectionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %v", err)
	}

	if err := s.verifyBackupTools(conn.Type); err != nil {
		return nil, err
	}

	backupID := uuid.New()
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("%s_%s.sql", conn.DatabaseName, timestamp)

	connectionFolder := filepath.Join(s.backupDir, common.SanitizeConnectionName(conn.Name))
	if err := os.MkdirAll(connectionFolder, 0755); err != nil {
		return nil, fmt.Errorf("failed to create connection backup folder: %v", err)
	}

	backupPath := filepath.Join(connectionFolder, filename)

	backup := &Backup{
		ID:           backupID,
		ConnectionID: connectionID,
		StartedTime:  time.Now(),
		Status:       "in_progress",
		Path:         backupPath,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	// Create log stream channel for this backup
	logChan := make(chan string, 100)
	s.logStreamsMutex.Lock()
	s.logStreams[backupID.String()] = logChan
	s.logStreamsMutex.Unlock()

	// Create backup record in database immediately so logs can be stored
	// This must succeed or logs won't be able to be stored
	if err := s.backupRepo.CreateBackup(backup); err != nil {
		// Log error and return it - we need the backup record to exist
		return nil, fmt.Errorf("failed to create backup record: %w", err)
	}

	// Run backup asynchronously
	go s.executeBackup(backup, conn, backupPath, filename, s3ProviderIDs)

	return backup, nil
}

// executeBackup executes the actual backup process
func (s *BackupService) executeBackup(backup *Backup, conn *connection.StoredConnection, backupPath string, filename string, s3ProviderIDs []string) {
	// Setup SSH tunnel if enabled
	tunnel, effectiveHost, effectivePort, err := s.setupSSHTunnelIfNeeded(conn)
	if err != nil {
		s.sendLog(backup.ID.String(), fmt.Sprintf("[ERROR] Failed to setup SSH tunnel: %v", err))
		s.cleanupLogStream(backup.ID.String())
		return
	}
	if tunnel != nil {
		defer tunnel.Stop()
		// Update connection to use tunnel
		conn.Host = effectiveHost
		conn.Port = effectivePort
	}

	// Send initial log
	s.sendLog(backup.ID.String(), fmt.Sprintf("Starting streaming backup for %s database '%s' on %s:%d", conn.Type, conn.DatabaseName, conn.Host, conn.Port))
	s.sendLog(backup.ID.String(), fmt.Sprintf("Backup will be streamed directly to S3: %s", filename))
	s.sendLog(backup.ID.String(), "[INFO] Using streaming mode - no local file will be created")

	// Check if we have S3 providers configured
	var providers []*S3Provider
	if len(s3ProviderIDs) > 0 {
		// Use specified providers
		for _, providerID := range s3ProviderIDs {
			provider, err := s.s3ProviderService.GetS3ProviderForUpload(providerID, conn.UserID)
			if err != nil {
				s.sendLog(backup.ID.String(), fmt.Sprintf("[WARNING] Failed to get S3 provider %s: %v", providerID, err))
				continue
			}
			providers = append(providers, provider)
		}
	} else {
		// Get ALL configured providers for this user
		allProviders, err := s.s3ProviderService.GetAllS3ProvidersForUpload(conn.UserID)
		if err == nil && len(allProviders) > 0 {
			providers = allProviders
			s.sendLog(backup.ID.String(), fmt.Sprintf("[INFO] Found %d S3 provider(s), will upload to all of them", len(providers)))
		} else {
			// Fallback: try to use default provider if no providers found
			defaultProvider, err := s.s3ProviderService.GetDefaultProvider(conn.UserID)
			if err == nil && defaultProvider != nil {
				provider, err := s.s3ProviderService.GetS3ProviderForUpload(defaultProvider.ID.String(), conn.UserID)
				if err == nil {
					providers = append(providers, provider)
					s.sendLog(backup.ID.String(), "[INFO] Using default S3 provider")
				}
			}
		}
	}

	// If no S3 providers, fall back to file-based backup
	if len(providers) == 0 {
		s.sendLog(backup.ID.String(), "[INFO] No S3 providers configured, falling back to file-based backup")
		s.executeFileBasedBackup(backup, conn, backupPath, filename, s3ProviderIDs)
		return
	}

	// Check versions for PostgreSQL backups
	if conn.Type == "postgresql" {
		// Check client version
		clientVersion := "unknown"
		if version, err := s.getPgDumpVersion(); err == nil {
			clientVersion = version
			s.sendLog(backup.ID.String(), fmt.Sprintf("[INFO] pg_dump client version: %s", version))
		} else {
			s.sendLog(backup.ID.String(), fmt.Sprintf("[WARNING] Could not determine pg_dump version: %v", err))
		}
		
		// Check server version
		if serverVersion, err := s.getPostgreSQLServerVersion(conn); err == nil {
			s.sendLog(backup.ID.String(), fmt.Sprintf("[INFO] PostgreSQL server version: %s", serverVersion))
			
			// Extract major version numbers for comparison
			clientMajor := extractPostgreSQLMajorVersion(clientVersion)
			serverMajor := extractPostgreSQLMajorVersion(serverVersion)
			
			if clientMajor != "" && serverMajor != "" && clientMajor != serverMajor {
				s.sendLog(backup.ID.String(), fmt.Sprintf("[WARNING] Version mismatch detected! Client: %s, Server: %s", clientMajor, serverMajor))
				s.sendLog(backup.ID.String(), "[WARNING] The backup may fail. Please install PostgreSQL client tools matching your server version.")
			}
		} else {
			s.sendLog(backup.ID.String(), fmt.Sprintf("[INFO] Could not determine server version: %v (this is not critical)", err))
		}

		// Check if TimescaleDB is installed
		if s.isTimescaleDBInstalled(conn) {
			s.sendLog(backup.ID.String(), "[INFO] TimescaleDB extension detected in database")
			s.sendLog(backup.ID.String(), "[INFO] Warnings about circular foreign keys in hypertable, chunk, and continuous_agg tables are expected and safe to ignore")
			s.sendLog(backup.ID.String(), "[INFO] These warnings are part of TimescaleDB's internal architecture and do not affect backup integrity")
		}
	}

	// Create streaming command (outputs to stdout)
	var cmd *exec.Cmd
	switch conn.Type {
	case "postgresql":
		// Use plain format for streaming (custom format doesn't support stdout)
		cmd = s.createPgDumpCmdForStreaming(conn)
	case "mysql", "mariadb":
		// Output to stdout for streaming
		cmd = s.createMySQLDumpCmdForStreaming(conn)
	case "mongodb":
		// MongoDB doesn't support stdout streaming easily, fall back to file-based
		s.sendLog(backup.ID.String(), "[INFO] MongoDB doesn't support stdout streaming, using file-based backup")
		s.executeFileBasedBackup(backup, conn, backupPath, filename, s3ProviderIDs)
		return
	case "redis":
		// Redis doesn't support stdout streaming, fall back to file-based
		s.sendLog(backup.ID.String(), "[INFO] Redis doesn't support stdout streaming, using file-based backup")
		s.executeFileBasedBackup(backup, conn, backupPath, filename, s3ProviderIDs)
		return
	default:
		s.sendLog(backup.ID.String(), fmt.Sprintf("[ERROR] Unsupported database type: %s", conn.Type))
		s.cleanupLogStream(backup.ID.String())
		return
	}

	if cmd == nil {
		errMsg := fmt.Sprintf("backup tool not found for %s. Please ensure %s is installed and available in PATH", conn.Type, requiredTools[conn.Type])
		s.sendLog(backup.ID.String(), fmt.Sprintf("[ERROR] %s", errMsg))
		s.cleanupLogStream(backup.ID.String())
		return
	}

	// Capture stdout and stderr separately
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		s.sendLog(backup.ID.String(), fmt.Sprintf("[ERROR] Failed to create stdout pipe: %v", err))
		s.cleanupLogStream(backup.ID.String())
		return
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		s.sendLog(backup.ID.String(), fmt.Sprintf("[ERROR] Failed to create stderr pipe: %v", err))
		s.cleanupLogStream(backup.ID.String())
		return
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		s.sendLog(backup.ID.String(), fmt.Sprintf("[ERROR] Failed to start backup command: %v", err))
		s.cleanupLogStream(backup.ID.String())
		return
	}

	// Stream stderr for logs
	var wg sync.WaitGroup
	var outputErr error
	var outputLines []string

	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			line := scanner.Text()
			outputLines = append(outputLines, line)
			s.sendLog(backup.ID.String(), line)
		}
		if err := scanner.Err(); err != nil && outputErr == nil {
			outputErr = err
		}
	}()

	// Stream backup data directly to S3 providers
	s.sendLog(backup.ID.String(), fmt.Sprintf("[INFO] Starting streaming upload to %d S3 provider(s)...", len(providers)))
	
	// Create a pipe to stream backup data
	pr, pw := io.Pipe()
	
	// Start goroutine to copy stdout to pipe
	var copyErr error
	go func() {
		defer pw.Close()
		_, copyErr = io.Copy(pw, stdoutPipe)
	}()

	// Stream to first provider, then copy to others
	firstProvider := providers[0]
	
	region := "us-east-1"
	if firstProvider.Region != nil && *firstProvider.Region != "" {
		region = *firstProvider.Region
	}

	pathPrefix := ""
	if firstProvider.PathPrefix != nil {
		pathPrefix = *firstProvider.PathPrefix
	}

	accessKey := cleanS3Credential(firstProvider.AccessKey)
	secretKey := cleanS3Credential(firstProvider.SecretKey)
	endpoint := strings.TrimSpace(firstProvider.Endpoint)
	bucket := cleanS3Credential(firstProvider.Bucket)

	s3Config := S3Config{
		Endpoint:   endpoint,
		Region:     region,
		Bucket:     bucket,
		AccessKey:  accessKey,
		SecretKey:  secretKey,
		UseSSL:     firstProvider.UseSSL,
		PathPrefix: pathPrefix,
	}

	s3Storage, err := NewS3Storage(s3Config)
	if err != nil {
		s.sendLog(backup.ID.String(), fmt.Sprintf("[ERROR] Failed to create S3 client: %v", err))
		pr.Close()
		cmd.Wait()
		wg.Wait()
		s.cleanupLogStream(backup.ID.String())
		return
	}

	// Stream compressed data to S3
	// UploadCompressedStream will add .gz extension and apply path prefix
	ctx := context.Background()
	sanitizedConnectionName := common.SanitizeConnectionName(conn.Name)
	s.sendLog(backup.ID.String(), fmt.Sprintf("[INFO] Streaming compressed backup to %s", firstProvider.Name))
	s.sendLog(backup.ID.String(), fmt.Sprintf("[INFO] Bucket: %s", s3Storage.GetBucket()))
	s.sendLog(backup.ID.String(), fmt.Sprintf("[INFO] Connection folder: %s", sanitizedConnectionName))
	
	uploadedKey, err := s3Storage.UploadCompressedStream(ctx, pr, filename, sanitizedConnectionName, func(message string) {
		s.sendLog(backup.ID.String(), fmt.Sprintf("[%s] %s", firstProvider.Name, message))
	})

	// Wait for command and copy to complete
	cmdErr := cmd.Wait()
	wg.Wait()
	pr.Close()

	if cmdErr != nil || outputErr != nil || copyErr != nil || err != nil {
		errorMsg := ""
		if cmdErr != nil {
			errorMsg = cmdErr.Error()
		} else if outputErr != nil {
			errorMsg = outputErr.Error()
		} else if copyErr != nil {
			errorMsg = copyErr.Error()
		} else if err != nil {
			errorMsg = err.Error()
		}
		
		if len(outputLines) > 0 && errorMsg == "" {
			errorMsg = outputLines[len(outputLines)-1]
		}

		s.sendLog(backup.ID.String(), fmt.Sprintf("[ERROR] Backup failed: %s", errorMsg))
		backup.Status = "failed"
		now := time.Now()
		backup.CompletedTime = &now
		if err := s.backupRepo.UpdateBackup(backup); err != nil {
			s.sendLog(backup.ID.String(), fmt.Sprintf("[ERROR] Failed to update backup: %v", err))
		}
		s.cleanupLogStream(backup.ID.String())
		return
	}

	// Get uploaded file size from S3 and verify it exists
	uploadedSize := int64(0)
	if size, err := s3Storage.GetFileSize(ctx, uploadedKey); err == nil {
		uploadedSize = size
		backup.Size = size
		s.sendLog(backup.ID.String(), fmt.Sprintf("[SUCCESS] Backup streamed successfully. Size: %s", s.formatBytes(size)))
		s.sendLog(backup.ID.String(), fmt.Sprintf("[INFO] File verified in S3: s3://%s/%s", s3Storage.GetBucket(), uploadedKey))
	} else {
		s.sendLog(backup.ID.String(), fmt.Sprintf("[WARNING] Could not verify file size in S3: %v", err))
		s.sendLog(backup.ID.String(), fmt.Sprintf("[INFO] File should be at: s3://%s/%s", s3Storage.GetBucket(), uploadedKey))
	}

	// Store S3 info
	backup.S3ObjectKey = &uploadedKey
	providerIDStr := firstProvider.ID.String()
	backup.S3ProviderID = &providerIDStr
	
	s.sendLog(backup.ID.String(), fmt.Sprintf("[INFO] S3 Object Key stored: %s", uploadedKey))

	// Track S3 provider
	if err := s.backupRepo.AddBackupS3Provider(backup.ID.String(), firstProvider.ID.String(), uploadedKey); err != nil {
		s.sendLog(backup.ID.String(), fmt.Sprintf("[WARNING] Failed to track S3 provider: %v", err))
	}

	// Upload to additional providers in parallel (copy from first)
	if len(providers) > 1 {
		s.sendLog(backup.ID.String(), fmt.Sprintf("[INFO] Copying backup to %d additional S3 provider(s)...", len(providers)-1))
		uploadErr := s.uploadToAdditionalS3Providers(backup, conn.UserID, providers[1:], uploadedKey, uploadedSize)
		if uploadErr != nil {
			s.sendLog(backup.ID.String(), fmt.Sprintf("[WARNING] Some additional S3 uploads failed: %v", uploadErr))
		}
	}

	now := time.Now()
	backup.CompletedTime = &now
	backup.Status = "success"
	s.sendLog(backup.ID.String(), "[SUCCESS] Backup completed and streamed to all S3 providers successfully")

	// Update backup record
	if err := s.backupRepo.UpdateBackup(backup); err != nil {
		s.sendLog(backup.ID.String(), fmt.Sprintf("[ERROR] Failed to update backup: %v", err))
	}

	// Send success notification
	if err := s.createSuccessNotification(backup.ConnectionID, backup); err != nil {
		s.sendLog(backup.ID.String(), fmt.Sprintf("[WARNING] Failed to send success notification: %v", err))
	}

	// Clean up log stream
	go func() {
		time.Sleep(2 * time.Second)
		s.cleanupLogStream(backup.ID.String())
	}()
}

// executeFileBasedBackup is the fallback method for file-based backups
// Used for MongoDB, Redis, or when no S3 providers are configured
func (s *BackupService) executeFileBasedBackup(backup *Backup, conn *connection.StoredConnection, backupPath string, filename string, s3ProviderIDs []string) {
	// This uses the original file-based backup logic
	// For simplicity, we'll just call uploadToS3Providers which handles file-based uploads
	// The backup file should already be created by the calling code
	
	// Get file size
	fileInfo, err := os.Stat(backupPath)
	if err != nil {
		s.sendLog(backup.ID.String(), fmt.Sprintf("[ERROR] Failed to get backup file info: %v", err))
		s.cleanupLogStream(backup.ID.String())
		return
	}

	backup.Size = fileInfo.Size()
	backup.Path = backupPath
	now := time.Now()
	backup.CompletedTime = &now

	s.sendLog(backup.ID.String(), fmt.Sprintf("Backup completed successfully. Size: %d bytes", backup.Size))

	// Upload to S3 providers and determine final status
	uploadErr := s.uploadToS3Providers(backup, conn.UserID, s3ProviderIDs)
	if uploadErr != nil {
		errMsg := uploadErr.Error()
		if strings.Contains(errMsg, "partial upload failure") {
			backup.Status = "completed_with_errors"
			s.sendLog(backup.ID.String(), fmt.Sprintf("[WARNING] Backup completed but some S3 uploads failed: %v", uploadErr))
		} else if strings.Contains(errMsg, "No S3 providers configured") {
			backup.Status = "success"
			s.sendLog(backup.ID.String(), "[INFO] No S3 providers configured, backup saved locally only")
		} else {
			backup.Status = "completed_with_errors"
			s.sendLog(backup.ID.String(), fmt.Sprintf("[WARNING] Backup completed but all S3 uploads failed: %v", uploadErr))
		}
	} else {
		backup.Status = "success"
		s.sendLog(backup.ID.String(), "[SUCCESS] Backup completed and uploaded to all S3 providers successfully")
	}

	// Update backup record
	if err := s.backupRepo.UpdateBackup(backup); err != nil {
		s.sendLog(backup.ID.String(), fmt.Sprintf("[ERROR] Failed to update backup: %v", err))
	}

	// Send success notification if backup was successful
	if backup.Status == "success" {
		if err := s.createSuccessNotification(backup.ConnectionID, backup); err != nil {
			s.sendLog(backup.ID.String(), fmt.Sprintf("[WARNING] Failed to send success notification: %v", err))
		}
	}

	// Clean up local backup file after successful S3 upload
	if backup.S3ObjectKey != nil && backup.S3ProviderID != nil {
		if err := os.Remove(backup.Path); err != nil {
			s.sendLog(backup.ID.String(), fmt.Sprintf("[WARNING] Failed to remove local backup file: %v", err))
		} else {
			s.sendLog(backup.ID.String(), fmt.Sprintf("[INFO] Local backup file removed: %s", backup.Path))
		}
	}

	// Close log stream
	go func() {
		time.Sleep(2 * time.Second)
		s.cleanupLogStream(backup.ID.String())
	}()
}

// CreateBackup is kept for backward compatibility but now calls StartBackup
func (s *BackupService) CreateBackup(connectionID string) (*Backup, error) {
	conn, err := s.connStorage.GetConnection(connectionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %v", err)
	}

	if err := s.verifyBackupTools(conn.Type); err != nil {
		return nil, err
	}

	// Setup SSH tunnel if enabled
	tunnel, effectiveHost, effectivePort, err := s.setupSSHTunnelIfNeeded(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to setup SSH tunnel: %v", err)
	}
	if tunnel != nil {
		defer tunnel.Stop()
		// Update connection to use tunnel
		conn.Host = effectiveHost
		conn.Port = effectivePort
	}

	backupID := uuid.New()
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("%s_%s.sql", conn.DatabaseName, timestamp)

	connectionFolder := filepath.Join(s.backupDir, common.SanitizeConnectionName(conn.Name))
	if err := os.MkdirAll(connectionFolder, 0755); err != nil {
		return nil, fmt.Errorf("failed to create connection backup folder: %v", err)
	}

	backupPath := filepath.Join(connectionFolder, filename)

	backup := &Backup{
		ID:           backupID,
		ConnectionID: connectionID,
		StartedTime:  time.Now(),
		Status:       "in_progress",
		Path:         backupPath,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	var cmd *exec.Cmd
	switch conn.Type {
	case "postgresql":
		cmd = s.createPgDumpCmd(conn, backupPath)
	case "mysql", "mariadb":
		cmd = s.createMySQLDumpCmd(conn, backupPath)
	case "mongodb":
		cmd = s.createMongoDumpCmd(conn, backupPath)
	case "redis":
		cmd = s.createRedisDumpCmd(conn, backupPath)
	default:
		return nil, fmt.Errorf("unsupported database type for backup: %s", conn.Type)
	}

	if cmd == nil {
		return nil, fmt.Errorf("backup tool not found for %s. Please ensure %s is installed and available in PATH", conn.Type, requiredTools[conn.Type])
	}

	// Create log stream channel for this backup
	logChan := make(chan string, 100)
	s.logStreamsMutex.Lock()
	s.logStreams[backupID.String()] = logChan
	s.logStreamsMutex.Unlock()

	// Send initial log
	s.sendLog(backupID.String(), fmt.Sprintf("Starting backup for %s database '%s' on %s:%d", conn.Type, conn.DatabaseName, conn.Host, conn.Port))
	s.sendLog(backupID.String(), fmt.Sprintf("Backup file: %s", filename))

	// Capture stdout and stderr separately for streaming
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		s.cleanupLogStream(backupID.String())
		return nil, fmt.Errorf("failed to create stdout pipe: %v", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		s.cleanupLogStream(backupID.String())
		return nil, fmt.Errorf("failed to create stderr pipe: %v", err)
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		s.cleanupLogStream(backupID.String())
		return nil, fmt.Errorf("failed to start backup command: %v", err)
	}

	// Stream stdout and stderr
	var wg sync.WaitGroup
	var outputErr error
	var outputLines []string

	wg.Add(2)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			line := scanner.Text()
			outputLines = append(outputLines, line)
			s.sendLog(backupID.String(), line)
		}
		if err := scanner.Err(); err != nil {
			outputErr = err
		}
	}()

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			line := scanner.Text()
			outputLines = append(outputLines, line)
			s.sendLog(backupID.String(), "[STDERR] "+line)
		}
		if err := scanner.Err(); err != nil && outputErr == nil {
			outputErr = err
		}
	}()

	// Wait for command to complete
	cmdErr := cmd.Wait()
	wg.Wait()

	// Check for errors
	if cmdErr != nil || outputErr != nil {
		errorMsg := ""
		if len(outputLines) > 0 {
			errorMsg = outputLines[len(outputLines)-1]
		}
		if errorMsg == "" && cmdErr != nil {
			errorMsg = cmdErr.Error()
		}
		if errorMsg == "" && outputErr != nil {
			errorMsg = outputErr.Error()
		}
		s.sendLog(backupID.String(), fmt.Sprintf("[ERROR] Backup failed: %s", errorMsg))
		s.cleanupLogStream(backupID.String())
		return nil, fmt.Errorf("backup failed for %s database '%s' on %s:%d - %s",
			conn.Type, conn.DatabaseName, conn.Host, conn.Port, errorMsg)
	}

	// Get file size
	fileInfo, err := os.Stat(backupPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get backup file info: %v", err)
	}

	backup.Size = fileInfo.Size()
	now := time.Now()
	backup.CompletedTime = &now

	s.sendLog(backupID.String(), fmt.Sprintf("Backup completed successfully. Size: %d bytes", backup.Size))

	// Upload to S3 providers and determine final status
	uploadErr := s.uploadToS3Providers(backup, conn.UserID, []string{})
	if uploadErr != nil {
		// Check if it's a partial failure (some succeeded, some failed) or complete failure
		errMsg := uploadErr.Error()
		if strings.Contains(errMsg, "partial upload failure") {
			// Some S3 uploads succeeded, some failed
			backup.Status = "completed_with_errors"
			s.sendLog(backupID.String(), fmt.Sprintf("[WARNING] Backup completed but some S3 uploads failed: %v", uploadErr))
		} else {
			// All S3 uploads failed or no providers configured
			// If no providers were configured, this is still a success
			if strings.Contains(errMsg, "No S3 providers configured") {
				backup.Status = "success"
				s.sendLog(backupID.String(), "[INFO] No S3 providers configured, backup saved locally only")
			} else {
				// All uploads failed
				backup.Status = "completed_with_errors"
				s.sendLog(backupID.String(), fmt.Sprintf("[WARNING] Backup completed but all S3 uploads failed: %v", uploadErr))
			}
		}
		fmt.Printf("Warning: S3 upload issue: %v\n", uploadErr)
	} else {
		// All S3 uploads succeeded (or no providers to upload to)
		backup.Status = "success"
		s.sendLog(backupID.String(), "[SUCCESS] Backup completed and uploaded to all S3 providers successfully")
	}

	if err := s.backupRepo.UpdateBackup(backup); err != nil {
		s.cleanupLogStream(backupID.String())
		return nil, fmt.Errorf("failed to update backup: %v", err)
	}

	// Clean up local backup file after successful S3 upload
	if backup.S3ObjectKey != nil && backup.S3ProviderID != nil {
		// Only delete if at least one S3 upload succeeded
		if err := os.Remove(backup.Path); err != nil {
			s.sendLog(backupID.String(), fmt.Sprintf("[WARNING] Failed to remove local backup file: %v", err))
			fmt.Printf("Warning: Failed to remove local backup file %s: %v\n", backup.Path, err)
		} else {
			s.sendLog(backupID.String(), fmt.Sprintf("[INFO] Local backup file removed: %s", backup.Path))
		}
	}

	// Close log stream after a short delay to allow final messages to be sent
	go func() {
		time.Sleep(2 * time.Second)
		s.cleanupLogStream(backupID.String())
	}()

	return backup, nil
}

// sendLog sends a log message to the stream if it exists and stores it in the database
func (s *BackupService) sendLog(backupID string, message string) {
	// Send to stream for real-time viewing
	s.logStreamsMutex.RLock()
	logChan, exists := s.logStreams[backupID]
	s.logStreamsMutex.RUnlock()

	if exists {
		select {
		case logChan <- message:
		default:
			// Channel is full, skip this message
		}
	}

	// Queue log for batched database write to prevent SQLite lock contention
	// We'll batch writes to reduce database lock issues
	s.logWriteQueueMutex.Lock()
	if s.logWriteQueue == nil {
		s.logWriteQueue = make(map[string][]string)
	}
	s.logWriteQueue[backupID] = append(s.logWriteQueue[backupID], message)
	queueLen := len(s.logWriteQueue[backupID])
	s.logWriteQueueMutex.Unlock()

	// Trigger batched write if queue reaches threshold or start batcher if not running
	if queueLen >= 10 {
		go s.flushLogQueue(backupID)
	} else if queueLen == 1 {
		// Start a delayed flush for this backup (in case we don't reach threshold)
		go func(id string) {
			time.Sleep(2 * time.Second)
			s.flushLogQueue(id)
		}(backupID)
	}
}

// flushLogQueue flushes queued logs for a backup to the database
func (s *BackupService) flushLogQueue(backupID string) {
	s.logWriteQueueMutex.Lock()
	logs, exists := s.logWriteQueue[backupID]
	if !exists || len(logs) == 0 {
		s.logWriteQueueMutex.Unlock()
		return
	}
	// Clear the queue for this backup atomically
	delete(s.logWriteQueue, backupID)
	s.logWriteQueueMutex.Unlock()

	// Combine all logs into a single string
	combinedLogs := strings.Join(logs, "\n")
	
	// Write to database (mutex in AppendLog will handle serialization with retry logic)
	// Use AppendLog which will append to existing logs in the database
	if err := s.backupRepo.AppendLog(backupID, combinedLogs); err != nil {
		// Log error but don't fail the backup
		// If it's a lock error, we'll retry on the next flush
		if strings.Contains(err.Error(), "database is locked") {
			// Re-queue the logs for retry
			s.logWriteQueueMutex.Lock()
			if s.logWriteQueue == nil {
				s.logWriteQueue = make(map[string][]string)
			}
			s.logWriteQueue[backupID] = append(s.logWriteQueue[backupID], logs...)
			s.logWriteQueueMutex.Unlock()
			
			// Retry after a short delay
			go func(id string, retryLogs []string) {
				time.Sleep(100 * time.Millisecond)
				s.logWriteQueueMutex.Lock()
				if s.logWriteQueue == nil {
					s.logWriteQueue = make(map[string][]string)
				}
				s.logWriteQueue[id] = append(s.logWriteQueue[id], retryLogs...)
				s.logWriteQueueMutex.Unlock()
				s.flushLogQueue(id)
			}(backupID, logs)
		} else {
			fmt.Printf("Warning: Failed to store logs for backup %s: %v\n", backupID, err)
		}
	}
}

// GetLogStream returns the log stream channel for a backup ID
func (s *BackupService) GetLogStream(backupID string) <-chan string {
	s.logStreamsMutex.RLock()
	defer s.logStreamsMutex.RUnlock()
	return s.logStreams[backupID]
}

// cleanupLogStream removes and closes a log stream
func (s *BackupService) cleanupLogStream(backupID string) {
	s.logStreamsMutex.Lock()
	defer s.logStreamsMutex.Unlock()
	if logChan, exists := s.logStreams[backupID]; exists {
		close(logChan)
		delete(s.logStreams, backupID)
	}
	
	// Flush any remaining queued logs before cleanup
	s.logWriteQueueMutex.Lock()
	if logs, exists := s.logWriteQueue[backupID]; exists && len(logs) > 0 {
		// Copy logs and clear queue
		logsToFlush := make([]string, len(logs))
		copy(logsToFlush, logs)
		delete(s.logWriteQueue, backupID)
		s.logWriteQueueMutex.Unlock()
		
		// Flush the logs
		combinedLogs := strings.Join(logsToFlush, "\n")
		if err := s.backupRepo.AppendLog(backupID, combinedLogs); err != nil {
			fmt.Printf("Warning: Failed to flush final logs for backup %s: %v\n", backupID, err)
		}
	} else {
		s.logWriteQueueMutex.Unlock()
	}
}

func (s *BackupService) GetBackup(id string) (*Backup, error) {
	return s.backupRepo.GetBackup(id)
}

func (s *BackupService) GetAllBackupsWithPagination(opts BackupListOptions) ([]*BackupList, int, error) {
	if opts.Limit <= 0 {
		opts.Limit = 10
	}
	if opts.Limit > 100 {
		opts.Limit = 100
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}

	return s.backupRepo.GetAllBackupsWithPagination(opts)
}

func (s *BackupService) GetBackupStats(userID uuid.UUID) (*BackupStats, error) {
	return s.backupRepo.GetBackupStats(userID)
}

func (s *BackupService) GetActiveBackups(userID uuid.UUID) ([]*BackupList, error) {
	return s.backupRepo.GetActiveBackups(userID)
}

func (s *BackupService) GetBackupLogs(backupID string) (string, error) {
	return s.backupRepo.GetBackupLogs(backupID)
}

// GetS3ProviderForDownload gets an S3 provider for download operations
func (s *BackupService) GetS3ProviderForDownload(providerID string, userID uuid.UUID) (*S3Storage, error) {
	provider, err := s.s3ProviderService.GetS3ProviderForDownload(providerID, userID)
	if err != nil {
		return nil, err
	}

	region := "us-east-1"
	if provider.Region != nil && *provider.Region != "" {
		region = *provider.Region
	}

	pathPrefix := ""
	if provider.PathPrefix != nil {
		pathPrefix = *provider.PathPrefix
	}

	// Clean credentials
	accessKey := cleanS3Credential(provider.AccessKey)
	secretKey := cleanS3Credential(provider.SecretKey)
	endpoint := strings.TrimSpace(provider.Endpoint)
	bucket := cleanS3Credential(provider.Bucket)

	s3Config := S3Config{
		Endpoint:   endpoint,
		Region:     region,
		Bucket:     bucket,
		AccessKey:  accessKey,
		SecretKey:  secretKey,
		UseSSL:     provider.UseSSL,
		PathPrefix: pathPrefix,
	}

	return NewS3Storage(s3Config)
}

// GetBackupS3Providers returns all S3 providers for a backup
func (s *BackupService) GetBackupS3Providers(backupID string) ([]BackupS3Provider, error) {
	return s.backupRepo.GetBackupS3Providers(backupID)
}

// CreateShareableLink creates a shareable download link for a backup
func (s *BackupService) CreateShareableLink(backupID, providerID string, expiresInHours int) (map[string]interface{}, error) {
	// Generate a secure random token
	token := uuid.New().String() + "-" + uuid.New().String()
	
	expiresAt := time.Now().Add(time.Duration(expiresInHours) * time.Hour)
	
	if err := s.backupRepo.CreateShareableLink(backupID, providerID, token, expiresAt); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"token":      token,
		"expires_at": expiresAt.Format(time.RFC3339),
		"url":        fmt.Sprintf("/api/backups/share/%s", token),
	}, nil
}

// ValidateShareableLink validates a shareable link token and returns backup ID and provider ID
func (s *BackupService) ValidateShareableLink(token string) (backupID, providerID string, err error) {
	return s.backupRepo.GetShareableLink(token)
}

// GetConnection gets a connection by ID (needed for shareable links)
func (s *BackupService) GetConnection(connectionID string) (*connection.StoredConnection, error) {
	return s.connStorage.GetConnection(connectionID)
}

// isPostgreSQLVersionMismatchError checks if the error is a PostgreSQL version mismatch
func (s *BackupService) isPostgreSQLVersionMismatchError(outputLines []string) bool {
	for _, line := range outputLines {
		if strings.Contains(strings.ToLower(line), "server version mismatch") ||
			strings.Contains(strings.ToLower(line), "aborting because of server version mismatch") {
			return true
		}
	}
	return false
}

// formatBytes formats bytes into human-readable format
func (s *BackupService) formatBytes(bytes int64) string {
	if bytes == 0 {
		return "0 B"
	}
	const unit = 1024
	sizes := []string{"B", "KB", "MB", "GB", "TB"}
	i := 0
	size := float64(bytes)
	for size >= unit && i < len(sizes)-1 {
		size /= unit
		i++
	}
	return fmt.Sprintf("%.2f %s", size, sizes[i])
}

// extractPostgreSQLMajorVersion extracts the major version number from a PostgreSQL version string
func extractPostgreSQLMajorVersion(versionStr string) string {
	// Look for patterns like "16.1", "PostgreSQL 16.1", "pg_dump (PostgreSQL) 16.1", etc.
	parts := strings.Fields(versionStr)
	for _, part := range parts {
		// Check if part starts with a number (major version)
		if len(part) > 0 && part[0] >= '0' && part[0] <= '9' {
			// Extract just the major version (first number before dot)
			if dotIndex := strings.Index(part, "."); dotIndex > 0 {
				return part[:dotIndex]
			}
			// If no dot, return first digit sequence
			var major strings.Builder
			for _, r := range part {
				if r >= '0' && r <= '9' {
					major.WriteRune(r)
				} else {
					break
				}
			}
			if major.Len() > 0 {
				return major.String()
			}
		}
	}
	return ""
}

// uploadToS3Providers uploads backup to specified S3 providers or falls back to default/legacy settings
func (s *BackupService) uploadToS3Providers(backup *Backup, userID uuid.UUID, s3ProviderIDs []string) error {
	backupID := backup.ID.String()
	
	var providers []*S3Provider
	
	if len(s3ProviderIDs) > 0 {
		// Use specified providers
		for _, providerID := range s3ProviderIDs {
			provider, err := s.s3ProviderService.GetS3ProviderForUpload(providerID, userID)
			if err != nil {
				s.sendLog(backupID, fmt.Sprintf("[WARNING] Failed to get S3 provider %s: %v", providerID, err))
				continue
			}
			providers = append(providers, provider)
		}
	} else {
		// Get ALL configured providers for this user
		allProviders, err := s.s3ProviderService.GetAllS3ProvidersForUpload(userID)
		if err == nil && len(allProviders) > 0 {
			providers = allProviders
			s.sendLog(backupID, fmt.Sprintf("[INFO] Found %d S3 provider(s), will upload to all of them", len(providers)))
		} else {
			// Fallback: try to use default provider if no providers found
			defaultProvider, err := s.s3ProviderService.GetDefaultProvider(userID)
			if err == nil && defaultProvider != nil {
				provider, err := s.s3ProviderService.GetS3ProviderForUpload(defaultProvider.ID.String(), userID)
				if err == nil {
					providers = append(providers, provider)
					s.sendLog(backupID, "[INFO] Using default S3 provider")
				}
			}
			
			// Fallback to legacy settings if no providers
			if len(providers) == 0 {
				return s.uploadToS3IfEnabled(backup, userID)
			}
		}
	}
	
	if len(providers) == 0 {
		s.sendLog(backupID, "[INFO] No S3 providers configured, skipping upload")
		// Return a special error that indicates no providers (not a failure)
		return fmt.Errorf("No S3 providers configured")
	}
	
	// Upload to all specified providers in parallel
	type uploadResult struct {
		provider  *S3Provider
		objectKey string
		err       error
	}

	uploadChan := make(chan uploadResult, len(providers))
	var uploadWg sync.WaitGroup

	// Start parallel uploads
	for _, provider := range providers {
		uploadWg.Add(1)
		go func(p *S3Provider) {
			defer uploadWg.Done()

			s.sendLog(backupID, fmt.Sprintf("[INFO] Starting S3 upload to provider: %s", p.Name))

			region := "us-east-1"
			if p.Region != nil && *p.Region != "" {
				region = *p.Region
			}

			pathPrefix := ""
			if p.PathPrefix != nil {
				pathPrefix = *p.PathPrefix
			}

			// Credentials should already be cleaned by GetS3ProviderForUpload, but clean again for safety
			accessKey := cleanS3Credential(p.AccessKey)
			secretKey := cleanS3Credential(p.SecretKey)
			endpoint := strings.TrimSpace(p.Endpoint)
			bucket := cleanS3Credential(p.Bucket)

			s3Config := S3Config{
				Endpoint:   endpoint,
				Region:     region,
				Bucket:     bucket,
				AccessKey:  accessKey,
				SecretKey:  secretKey,
				UseSSL:     p.UseSSL,
				PathPrefix: pathPrefix,
			}

			s.sendLog(backupID, fmt.Sprintf("[INFO] S3 Configuration: Provider=%s, Endpoint=%s, Bucket=%s, Region=%s",
				p.Name, p.Endpoint, p.Bucket, region))

			s3Storage, err := NewS3Storage(s3Config)
			if err != nil {
				errMsg := fmt.Sprintf("Failed to create S3 client for %s: %v", p.Name, err)
				s.sendLog(backupID, fmt.Sprintf("[ERROR] %s", errMsg))
				uploadChan <- uploadResult{provider: p, err: fmt.Errorf(errMsg)}
				return
			}

			s.sendLog(backupID, fmt.Sprintf("[INFO] Successfully connected to S3 storage: %s", p.Name))

			fileInfo, err := os.Stat(backup.Path)
			fileSize := int64(0)
			if err == nil {
				fileSize = fileInfo.Size()
				s.sendLog(backupID, fmt.Sprintf("[INFO] Preparing to upload backup file to %s: %s (Size: %d bytes)",
					p.Name, filepath.Base(backup.Path), fileSize))
			}

			ctx := context.Background()
			objectKey, err := s3Storage.UploadFileWithLogging(ctx, backup.Path, func(message string) {
				s.sendLog(backupID, fmt.Sprintf("[%s] %s", p.Name, message))
			})

			if err != nil {
				errMsg := fmt.Sprintf("Failed to upload to %s: %v", p.Name, err)
				s.sendLog(backupID, fmt.Sprintf("[ERROR] %s", errMsg))
				uploadChan <- uploadResult{provider: p, err: fmt.Errorf(errMsg)}
				return
			}

			s.sendLog(backupID, fmt.Sprintf("[SUCCESS] Backup successfully uploaded to %s: %s", p.Name, objectKey))
			if fileSize > 0 {
				s.sendLog(backupID, fmt.Sprintf("[INFO] Uploaded file size to %s: %d bytes (%.2f MB)",
					p.Name, fileSize, float64(fileSize)/(1024*1024)))
			}

			uploadChan <- uploadResult{provider: p, objectKey: objectKey, err: nil}
		}(provider)
	}

	// Wait for all uploads to complete
	go func() {
		uploadWg.Wait()
		close(uploadChan)
	}()

	// Collect results
	var uploadErrors []string
	successCount := 0
	totalProviders := len(providers)

	for result := range uploadChan {
		if result.err != nil {
			uploadErrors = append(uploadErrors, result.err.Error())
		} else {
			successCount++
			// Store the first successful upload's object key and provider ID in backup record
			if backup.S3ObjectKey == nil {
				backup.S3ObjectKey = &result.objectKey
				providerIDStr := result.provider.ID.String()
				backup.S3ProviderID = &providerIDStr
			}

			// Track all successful S3 providers for this backup
			if err := s.backupRepo.AddBackupS3Provider(backupID, result.provider.ID.String(), result.objectKey); err != nil {
				s.sendLog(backupID, fmt.Sprintf("[WARNING] Failed to track S3 provider %s: %v", result.provider.Name, err))
			}
		}
	}

	if successCount == 0 {
		// All uploads failed
		return fmt.Errorf("failed to upload to any S3 provider: %s", strings.Join(uploadErrors, "; "))
	}

	if len(uploadErrors) > 0 {
		// Partial success - some succeeded, some failed
		s.sendLog(backupID, fmt.Sprintf("[WARNING] Uploaded to %d/%d providers. Errors: %s",
			successCount, totalProviders, strings.Join(uploadErrors, "; ")))
		return fmt.Errorf("partial upload failure: %d/%d succeeded, errors: %s",
			successCount, totalProviders, strings.Join(uploadErrors, "; "))
	}

	// All uploads succeeded
	s.sendLog(backupID, fmt.Sprintf("[SUCCESS] Backup uploaded successfully to all %d S3 provider(s)", successCount))
	return nil
}

// uploadToS3IfEnabled is the legacy function for backward compatibility
func (s *BackupService) uploadToS3IfEnabled(backup *Backup, userID uuid.UUID) error {
	backupID := backup.ID.String()
	
	userSettings, err := s.settingsService.GetUserSettings(userID)
	if err != nil {
		return fmt.Errorf("failed to get user settings: %w", err)
	}

	if !userSettings.S3Enabled {
		s.sendLog(backupID, "[INFO] S3 storage is disabled, skipping upload")
		return nil
	}

	s.sendLog(backupID, "[INFO] Starting S3 upload process...")

	if userSettings.S3Endpoint == nil || *userSettings.S3Endpoint == "" {
		return fmt.Errorf("S3 endpoint not configured")
	}
	if userSettings.S3Bucket == nil || *userSettings.S3Bucket == "" {
		return fmt.Errorf("S3 bucket not configured")
	}
	if userSettings.S3AccessKey == nil || *userSettings.S3AccessKey == "" {
		return fmt.Errorf("S3 access key not configured")
	}
	if userSettings.S3SecretKey == nil || *userSettings.S3SecretKey == "" {
		// Provide more helpful error message
		if userSettings.S3SecretKey == nil {
			return fmt.Errorf("S3 secret key not configured (field is NULL). Please save your S3 secret key in Settings.")
		}
		return fmt.Errorf("S3 secret key not configured (field is empty). Please save your S3 secret key in Settings.")
	}

	s.sendLog(backupID, fmt.Sprintf("[INFO] S3 Configuration: Endpoint=%s, Bucket=%s, Region=%s", 
		*userSettings.S3Endpoint, *userSettings.S3Bucket, 
		func() string {
			if userSettings.S3Region != nil && *userSettings.S3Region != "" {
				return *userSettings.S3Region
			}
			return "us-east-1 (default)"
		}()))

	secretKey, err := s.cryptoService.Decrypt(*userSettings.S3SecretKey)
	if err != nil {
		return fmt.Errorf("failed to decrypt S3 secret key: %w", err)
	}

	// (default to us-east-1 if not set)
	region := "us-east-1"
	if userSettings.S3Region != nil && *userSettings.S3Region != "" {
		region = *userSettings.S3Region
	}

	pathPrefix := ""
	if userSettings.S3PathPrefix != nil {
		pathPrefix = *userSettings.S3PathPrefix
	}

	// Trim whitespace from credentials (common issue with copy/paste)
	accessKey := strings.TrimSpace(*userSettings.S3AccessKey)
	secretKey = strings.TrimSpace(secretKey)
	endpoint := strings.TrimSpace(*userSettings.S3Endpoint)
	bucket := strings.TrimSpace(*userSettings.S3Bucket)
	pathPrefix = strings.TrimSpace(pathPrefix)

	s3Config := S3Config{
		Endpoint:   endpoint,
		Region:     region,
		Bucket:     bucket,
		AccessKey:  accessKey,
		SecretKey:  secretKey,
		UseSSL:     userSettings.S3UseSSL,
		PathPrefix: pathPrefix,
	}

	s.sendLog(backupID, "[INFO] Connecting to S3 storage...")
	s3Storage, err := NewS3Storage(s3Config)
	if err != nil {
		s.sendLog(backupID, fmt.Sprintf("[ERROR] Failed to create S3 client: %v", err))
		return fmt.Errorf("failed to create S3 storage client: %w", err)
	}
	s.sendLog(backupID, "[INFO] Successfully connected to S3 storage")

	// Get file size for logging
	fileInfo, err := os.Stat(backup.Path)
	fileSize := int64(0)
	if err == nil {
		fileSize = fileInfo.Size()
		s.sendLog(backupID, fmt.Sprintf("[INFO] Preparing to upload backup file: %s (Size: %d bytes)", filepath.Base(backup.Path), fileSize))
	}

	ctx := context.Background()
	objectKey, err := s3Storage.UploadFileWithLogging(ctx, backup.Path, func(message string) {
		s.sendLog(backupID, message)
	})
	if err != nil {
		s.sendLog(backupID, fmt.Sprintf("[ERROR] S3 upload failed: %v", err))
		return fmt.Errorf("failed to upload backup to S3: %w", err)
	}

	backup.S3ObjectKey = &objectKey
	s.sendLog(backupID, fmt.Sprintf("[SUCCESS] Backup successfully uploaded to S3: %s", objectKey))
	if fileSize > 0 {
		s.sendLog(backupID, fmt.Sprintf("[INFO] Uploaded file size: %d bytes (%.2f MB)", fileSize, float64(fileSize)/(1024*1024)))
	}

	fmt.Printf("Successfully uploaded backup %s to S3: %s\n", backup.ID, objectKey)
	return nil
}

// uploadToAdditionalS3Providers copies the backup from the first provider to additional providers
func (s *BackupService) uploadToAdditionalS3Providers(backup *Backup, userID uuid.UUID, providers []*S3Provider, sourceObjectKey string, sourceSize int64) error {
	backupID := backup.ID.String()
	
	if len(providers) == 0 {
		return nil
	}

	// Download from source provider
	if backup.S3ProviderID == nil {
		return fmt.Errorf("no source S3 provider ID available")
	}
	
	sourceStorage, err := s.GetS3ProviderForDownload(*backup.S3ProviderID, userID)
	if err != nil {
		return fmt.Errorf("failed to create source S3 storage: %w", err)
	}

	ctx := context.Background()
	sourceObject, err := sourceStorage.GetObject(ctx, sourceObjectKey)
	if err != nil {
		return fmt.Errorf("failed to get source object: %w", err)
	}
	defer sourceObject.Close()

	// Upload to additional providers in parallel
	type copyResult struct {
		provider  *S3Provider
		objectKey string
		err       error
	}

	copyChan := make(chan copyResult, len(providers))
	var copyWg sync.WaitGroup

	for _, provider := range providers {
		copyWg.Add(1)
		go func(p *S3Provider) {
			defer copyWg.Done()

			s.sendLog(backupID, fmt.Sprintf("[INFO] Copying backup to provider: %s", p.Name))

			region := "us-east-1"
			if p.Region != nil && *p.Region != "" {
				region = *p.Region
			}

			pathPrefix := ""
			if p.PathPrefix != nil {
				pathPrefix = *p.PathPrefix
			}

			accessKey := cleanS3Credential(p.AccessKey)
			secretKey := cleanS3Credential(p.SecretKey)
			endpoint := strings.TrimSpace(p.Endpoint)
			bucket := cleanS3Credential(p.Bucket)

			s3Config := S3Config{
				Endpoint:   endpoint,
				Region:     region,
				Bucket:     bucket,
				AccessKey:  accessKey,
				SecretKey:  secretKey,
				UseSSL:     p.UseSSL,
				PathPrefix: pathPrefix,
			}

			destStorage, err := NewS3Storage(s3Config)
			if err != nil {
				copyChan <- copyResult{provider: p, err: fmt.Errorf("failed to create S3 client: %w", err)}
				return
			}

			// Read source object into memory (for small files) or stream it
			// For large files, we should stream, but for simplicity, let's read it
			// Actually, we need to re-read the source for each provider
			// Let's create a new reader from source
			sourceObject2, err := sourceStorage.GetObject(ctx, sourceObjectKey)
			if err != nil {
				copyChan <- copyResult{provider: p, err: fmt.Errorf("failed to get source object: %w", err)}
				return
			}
			defer sourceObject2.Close()

			// Extract connection name from source object key
			// Format: prefix/connection_name/filename or connection_name/filename
			connectionName := ""
			keyParts := strings.Split(sourceObjectKey, "/")
			if len(keyParts) >= 2 {
				// Connection name is usually second-to-last part (before filename)
				// Skip prefix if present, then connection name, then filename
				for i := len(keyParts) - 2; i >= 0; i-- {
					if keyParts[i] != "" && !strings.HasSuffix(keyParts[i], ".gz") && !strings.HasSuffix(keyParts[i], ".sql") {
						connectionName = keyParts[i]
						break
					}
				}
			}
			
			objectKey := filepath.Base(sourceObjectKey)
			
			// Upload the stream
			uploadedKey, err := destStorage.UploadStream(ctx, sourceObject2, objectKey, connectionName, func(message string) {
				s.sendLog(backupID, fmt.Sprintf("[%s] %s", p.Name, message))
			})

			if err != nil {
				copyChan <- copyResult{provider: p, err: fmt.Errorf("failed to upload: %w", err)}
				return
			}

			// Track S3 provider
			if err := s.backupRepo.AddBackupS3Provider(backupID, p.ID.String(), uploadedKey); err != nil {
				s.sendLog(backupID, fmt.Sprintf("[WARNING] Failed to track S3 provider %s: %v", p.Name, err))
			}

			s.sendLog(backupID, fmt.Sprintf("[SUCCESS] Backup copied to %s: %s", p.Name, uploadedKey))
			copyChan <- copyResult{provider: p, objectKey: uploadedKey, err: nil}
		}(provider)
	}

	go func() {
		copyWg.Wait()
		close(copyChan)
	}()

	var errors []string
	successCount := 0

	for result := range copyChan {
		if result.err != nil {
			errors = append(errors, fmt.Sprintf("%s: %v", result.provider.Name, result.err))
		} else {
			successCount++
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("partial copy failure: %d/%d succeeded, errors: %s", successCount, len(providers), strings.Join(errors, "; "))
	}

	return nil
}
