package backup

import (
	"bufio"
	"context"
	"fmt"
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
	s.sendLog(backup.ID.String(), fmt.Sprintf("Starting backup for %s database '%s' on %s:%d", conn.Type, conn.DatabaseName, conn.Host, conn.Port))
	s.sendLog(backup.ID.String(), fmt.Sprintf("Backup file: %s", filename))

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

	// Capture stdout and stderr separately for streaming
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

	// Stream stdout and stderr
	var wg sync.WaitGroup
	var outputErr error
	var outputLines []string

	// For PostgreSQL, monitor file size for progress reporting
	var fileSizeMonitor *time.Ticker
	var fileSizeStop chan bool
	var lastSize int64 = 0
	var lastCheckTime time.Time
	if conn.Type == "postgresql" {
		fileSizeMonitor = time.NewTicker(2 * time.Second) // Check every 2 seconds
		fileSizeStop = make(chan bool)
		go func() {
			defer fileSizeMonitor.Stop()
			lastCheckTime = time.Now()
			for {
				select {
				case <-fileSizeMonitor.C:
					if info, err := os.Stat(backupPath); err == nil {
						size := info.Size()
						if size > 0 && size != lastSize {
							now := time.Now()
							rateMsg := ""
							if lastSize > 0 {
								// Calculate rate based on size difference
								elapsed := now.Sub(lastCheckTime).Seconds()
								if elapsed > 0 {
									sizeDiff := size - lastSize
									rate := float64(sizeDiff) / elapsed
									rateMsg = fmt.Sprintf(" (%.2f MB/s)", rate/(1024*1024))
								}
							}
							lastSize = size
							lastCheckTime = now
							s.sendLog(backup.ID.String(), fmt.Sprintf("[PROGRESS] Backup file size: %s%s", s.formatBytes(size), rateMsg))
						}
					}
				case <-fileSizeStop:
					// Final size report
					if info, err := os.Stat(backupPath); err == nil {
						finalSize := info.Size()
						if finalSize > 0 {
							s.sendLog(backup.ID.String(), fmt.Sprintf("[PROGRESS] Final backup file size: %s", s.formatBytes(finalSize)))
						}
					}
					return
				}
			}
		}()
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			line := scanner.Text()
			outputLines = append(outputLines, line)
			// Parse verbose output for progress information
			if conn.Type == "postgresql" {
				// pg_dump --verbose outputs lines like:
				// "pg_dump: dumping contents of table \"table_name\""
				// "pg_dump: dumping schema \"schema_name\""
				// These are already informative progress messages
				s.sendLog(backup.ID.String(), line)
			} else {
				s.sendLog(backup.ID.String(), line)
			}
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
			s.sendLog(backup.ID.String(), "[STDERR] "+line)
		}
		if err := scanner.Err(); err != nil && outputErr == nil {
			outputErr = err
		}
	}()

	// Wait for command to complete
	cmdErr := cmd.Wait()
	if fileSizeStop != nil {
		close(fileSizeStop)
	}
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
		
		// Check for specific PostgreSQL version mismatch error
		if conn.Type == "postgresql" && s.isPostgreSQLVersionMismatchError(outputLines) {
			s.sendLog(backup.ID.String(), fmt.Sprintf("[ERROR] Backup failed: %s", errorMsg))
			s.sendLog(backup.ID.String(), "[INFO] PostgreSQL version mismatch detected.")
			s.sendLog(backup.ID.String(), "[INFO] The pg_dump client version must match the PostgreSQL server version.")
			s.sendLog(backup.ID.String(), "[INFO] Solution: Install PostgreSQL client tools that match your server version.")
			s.sendLog(backup.ID.String(), "[INFO] If running in Docker, rebuild your image with PostgreSQL 16 client tools:")
			s.sendLog(backup.ID.String(), "[INFO]   docker-compose build --no-cache api")
			s.sendLog(backup.ID.String(), "[INFO]   docker-compose up -d")
			s.sendLog(backup.ID.String(), "[INFO] Or update your Dockerfile to install postgresql16-client package.")
			s.sendLog(backup.ID.String(), "[INFO] You can check your server version with: SELECT version();")
			s.sendLog(backup.ID.String(), "[INFO] You can check your client version with: pg_dump --version")
		} else {
			s.sendLog(backup.ID.String(), fmt.Sprintf("[ERROR] Backup failed: %s", errorMsg))
		}
		
		backup.Status = "failed"
		now := time.Now()
		backup.CompletedTime = &now
		if err := s.backupRepo.UpdateBackup(backup); err != nil {
			s.sendLog(backup.ID.String(), fmt.Sprintf("[ERROR] Failed to update backup: %v", err))
		}
		s.cleanupLogStream(backup.ID.String())
		return
	}

	// Get file size
	fileInfo, err := os.Stat(backupPath)
	if err != nil {
		s.sendLog(backup.ID.String(), fmt.Sprintf("[ERROR] Failed to get backup file info: %v", err))
		s.cleanupLogStream(backup.ID.String())
		return
	}

	backup.Size = fileInfo.Size()
	now := time.Now()
	backup.CompletedTime = &now

	s.sendLog(backup.ID.String(), fmt.Sprintf("Backup completed successfully. Size: %d bytes", backup.Size))

	// Upload to S3 providers and determine final status
	uploadErr := s.uploadToS3Providers(backup, conn.UserID, s3ProviderIDs)
	if uploadErr != nil {
		// Check if it's a partial failure (some succeeded, some failed) or complete failure
		errMsg := uploadErr.Error()
		if strings.Contains(errMsg, "partial upload failure") {
			// Some S3 uploads succeeded, some failed
			backup.Status = "completed_with_errors"
			s.sendLog(backup.ID.String(), fmt.Sprintf("[WARNING] Backup completed but some S3 uploads failed: %v", uploadErr))
		} else {
			// All S3 uploads failed or no providers configured
			// If no providers were configured, this is still a success
			if strings.Contains(errMsg, "No S3 providers configured") {
				backup.Status = "success"
				s.sendLog(backup.ID.String(), "[INFO] No S3 providers configured, backup saved locally only")
			} else {
				// All uploads failed
				backup.Status = "completed_with_errors"
				s.sendLog(backup.ID.String(), fmt.Sprintf("[WARNING] Backup completed but all S3 uploads failed: %v", uploadErr))
			}
		}
		fmt.Printf("Warning: S3 upload issue: %v\n", uploadErr)
	} else {
		// All S3 uploads succeeded (or no providers to upload to)
		backup.Status = "success"
		s.sendLog(backup.ID.String(), "[SUCCESS] Backup completed and uploaded to all S3 providers successfully")
	}

	// Update backup record with final status and details
	if err := s.backupRepo.UpdateBackup(backup); err != nil {
		s.sendLog(backup.ID.String(), fmt.Sprintf("[ERROR] Failed to update backup: %v", err))
		fmt.Printf("Warning: Failed to update backup: %v\n", err)
	}

	// Clean up local backup file after successful S3 upload
	if backup.S3ObjectKey != nil && backup.S3ProviderID != nil {
		// Only delete if at least one S3 upload succeeded
		if err := os.Remove(backup.Path); err != nil {
			s.sendLog(backup.ID.String(), fmt.Sprintf("[WARNING] Failed to remove local backup file: %v", err))
			fmt.Printf("Warning: Failed to remove local backup file %s: %v\n", backup.Path, err)
		} else {
			s.sendLog(backup.ID.String(), fmt.Sprintf("[INFO] Local backup file removed: %s", backup.Path))
		}
	}

	// Close log stream after a short delay to allow final messages to be sent
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
		// Try to use default provider first
		defaultProvider, err := s.s3ProviderService.GetDefaultProvider(userID)
		if err == nil && defaultProvider != nil {
			provider, err := s.s3ProviderService.GetS3ProviderForUpload(defaultProvider.ID.String(), userID)
			if err == nil {
				providers = append(providers, provider)
			}
		}
		
		// Fallback to legacy settings if no default provider
		if len(providers) == 0 {
			return s.uploadToS3IfEnabled(backup, userID)
		}
	}
	
	if len(providers) == 0 {
		s.sendLog(backupID, "[INFO] No S3 providers configured, skipping upload")
		// Return a special error that indicates no providers (not a failure)
		return fmt.Errorf("No S3 providers configured")
	}
	
	// Upload to all specified providers
	var uploadErrors []string
	successCount := 0
	totalProviders := len(providers)
	
	for i, provider := range providers {
		s.sendLog(backupID, fmt.Sprintf("[INFO] Starting S3 upload to provider %d/%d: %s", i+1, len(providers), provider.Name))
		
		region := "us-east-1"
		if provider.Region != nil && *provider.Region != "" {
			region = *provider.Region
		}
		
		pathPrefix := ""
		if provider.PathPrefix != nil {
			pathPrefix = *provider.PathPrefix
		}
		
		// Credentials should already be cleaned by GetS3ProviderForUpload, but clean again for safety
		// This ensures no whitespace or control characters make it through
		accessKey := cleanS3Credential(provider.AccessKey)
		secretKey := cleanS3Credential(provider.SecretKey)
		endpoint := strings.TrimSpace(provider.Endpoint) // Endpoint can have spaces in domain names
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
		
		s.sendLog(backupID, fmt.Sprintf("[INFO] S3 Configuration: Provider=%s, Endpoint=%s, Bucket=%s, Region=%s",
			provider.Name, provider.Endpoint, provider.Bucket, region))
		
		s3Storage, err := NewS3Storage(s3Config)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to create S3 client for %s: %v", provider.Name, err)
			s.sendLog(backupID, fmt.Sprintf("[ERROR] %s", errMsg))
			uploadErrors = append(uploadErrors, errMsg)
			continue
		}
		
		s.sendLog(backupID, fmt.Sprintf("[INFO] Successfully connected to S3 storage: %s", provider.Name))
		
		fileInfo, err := os.Stat(backup.Path)
		fileSize := int64(0)
		if err == nil {
			fileSize = fileInfo.Size()
			s.sendLog(backupID, fmt.Sprintf("[INFO] Preparing to upload backup file to %s: %s (Size: %d bytes)",
				provider.Name, filepath.Base(backup.Path), fileSize))
		}
		
		ctx := context.Background()
		objectKey, err := s3Storage.UploadFileWithLogging(ctx, backup.Path, func(message string) {
			s.sendLog(backupID, fmt.Sprintf("[%s] %s", provider.Name, message))
		})
		
		if err != nil {
			errMsg := fmt.Sprintf("Failed to upload to %s: %v", provider.Name, err)
			s.sendLog(backupID, fmt.Sprintf("[ERROR] %s", errMsg))
			uploadErrors = append(uploadErrors, errMsg)
			continue
		}
		
		successCount++
		s.sendLog(backupID, fmt.Sprintf("[SUCCESS] Backup successfully uploaded to %s: %s", provider.Name, objectKey))
		if fileSize > 0 {
			s.sendLog(backupID, fmt.Sprintf("[INFO] Uploaded file size to %s: %d bytes (%.2f MB)",
				provider.Name, fileSize, float64(fileSize)/(1024*1024)))
		}
		
		// Store the first successful upload's object key and provider ID in backup record
		if backup.S3ObjectKey == nil {
			backup.S3ObjectKey = &objectKey
			providerIDStr := provider.ID.String()
			backup.S3ProviderID = &providerIDStr
		}
		
		// Track all successful S3 providers for this backup
		if err := s.backupRepo.AddBackupS3Provider(backupID, provider.ID.String(), objectKey); err != nil {
			s.sendLog(backupID, fmt.Sprintf("[WARNING] Failed to track S3 provider %s: %v", provider.Name, err))
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
