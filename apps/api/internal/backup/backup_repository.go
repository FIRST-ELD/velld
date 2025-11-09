package backup

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dendianugerah/velld/internal/common"
	"github.com/google/uuid"
)

type BackupRepository struct {
	db            *sql.DB
	appendLogMutex sync.Mutex // Protects concurrent log appends
}

func NewBackupRepository(db *sql.DB) *BackupRepository {
	return &BackupRepository{
		db: db,
	}
}

func (r *BackupRepository) CreateBackupSchedule(schedule *BackupSchedule) error {
	var nextRunStr *string
	if schedule.NextRunTime != nil {
		str := schedule.NextRunTime.Format(time.RFC3339)
		nextRunStr = &str
	}

	var lastBackupStr *string
	if schedule.LastBackupTime != nil {
		str := schedule.LastBackupTime.Format(time.RFC3339)
		lastBackupStr = &str
	}

	now := time.Now().Format(time.RFC3339)
	_, err := r.db.Exec(`
		INSERT INTO backup_schedules (
			id, connection_id, enabled, cron_schedule, retention_days,
			next_run_time, last_backup_time, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		schedule.ID, schedule.ConnectionID, schedule.Enabled,
		schedule.CronSchedule, schedule.RetentionDays,
		nextRunStr, lastBackupStr, now, now)
	return err
}

func (r *BackupRepository) UpdateBackupSchedule(schedule *BackupSchedule) error {
	var nextRunStr *string
	if schedule.NextRunTime != nil {
		str := schedule.NextRunTime.Format(time.RFC3339)
		nextRunStr = &str
	}

	var lastBackupStr *string
	if schedule.LastBackupTime != nil {
		str := schedule.LastBackupTime.Format(time.RFC3339)
		lastBackupStr = &str
	}

	query := `
		UPDATE backup_schedules 
		SET enabled = $1, 
		    cron_schedule = $2, 
		    retention_days = $3, 
		    next_run_time = $4,
		    last_backup_time = $5,
		    updated_at = $6
		WHERE id = $7
	`

	_, err := r.db.Exec(query,
		schedule.Enabled,
		schedule.CronSchedule,
		schedule.RetentionDays,
		nextRunStr,
		lastBackupStr,
		time.Now(),
		schedule.ID)
	if err != nil {
		return fmt.Errorf("failed to update backup schedule: %v", err)
	}

	return nil
}

func (r *BackupRepository) GetBackupSchedule(connectionID string) (*BackupSchedule, error) {
	var (
		nextRunStr    sql.NullString
		lastBackupStr sql.NullString
		createdAtStr  string
		updatedAtStr  string
	)
	schedule := &BackupSchedule{}
	err := r.db.QueryRow(`
		SELECT id, connection_id, enabled, cron_schedule, retention_days,
		       next_run_time, last_backup_time, created_at, updated_at 
		FROM backup_schedules 
		WHERE connection_id = $1
		ORDER BY created_at DESC LIMIT 1`,
		connectionID).Scan(
		&schedule.ID, &schedule.ConnectionID, &schedule.Enabled,
		&schedule.CronSchedule, &schedule.RetentionDays,
		&nextRunStr, &lastBackupStr, &createdAtStr, &updatedAtStr)
	if err != nil {
		return nil, err
	}

	// Parse next_run_time if not null
	if nextRunStr.Valid {
		nextRun, err := common.ParseTime(nextRunStr.String)
		if err != nil {
			return nil, fmt.Errorf("error parsing next_run_time: %v", err)
		}
		schedule.NextRunTime = &nextRun
	}

	// Parse last_backup_time if not null
	if lastBackupStr.Valid {
		lastBackup, err := common.ParseTime(lastBackupStr.String)
		if err != nil {
			return nil, fmt.Errorf("error parsing last_backup_time: %v", err)
		}
		schedule.LastBackupTime = &lastBackup
	}

	// Parse created_at and updated_at
	createdAt, err := common.ParseTime(createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing created_at: %v", err)
	}
	schedule.CreatedAt = createdAt

	updatedAt, err := common.ParseTime(updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing updated_at: %v", err)
	}
	schedule.UpdatedAt = updatedAt

	return schedule, nil
}

func (r *BackupRepository) GetAllActiveSchedules() ([]*BackupSchedule, error) {
	rows, err := r.db.Query(`
		SELECT id, connection_id, enabled, cron_schedule, retention_days,
		       next_run_time, last_backup_time, created_at, updated_at 
		FROM backup_schedules 
		WHERE enabled = true
		ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schedules []*BackupSchedule
	for rows.Next() {
		var (
			nextRunStr    sql.NullString
			lastBackupStr sql.NullString
			createdAtStr  string
			updatedAtStr  string
		)
		schedule := &BackupSchedule{}
		err := rows.Scan(
			&schedule.ID, &schedule.ConnectionID, &schedule.Enabled,
			&schedule.CronSchedule, &schedule.RetentionDays,
			&nextRunStr, &lastBackupStr, &createdAtStr, &updatedAtStr)
		if err != nil {
			return nil, err
		}

		// Parse next_run_time if not null
		if nextRunStr.Valid {
			nextRun, err := common.ParseTime(nextRunStr.String)
			if err != nil {
				return nil, fmt.Errorf("error parsing next_run_time: %v", err)
			}
			schedule.NextRunTime = &nextRun
		}

		// Parse last_backup_time if not null
		if lastBackupStr.Valid {
			lastBackup, err := common.ParseTime(lastBackupStr.String)
			if err != nil {
				return nil, fmt.Errorf("error parsing last_backup_time: %v", err)
			}
			schedule.LastBackupTime = &lastBackup
		}

		// Parse created_at and updated_at
		createdAt, err := common.ParseTime(createdAtStr)
		if err != nil {
			return nil, fmt.Errorf("error parsing created_at: %v", err)
		}
		schedule.CreatedAt = createdAt

		updatedAt, err := common.ParseTime(updatedAtStr)
		if err != nil {
			return nil, fmt.Errorf("error parsing updated_at: %v", err)
		}
		schedule.UpdatedAt = updatedAt

		schedules = append(schedules, schedule)
	}

	return schedules, rows.Err()
}

// Backup Methods

func (r *BackupRepository) CreateBackup(backup *Backup) error {
	_, err := r.db.Exec(`
		INSERT INTO backups (
			id, connection_id, schedule_id, status, path, s3_object_key, s3_provider_id, size, logs,
			started_time, completed_time, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		backup.ID, backup.ConnectionID, backup.ScheduleID,
		backup.Status, backup.Path, backup.S3ObjectKey, backup.S3ProviderID, backup.Size, backup.Logs,
		backup.StartedTime, backup.CompletedTime,
		backup.CreatedAt, backup.UpdatedAt)
	return err
}

func (r *BackupRepository) UpdateBackupStatus(id string, status string) error {
	_, err := r.db.Exec("UPDATE backups SET status = $1, updated_at = $2 WHERE id = $3",
		status, time.Now().Format(time.RFC3339), id)
	return err
}

func (r *BackupRepository) UpdateBackup(backup *Backup) error {
	var completedTimeStr *string
	if backup.CompletedTime != nil {
		str := backup.CompletedTime.Format(time.RFC3339)
		completedTimeStr = &str
	}

	// Don't overwrite logs in UpdateBackup - logs should only be updated via AppendLog
	// This prevents clearing accumulated logs when updating backup status
	// If backup.Logs is provided and not nil, we'll update it, otherwise preserve existing logs
	var logsValue interface{}
	if backup.Logs != nil && *backup.Logs != "" {
		// Only update logs if explicitly provided and not empty
		logsValue = *backup.Logs
	} else {
		// Preserve existing logs by using COALESCE or not updating the field
		// We'll use a subquery to keep existing logs
		logsValue = nil // Will use COALESCE in SQL
	}

	if logsValue != nil {
		// Update with new logs value
		_, err := r.db.Exec(`
			UPDATE backups SET
				status = $1,
				path = $2,
				s3_object_key = $3,
				s3_provider_id = $4,
				size = $5,
				logs = $6,
				started_time = $7,
				completed_time = $8,
				updated_at = $9
			WHERE id = $10`,
			backup.Status, backup.Path, backup.S3ObjectKey, backup.S3ProviderID, backup.Size, logsValue,
			backup.StartedTime.Format(time.RFC3339), completedTimeStr,
			time.Now().Format(time.RFC3339), backup.ID)
		return err
	} else {
		// Don't update logs field - preserve existing logs
		_, err := r.db.Exec(`
			UPDATE backups SET
				status = $1,
				path = $2,
				s3_object_key = $3,
				s3_provider_id = $4,
				size = $5,
				started_time = $6,
				completed_time = $7,
				updated_at = $8
			WHERE id = $9`,
			backup.Status, backup.Path, backup.S3ObjectKey, backup.S3ProviderID, backup.Size,
			backup.StartedTime.Format(time.RFC3339), completedTimeStr,
			time.Now().Format(time.RFC3339), backup.ID)
		return err
	}
}

func (r *BackupRepository) GetBackupsOlderThan(connectionID string, cutoffTime time.Time) ([]*Backup, error) {
	rows, err := r.db.Query(`
		SELECT id, path, created_at 
		FROM backups 
		WHERE connection_id = $1 
		AND created_at < $2 
		AND status = 'completed'`,
		connectionID, cutoffTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var backups []*Backup
	for rows.Next() {
		backup := &Backup{}
		var createdAtStr string
		err := rows.Scan(&backup.ID, &backup.Path, &createdAtStr)
		if err != nil {
			return nil, err
		}
		createdAt, err := common.ParseTime(createdAtStr)
		if err != nil {
			return nil, fmt.Errorf("error parsing created_at: %v", err)
		}
		backup.CreatedAt = createdAt
		backups = append(backups, backup)
	}
	return backups, rows.Err()
}

func (r *BackupRepository) DeleteBackup(id string) error {
	_, err := r.db.Exec("DELETE FROM backups WHERE id = $1", id)
	return err
}

func (r *BackupRepository) GetBackup(id string) (*Backup, error) {
	var (
		startedTimeStr   string
		completedTimeStr sql.NullString
		createdAtStr     string
		updatedAtStr     string
	)
	var logsStr sql.NullString
	var s3ProviderIDStr sql.NullString
	backup := &Backup{}
	err := r.db.QueryRow(`
		SELECT id, connection_id, schedule_id, status, path, s3_object_key, s3_provider_id, size, logs,
			   started_time, completed_time, created_at, updated_at 
		FROM backups WHERE id = $1`, id).
		Scan(&backup.ID, &backup.ConnectionID, &backup.ScheduleID,
			&backup.Status, &backup.Path, &backup.S3ObjectKey, &s3ProviderIDStr, &backup.Size, &logsStr,
			&startedTimeStr, &completedTimeStr,
			&createdAtStr, &updatedAtStr)
	if err != nil {
		return nil, err
	}

	// Parse started_time
	startedTime, err := common.ParseTime(startedTimeStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing started_time: %v", err)
	}
	backup.StartedTime = startedTime

	// Parse completed_time if not null
	if completedTimeStr.Valid {
		completedTime, err := common.ParseTime(completedTimeStr.String)
		if err != nil {
			return nil, fmt.Errorf("error parsing completed_time: %v", err)
		}
		backup.CompletedTime = &completedTime
	}

	// Parse created_at and updated_at
	createdAt, err := common.ParseTime(createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing created_at: %v", err)
	}
	backup.CreatedAt = createdAt

	updatedAt, err := common.ParseTime(updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing updated_at: %v", err)
	}
	backup.UpdatedAt = updatedAt

	// Parse logs if not null
	if logsStr.Valid {
		backup.Logs = &logsStr.String
	}

	// Parse s3_provider_id if not null
	if s3ProviderIDStr.Valid {
		backup.S3ProviderID = &s3ProviderIDStr.String
	}

	return backup, nil
}

func (r *BackupRepository) GetAllBackupsWithPagination(opts BackupListOptions) ([]*BackupList, int, error) {
	whereClause := "WHERE c.user_id = $1"
	args := []interface{}{opts.UserID}
	argCount := 2

	if opts.Search != "" {
		whereClause += fmt.Sprintf(" AND (LOWER(b.path) LIKE $%d OR LOWER(b.status) LIKE $%d)", argCount, argCount)
		args = append(args, "%"+strings.ToLower(opts.Search)+"%")
		argCount++
	}

	countQuery := fmt.Sprintf(`
		SELECT COUNT(*) 
		FROM backups b
		INNER JOIN connections c ON b.connection_id = c.id
		%s`, whereClause)

	var total int
	if err := r.db.QueryRow(countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	query := fmt.Sprintf(`
		SELECT 
			b.id, b.connection_id, c.type, b.schedule_id, b.status, b.path, b.s3_object_key, b.size,
			b.started_time, b.completed_time, b.created_at, b.updated_at,
			c.database_name
		FROM backups b
		INNER JOIN connections c ON b.connection_id = c.id
		%s
		ORDER BY b.created_at DESC
		LIMIT $%d OFFSET $%d
	`, whereClause, argCount, argCount+1)

	args = append(args, opts.Limit, opts.Offset)
	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	backups := make([]*BackupList, 0)
	for rows.Next() {
		var (
			startedTimeStr   sql.NullString
			completedTimeStr sql.NullString
			createdAtStr     string
			updatedAtStr     string
		)
		backup := &BackupList{}
		err := rows.Scan(
			&backup.ID, &backup.ConnectionID, &backup.DatabaseType,
			&backup.ScheduleID, &backup.Status, &backup.Path, &backup.S3ObjectKey, &backup.Size,
			&startedTimeStr, &completedTimeStr,
			&createdAtStr, &updatedAtStr,
			&backup.DatabaseName,
		)
		if err != nil {
			return nil, 0, err
		}

		backup.StartedTime = startedTimeStr.String
		backup.CompletedTime = completedTimeStr.String
		backup.CreatedAt = createdAtStr
		backup.UpdatedAt = updatedAtStr

		backups = append(backups, backup)
	}

	return backups, total, rows.Err()
}

func (r *BackupRepository) GetActiveBackups(userID uuid.UUID) ([]*BackupList, error) {
	query := `
		SELECT 
			b.id, b.connection_id, c.type, b.schedule_id, b.status, b.path, b.s3_object_key, b.size,
			b.started_time, b.completed_time, b.created_at, b.updated_at,
			c.database_name
		FROM backups b
		INNER JOIN connections c ON b.connection_id = c.id
		WHERE c.user_id = $1 AND b.status = 'in_progress'
		ORDER BY b.started_time DESC
	`

	rows, err := r.db.Query(query, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	backups := make([]*BackupList, 0)
	for rows.Next() {
		var (
			startedTimeStr   sql.NullString
			completedTimeStr sql.NullString
			createdAtStr     string
			updatedAtStr     string
		)
		backup := &BackupList{}
		err := rows.Scan(
			&backup.ID, &backup.ConnectionID, &backup.DatabaseType,
			&backup.ScheduleID, &backup.Status, &backup.Path, &backup.S3ObjectKey, &backup.Size,
			&startedTimeStr, &completedTimeStr,
			&createdAtStr, &updatedAtStr,
			&backup.DatabaseName,
		)
		if err != nil {
			return nil, err
		}

		backup.StartedTime = startedTimeStr.String
		backup.CompletedTime = completedTimeStr.String
		backup.CreatedAt = createdAtStr
		backup.UpdatedAt = updatedAtStr

		backups = append(backups, backup)
	}

	return backups, rows.Err()
}

func (r *BackupRepository) UpdateBackupStatusAndSchedule(id string, status string, scheduleID string) error {
	_, err := r.db.Exec(`
		UPDATE backups 
		SET status = $1, schedule_id = $2, updated_at = $3 
		WHERE id = $4`,
		status, scheduleID, time.Now().Format(time.RFC3339), id)
	return err
}

// AppendLog appends log lines to the backup_logs table
// This is much more efficient than storing logs in a TEXT column
// Supports batch inserts for better performance
func (r *BackupRepository) AppendLog(backupID string, logLine string) error {
	r.appendLogMutex.Lock()
	defer r.appendLogMutex.Unlock()

	// Split logLine by newlines to handle multiple lines
	lines := strings.Split(logLine, "\n")
	if len(lines) == 0 {
		return nil
	}

	maxRetries := 5
	baseDelay := 10 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		tx, err := r.db.Begin()
		if err != nil {
			if attempt < maxRetries-1 && (err.Error() == "database is locked" || err.Error() == "database is locked (5)") {
				delay := baseDelay * time.Duration(1<<uint(attempt))
				time.Sleep(delay)
				continue
			}
			return err
		}

		// Get the current max line number for this backup
		var maxLineNumber sql.NullInt64
		err = tx.QueryRow(`SELECT COALESCE(MAX(line_number), 0) FROM backup_logs WHERE backup_id = $1`, backupID).Scan(&maxLineNumber)
		if err != nil && err != sql.ErrNoRows {
			tx.Rollback()
			if attempt < maxRetries-1 && (err.Error() == "database is locked" || err.Error() == "database is locked (5)") {
				delay := baseDelay * time.Duration(1<<uint(attempt))
				time.Sleep(delay)
				continue
			}
			return err
		}

		startLineNumber := int64(1)
		if maxLineNumber.Valid {
			startLineNumber = maxLineNumber.Int64 + 1
		}

		// Insert all log lines in a batch for better performance
		now := time.Now().Format(time.RFC3339)
		validLines := make([]string, 0, len(lines))
		
		// Filter out empty lines and collect valid ones
		for _, line := range lines {
			if line != "" {
				validLines = append(validLines, line)
			}
		}
		
		if len(validLines) == 0 {
			tx.Rollback()
			return nil // No valid lines to insert
		}
		
		// Use batch insert for better performance
		// Build VALUES clause for batch insert
		valuePlaceholders := make([]string, len(validLines))
		args := make([]interface{}, 0, len(validLines)*5)
		argIndex := 1
		
		for i, line := range validLines {
			logID := uuid.New().String()
			lineNumber := startLineNumber + int64(i)
			valuePlaceholders[i] = fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)", 
				argIndex, argIndex+1, argIndex+2, argIndex+3, argIndex+4)
			args = append(args, logID, backupID, line, lineNumber, now)
			argIndex += 5
		}
		
		query := fmt.Sprintf(`
			INSERT INTO backup_logs (id, backup_id, log_line, line_number, created_at)
			VALUES %s`,
			strings.Join(valuePlaceholders, ", "))
		
		_, err = tx.Exec(query, args...)
		if err != nil {
			// If table doesn't exist yet, fall back to old method
			if strings.Contains(err.Error(), "no such table: backup_logs") {
				tx.Rollback()
				return r.appendLogLegacy(backupID, logLine)
			}
			
			tx.Rollback()
			if attempt < maxRetries-1 && (err.Error() == "database is locked" || err.Error() == "database is locked (5)") {
				delay := baseDelay * time.Duration(1<<uint(attempt))
				time.Sleep(delay)
				continue
			}
			return err
		}

		err = tx.Commit()
		if err != nil {
			if attempt < maxRetries-1 && (err.Error() == "database is locked" || err.Error() == "database is locked (5)") {
				delay := baseDelay * time.Duration(1<<uint(attempt))
				time.Sleep(delay)
				continue
			}
			return err
		}

		// Success
		return nil
	}

	return fmt.Errorf("failed to append log after %d retries", maxRetries)
}

// appendLogLegacy appends logs to the old logs column (for backward compatibility)
func (r *BackupRepository) appendLogLegacy(backupID string, logLine string) error {
	maxRetries := 5
	baseDelay := 10 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		tx, err := r.db.Begin()
		if err != nil {
			if attempt < maxRetries-1 && (err.Error() == "database is locked" || err.Error() == "database is locked (5)") {
				delay := baseDelay * time.Duration(1<<uint(attempt))
				time.Sleep(delay)
				continue
			}
			return err
		}

		var currentLogs sql.NullString
		err = tx.QueryRow(`SELECT logs FROM backups WHERE id = $1`, backupID).Scan(&currentLogs)
		if err != nil {
			tx.Rollback()
			if attempt < maxRetries-1 && (err.Error() == "database is locked" || err.Error() == "database is locked (5)") {
				delay := baseDelay * time.Duration(1<<uint(attempt))
				time.Sleep(delay)
				continue
			}
			return err
		}

		var newLogs string
		if currentLogs.Valid && currentLogs.String != "" {
			newLogs = currentLogs.String + "\n" + logLine
		} else {
			newLogs = logLine
		}

		_, err = tx.Exec(`UPDATE backups SET logs = $1, updated_at = $2 WHERE id = $3`,
			newLogs, time.Now().Format(time.RFC3339), backupID)
		if err != nil {
			tx.Rollback()
			if attempt < maxRetries-1 && (err.Error() == "database is locked" || err.Error() == "database is locked (5)") {
				delay := baseDelay * time.Duration(1<<uint(attempt))
				time.Sleep(delay)
				continue
			}
			return err
		}

		err = tx.Commit()
		if err != nil {
			if attempt < maxRetries-1 && (err.Error() == "database is locked" || err.Error() == "database is locked (5)") {
				delay := baseDelay * time.Duration(1<<uint(attempt))
				time.Sleep(delay)
				continue
			}
			return err
		}

		return nil
	}

	return fmt.Errorf("failed to append log after %d retries", maxRetries)
}

// GetBackupLogs retrieves the logs for a backup from the backup_logs table
// Falls back to the old logs column for backward compatibility
func (r *BackupRepository) GetBackupLogs(backupID string) (string, error) {
	// Try to get logs from the new backup_logs table first
	rows, err := r.db.Query(`
		SELECT log_line 
		FROM backup_logs 
		WHERE backup_id = $1 
		ORDER BY line_number ASC`,
		backupID)
	
	if err == nil {
		defer rows.Close()
		var logLines []string
		for rows.Next() {
			var logLine string
			if err := rows.Scan(&logLine); err == nil {
				logLines = append(logLines, logLine)
			}
		}
		
		if len(logLines) > 0 {
			return strings.Join(logLines, "\n"), nil
		}
	}

	// Fallback to old logs column for backward compatibility
	var logs sql.NullString
	err = r.db.QueryRow(`SELECT logs FROM backups WHERE id = $1`, backupID).Scan(&logs)
	if err != nil {
		return "", err
	}

	if logs.Valid {
		return logs.String, nil
	}
	return "", nil
}

func (r *BackupRepository) GetBackupStats(userID uuid.UUID) (*BackupStats, error) {
	stats := &BackupStats{
		TotalBackups:    0,
		FailedBackups:   0,
		TotalSize:       0,
		AverageDuration: 0,
		SuccessRate:     100, // Default to 100% if no backups
	}

	err := r.db.QueryRow(`
		SELECT 
				COALESCE(COUNT(*), 0) as total_backups,
				COALESCE(SUM(CASE WHEN b.status != 'completed' THEN 1 ELSE 0 END), 0) as failed_backups,
				COALESCE(SUM(b.size), 0) as total_size
		FROM backups b
		INNER JOIN connections c ON b.connection_id = c.id
		WHERE c.user_id = $1
	`, userID).Scan(&stats.TotalBackups, &stats.FailedBackups, &stats.TotalSize)
	if err != nil {
		if err == sql.ErrNoRows {
			return stats, nil // Return default values if no data
		}
		return nil, fmt.Errorf("failed to get backup counts: %v", err)
	}

	// Calculate success rate only if there are backups
	if stats.TotalBackups > 0 {
		successfulBackups := stats.TotalBackups - stats.FailedBackups
		stats.SuccessRate = float64(successfulBackups) / float64(stats.TotalBackups) * 100
	}

	// Calculate average duration for completed backups
	var totalDuration float64
	var completedBackups int
	rows, err := r.db.Query(`
		SELECT 
			b.started_time,
			b.completed_time
		FROM backups b
		INNER JOIN connections c ON b.connection_id = c.id
		WHERE c.user_id = $1 
		AND b.status = 'completed'
		AND b.completed_time IS NOT NULL
	`, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get backup durations: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var startStr, endStr string
		if err := rows.Scan(&startStr, &endStr); err != nil {
			continue
		}

		startTime, err := common.ParseTime(startStr)
		if err != nil {
			continue
		}

		endTime, err := common.ParseTime(endStr)
		if err != nil {
			continue
		}

		duration := endTime.Sub(startTime).Minutes()
		totalDuration += duration
		completedBackups++
	}

	if completedBackups > 0 {
		stats.AverageDuration = totalDuration / float64(completedBackups)
	}

	return stats, nil
}

// AddBackupS3Provider adds an S3 provider record for a backup
func (r *BackupRepository) AddBackupS3Provider(backupID, providerID, objectKey string) error {
	id := uuid.New().String()
	_, err := r.db.Exec(`
		INSERT INTO backup_s3_providers (id, backup_id, s3_provider_id, s3_object_key, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT(backup_id, s3_provider_id) DO UPDATE SET s3_object_key = $3, created_at = $5`,
		id, backupID, providerID, objectKey, time.Now().Format(time.RFC3339))
	return err
}

// BackupS3Provider represents an S3 provider for a backup
type BackupS3Provider struct {
	ProviderID string `json:"provider_id"`
	ObjectKey  string `json:"object_key"`
}

// GetBackupS3Providers returns all S3 providers for a backup
func (r *BackupRepository) GetBackupS3Providers(backupID string) ([]BackupS3Provider, error) {
	rows, err := r.db.Query(`
		SELECT s3_provider_id, s3_object_key
		FROM backup_s3_providers
		WHERE backup_id = $1
		ORDER BY created_at ASC`,
		backupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var providers []BackupS3Provider

	for rows.Next() {
		var providerID, objectKey string
		if err := rows.Scan(&providerID, &objectKey); err != nil {
			return nil, err
		}
		providers = append(providers, BackupS3Provider{
			ProviderID: providerID,
			ObjectKey:  objectKey,
		})
	}

	return providers, rows.Err()
}

// CreateShareableLink creates a shareable download link for a backup
func (r *BackupRepository) CreateShareableLink(backupID, providerID, token string, expiresAt time.Time) error {
	id := uuid.New().String()
	_, err := r.db.Exec(`
		INSERT INTO shareable_links (id, backup_id, s3_provider_id, token, expires_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		id, backupID, providerID, token, expiresAt.Format(time.RFC3339), time.Now().Format(time.RFC3339))
	return err
}

// GetShareableLink retrieves a shareable link by token
func (r *BackupRepository) GetShareableLink(token string) (backupID, providerID string, err error) {
	var expiresAtStr string
	err = r.db.QueryRow(`
		SELECT backup_id, s3_provider_id, expires_at
		FROM shareable_links
		WHERE token = $1`,
		token).Scan(&backupID, &providerID, &expiresAtStr)
	if err != nil {
		return "", "", err
	}

	// Check if expired
	expiresAt, err := common.ParseTime(expiresAtStr)
	if err != nil {
		return "", "", fmt.Errorf("invalid expiration time: %v", err)
	}

	if time.Now().After(expiresAt) {
		return "", "", fmt.Errorf("link has expired")
	}

	// Update access count
	_, err = r.db.Exec(`
		UPDATE shareable_links
		SET access_count = access_count + 1
		WHERE token = $1`,
		token)

	return backupID, providerID, err
}
