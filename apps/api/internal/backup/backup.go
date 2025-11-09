package backup

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/dendianugerah/velld/internal/common"
	"github.com/dendianugerah/velld/internal/common/response"
	"github.com/gorilla/mux"
)

type BackupHandler struct {
	backupService *BackupService
}

func NewBackupHandler(bs *BackupService) *BackupHandler {
	return &BackupHandler{
		backupService: bs,
	}
}

func (h *BackupHandler) CreateBackup(w http.ResponseWriter, r *http.Request) {
	var req BackupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.SendError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Start backup asynchronously and return immediately
	backup, err := h.backupService.StartBackup(req.ConnectionID, req.S3ProviderIDs)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "Backup started successfully", backup)
}

func (h *BackupHandler) GetBackup(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	backupID := vars["id"]

	backup, err := h.backupService.GetBackup(backupID)
	if err != nil {
		if err == sql.ErrNoRows {
			response.SendError(w, http.StatusNotFound, "Backup not found")
			return
		}
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "Backup retrieved successfully", backup)
}

func (h *BackupHandler) ListBackups(w http.ResponseWriter, r *http.Request) {
	userID, err := common.GetUserIDFromContext(r.Context())
	if err != nil {
		response.SendError(w, http.StatusBadRequest, err.Error())
		return
	}

	page := 1
	limit := 10
	if pageStr := r.URL.Query().Get("page"); pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
			page = p
		}
	}
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	search := r.URL.Query().Get("search")
	offset := (page - 1) * limit

	opts := BackupListOptions{
		UserID: userID,
		Limit:  limit,
		Offset: offset,
		Search: search,
	}

	backups, total, err := h.backupService.GetAllBackupsWithPagination(opts)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendPaginatedSuccess(w, "Backups retrieved successfully", backups, page, limit, total)
}

func (h *BackupHandler) ScheduleBackup(w http.ResponseWriter, r *http.Request) {
	var req ScheduleBackupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.SendError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.ConnectionID == "" {
		response.SendError(w, http.StatusBadRequest, "connection_id is required")
		return
	}
	if req.CronSchedule == "" {
		response.SendError(w, http.StatusBadRequest, "cron_schedule is required")
		return
	}
	if req.RetentionDays <= 0 {
		response.SendError(w, http.StatusBadRequest, "retention_days must be greater than 0")
		return
	}

	err := h.backupService.ScheduleBackup(&req)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "Backup scheduled successfully", nil)
}

func (h *BackupHandler) DisableBackupSchedule(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	connectionID := vars["connection_id"]

	err := h.backupService.DisableBackupSchedule(connectionID)
	if err != nil {
		if err == sql.ErrNoRows {
			response.SendError(w, http.StatusNotFound, "No active schedule found")
			return
		}
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "Backup schedule disabled successfully", nil)
}

func (h *BackupHandler) UpdateBackupSchedule(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	connectionID := vars["connection_id"]

	var req UpdateScheduleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.SendError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.CronSchedule == "" {
		response.SendError(w, http.StatusBadRequest, "cron_schedule is required")
		return
	}
	if req.RetentionDays <= 0 {
		response.SendError(w, http.StatusBadRequest, "retention_days must be greater than 0")
		return
	}

	err := h.backupService.UpdateBackupSchedule(connectionID, &req)
	if err != nil {
		if err == sql.ErrNoRows {
			response.SendError(w, http.StatusNotFound, "No active schedule found")
			return
		}
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "Backup schedule updated successfully", nil)
}

func (h *BackupHandler) GetBackupStats(w http.ResponseWriter, r *http.Request) {
	userID, err := common.GetUserIDFromContext(r.Context())
	if err != nil {
		response.SendError(w, http.StatusBadRequest, err.Error())
		return
	}

	stats, err := h.backupService.GetBackupStats(userID)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "Backup statistics retrieved successfully", stats)
}

func (h *BackupHandler) DownloadBackup(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	backupID := vars["id"]
	
	// Check for optional provider_id query parameter
	providerID := r.URL.Query().Get("provider_id")

	backup, err := h.backupService.GetBackup(backupID)
	if err != nil {
		if err == sql.ErrNoRows {
			response.SendError(w, http.StatusNotFound, "Backup not found")
			return
		}
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	userID, err := common.GetUserIDFromContext(r.Context())
	if err != nil {
		response.SendError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	// If provider_id is specified, use that provider
	if providerID != "" {
		// Get all S3 providers for this backup
		providers, err := h.backupService.GetBackupS3Providers(backupID)
		if err == nil {
			// Find the specified provider
			for _, p := range providers {
				if p.ProviderID == providerID {
					s3Storage, err := h.backupService.GetS3ProviderForDownload(providerID, userID)
					if err == nil {
						ctx := r.Context()
						object, err := s3Storage.GetObject(ctx, p.ObjectKey)
						if err == nil {
							defer object.Close()
							
							filename := filepath.Base(backup.Path)
							w.Header().Set("Content-Disposition", "attachment; filename="+filename)
							w.Header().Set("Content-Type", "application/octet-stream")
							
							_, err = io.Copy(w, object)
							if err == nil {
								return // Successfully downloaded from specified S3 provider
							}
						}
					}
					break
				}
			}
		}
		response.SendError(w, http.StatusBadRequest, "Invalid or inaccessible S3 provider")
		return
	}

	// Try to download from default S3 provider first
	if backup.S3ObjectKey != nil && backup.S3ProviderID != nil {
		// Get the S3 storage
		s3Storage, err := h.backupService.GetS3ProviderForDownload(*backup.S3ProviderID, userID)
		if err == nil {
			// Download from S3
			ctx := r.Context()
			object, err := s3Storage.GetObject(ctx, *backup.S3ObjectKey)
			if err == nil {
				defer object.Close()
				
				filename := filepath.Base(backup.Path)
				w.Header().Set("Content-Disposition", "attachment; filename="+filename)
				w.Header().Set("Content-Type", "application/octet-stream")
				
				_, err = io.Copy(w, object)
				if err == nil {
					return // Successfully downloaded from S3
				}
			}
		}
		// If S3 download fails, fall through to local file
	}

	// Fallback to local file if S3 is not available
	if _, err := os.Stat(backup.Path); err == nil {
		file, err := os.Open(backup.Path)
		if err == nil {
			defer file.Close()
			
			filename := filepath.Base(backup.Path)
			w.Header().Set("Content-Disposition", "attachment; filename="+filename)
			w.Header().Set("Content-Type", "application/octet-stream")
			
			_, err = io.Copy(w, file)
			if err == nil {
				return
			}
		}
	}

	response.SendError(w, http.StatusInternalServerError, "Failed to download backup file")
}

func (h *BackupHandler) RestoreBackup(w http.ResponseWriter, r *http.Request) {
	var req RestoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.SendError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.BackupID == "" {
		response.SendError(w, http.StatusBadRequest, "backup_id is required")
		return
	}

	if req.ConnectionID == "" {
		response.SendError(w, http.StatusBadRequest, "connection_id is required")
		return
	}

	err := h.backupService.RestoreBackup(req.BackupID, req.ConnectionID)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "Backup restored successfully", nil)
}

func (h *BackupHandler) StreamBackupLogs(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	backupID := vars["id"]

	if backupID == "" {
		response.SendError(w, http.StatusBadRequest, "backup_id is required")
		return
	}

	// Set headers for Server-Sent Events
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Cache-Control")

	// Get the log stream
	logStream := h.backupService.GetLogStream(backupID)
	if logStream == nil {
		// Stream doesn't exist yet, send a message and wait
		fmt.Fprintf(w, "data: %s\n\n", jsonEscape("Waiting for backup to start..."))
		w.(http.Flusher).Flush()

		// Wait a bit and check again
		timeout := time.After(30 * time.Second)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-timeout:
				fmt.Fprintf(w, "data: %s\n\n", jsonEscape("Backup not found or already completed"))
				w.(http.Flusher).Flush()
				return
			case <-ticker.C:
				logStream = h.backupService.GetLogStream(backupID)
				if logStream != nil {
					goto streamLogs
				}
			case <-r.Context().Done():
				return
			}
		}
	}

streamLogs:
	// Stream logs
	for {
		select {
		case log, ok := <-logStream:
			if !ok {
				// Channel closed, send final message
				fmt.Fprintf(w, "data: %s\n\n", jsonEscape("[STREAM ENDED]"))
				w.(http.Flusher).Flush()
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", jsonEscape(log))
			w.(http.Flusher).Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func (h *BackupHandler) GetActiveBackups(w http.ResponseWriter, r *http.Request) {
	userID, err := common.GetUserIDFromContext(r.Context())
	if err != nil {
		response.SendError(w, http.StatusBadRequest, err.Error())
		return
	}

	backups, err := h.backupService.GetActiveBackups(userID)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "Active backups retrieved successfully", backups)
}

func (h *BackupHandler) GetBackupLogs(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	backupID := vars["id"]

	if backupID == "" {
		response.SendError(w, http.StatusBadRequest, "backup_id is required")
		return
	}

	logs, err := h.backupService.GetBackupLogs(backupID)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "Backup logs retrieved successfully", map[string]string{
		"logs": logs,
	})
}

func (h *BackupHandler) GetBackupS3Providers(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	backupID := vars["id"]

	providers, err := h.backupService.GetBackupS3Providers(backupID)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "S3 providers retrieved successfully", providers)
}

func (h *BackupHandler) CreateShareableLink(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	backupID := vars["id"]

	var req struct {
		ProviderID string `json:"provider_id,omitempty"`
		ExpiresIn  int    `json:"expires_in"` // Hours until expiration
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.SendError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.ExpiresIn <= 0 {
		req.ExpiresIn = 24 // Default to 24 hours
	}
	if req.ExpiresIn > 168 { // Max 7 days
		req.ExpiresIn = 168
	}

	link, err := h.backupService.CreateShareableLink(backupID, req.ProviderID, req.ExpiresIn)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "Shareable link created successfully", link)
}

func (h *BackupHandler) DownloadViaShareableLink(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	token := vars["token"]

	backupID, providerID, err := h.backupService.ValidateShareableLink(token)
	if err != nil {
		response.SendError(w, http.StatusNotFound, "Invalid or expired link")
		return
	}

	backup, err := h.backupService.GetBackup(backupID)
	if err != nil {
		response.SendError(w, http.StatusNotFound, "Backup not found")
		return
	}

	// Get S3 providers for this backup
	providers, err := h.backupService.GetBackupS3Providers(backupID)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Find the provider and object key
	var objectKey string
	for _, p := range providers {
		if providerID == "" || p.ProviderID == providerID {
			objectKey = p.ObjectKey
			if providerID == "" {
				providerID = p.ProviderID
			}
			break
		}
	}

	if objectKey == "" {
		response.SendError(w, http.StatusNotFound, "S3 object not found")
		return
	}

	// Get connection to find user ID (needed for S3 access)
	conn, err := h.backupService.GetConnection(backup.ConnectionID)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Get S3 storage
	s3Storage, err := h.backupService.GetS3ProviderForDownload(providerID, conn.UserID)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Download from S3
	ctx := r.Context()
	object, err := s3Storage.GetObject(ctx, objectKey)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, "Failed to download from S3")
		return
	}
	defer object.Close()

	filename := filepath.Base(backup.Path)
	w.Header().Set("Content-Disposition", "attachment; filename="+filename)
	w.Header().Set("Content-Type", "application/octet-stream")

	_, err = io.Copy(w, object)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, "Failed to send file")
		return
	}
}

// jsonEscape escapes a string for JSON encoding
func jsonEscape(s string) string {
	b, _ := json.Marshal(s)
	return string(b[1 : len(b)-1])
}
