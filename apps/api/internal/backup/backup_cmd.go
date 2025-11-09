package backup

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/dendianugerah/velld/internal/common"
	"github.com/dendianugerah/velld/internal/connection"
)

var requiredTools = map[string]string{
	"postgresql": "pg_dump",
	"mysql":      "mysqldump",
	"mariadb":    "mysqldump",
	"mongodb":    "mongodump",
	"redis":      "redis-cli",
}

func (s *BackupService) verifyBackupTools(dbType string) error {
	if _, exists := requiredTools[dbType]; !exists {
		return fmt.Errorf("unsupported database type: %s", dbType)
	}
	return nil
}

func (s *BackupService) findDatabaseBinaryPath(dbType string) string {
	if path := common.FindBinaryPath(dbType, requiredTools[dbType]); path != "" {
		return path
	}

	return ""
}

func (s *BackupService) setupSSHTunnelIfNeeded(conn *connection.StoredConnection) (*connection.SSHTunnel, string, int, error) {
	if !conn.SSHEnabled {
		return nil, conn.Host, conn.Port, nil
	}

	tunnel, err := connection.NewSSHTunnel(
		conn.SSHHost,
		conn.SSHPort,
		conn.SSHUsername,
		conn.SSHPassword,
		conn.SSHPrivateKey,
		conn.Host,
		conn.Port,
	)
	if err != nil {
		return nil, "", 0, fmt.Errorf("failed to create SSH tunnel: %w", err)
	}

	if err := tunnel.Start(); err != nil {
		return nil, "", 0, fmt.Errorf("failed to start SSH tunnel: %w", err)
	}

	return tunnel, "127.0.0.1", tunnel.GetLocalPort(), nil
}

func (s *BackupService) createPgDumpCmd(conn *connection.StoredConnection, outputPath string) *exec.Cmd {
	binaryPath := s.findDatabaseBinaryPath("postgresql")
	if binaryPath == "" {
		fmt.Printf("ERROR: pg_dump binary not found. Please install PostgreSQL client tools.\n")
		return nil
	}

	binPath := filepath.Join(binaryPath, common.GetPlatformExecutableName(requiredTools["postgresql"]))

	// Use custom format (-F c) for better compression and faster restores
	// Custom format is compressed internally and allows parallel restore
	args := []string{
		"-h", conn.Host,
		"-p", fmt.Sprintf("%d", conn.Port),
		"-U", conn.Username,
		"-d", conn.DatabaseName,
		"-F", "c", // Custom format (compressed internally, faster restores)
		"-f", outputPath,
		"--no-owner",      // Don't dump ownership commands (helps with TimescaleDB and cross-database restores)
		"--no-privileges", // Don't dump access privileges (helps with TimescaleDB and cross-database restores)
		"--verbose",       // Verbose output shows progress: what tables/schemas are being dumped
	}

	// Check if TimescaleDB is installed and log appropriate message
	if s.isTimescaleDBInstalled(conn) {
		// For TimescaleDB, the warnings about circular foreign keys in hypertable, chunk, and continuous_agg
		// tables are expected and safe to ignore. These are part of TimescaleDB's internal architecture.
		// The --no-owner and --no-privileges flags help ensure the backup can be restored properly.
		// Note: We don't exclude these tables as they contain important metadata for hypertables.
	}

	cmd := exec.Command(binPath, args...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", conn.Password))
	return cmd
}

// createPgDumpCmdForStreaming creates a pg_dump command that outputs to stdout
// Uses plain format (-F p) since custom format doesn't support stdout
func (s *BackupService) createPgDumpCmdForStreaming(conn *connection.StoredConnection) *exec.Cmd {
	binaryPath := s.findDatabaseBinaryPath("postgresql")
	if binaryPath == "" {
		fmt.Printf("ERROR: pg_dump binary not found. Please install PostgreSQL client tools.\n")
		return nil
	}

	binPath := filepath.Join(binaryPath, common.GetPlatformExecutableName(requiredTools["postgresql"]))

	// Use plain format (-F p) for stdout streaming
	// We'll compress it on-the-fly during upload
	args := []string{
		"-h", conn.Host,
		"-p", fmt.Sprintf("%d", conn.Port),
		"-U", conn.Username,
		"-d", conn.DatabaseName,
		"-F", "p", // Plain format (SQL script) - supports stdout
		"--no-owner",      // Don't dump ownership commands
		"--no-privileges", // Don't dump access privileges
		"--verbose",       // Verbose output shows progress
	}

	cmd := exec.Command(binPath, args...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", conn.Password))
	return cmd
}

// compressBackup compresses a backup file using gzip
func (s *BackupService) compressBackup(inputPath, outputPath string) error {
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer inputFile.Close()

	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outputFile.Close()

	gzipWriter := gzip.NewWriter(outputFile)
	defer gzipWriter.Close()

	_, err = io.Copy(gzipWriter, inputFile)
	if err != nil {
		return fmt.Errorf("failed to compress file: %w", err)
	}

	return nil
}

// isTimescaleDBInstalled checks if TimescaleDB extension is installed in the database
func (s *BackupService) isTimescaleDBInstalled(conn *connection.StoredConnection) bool {
	psqlPath := common.FindBinaryPath("postgresql", "psql")
	if psqlPath == "" {
		return false
	}

	binPath := filepath.Join(psqlPath, common.GetPlatformExecutableName("psql"))
	
	// Query to check if TimescaleDB extension exists
	cmd := exec.Command(binPath,
		"-h", conn.Host,
		"-p", fmt.Sprintf("%d", conn.Port),
		"-U", conn.Username,
		"-d", conn.DatabaseName,
		"-t", "-A", // terse, aligned output
		"-c", "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb');",
	)
	
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", conn.Password))
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	result := strings.TrimSpace(string(output))
	return result == "t" || result == "true" || result == "1"
}

// getPgDumpVersion returns the version of pg_dump being used
func (s *BackupService) getPgDumpVersion() (string, error) {
	binaryPath := s.findDatabaseBinaryPath("postgresql")
	if binaryPath == "" {
		return "", fmt.Errorf("pg_dump binary not found")
	}

	binPath := filepath.Join(binaryPath, common.GetPlatformExecutableName(requiredTools["postgresql"]))
	cmd := exec.Command(binPath, "--version")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get pg_dump version: %v", err)
	}

	return strings.TrimSpace(string(output)), nil
}

// getPostgreSQLServerVersion returns the PostgreSQL server version
func (s *BackupService) getPostgreSQLServerVersion(conn *connection.StoredConnection) (string, error) {
	// Find psql binary - we'll use common.FindBinaryPath directly since we need psql
	psqlPath := common.FindBinaryPath("postgresql", "psql")
	if psqlPath == "" {
		return "", fmt.Errorf("psql binary not found")
	}

	binPath := filepath.Join(psqlPath, common.GetPlatformExecutableName("psql"))

	// Query server version using psql
	cmd := exec.Command(binPath,
		"-h", conn.Host,
		"-p", fmt.Sprintf("%d", conn.Port),
		"-U", conn.Username,
		"-d", conn.DatabaseName,
		"-t", "-A", // terse, aligned output
		"-c", "SELECT version();",
	)
	
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", conn.Password))
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get server version: %v", err)
	}

	version := strings.TrimSpace(string(output))
	// Extract just the version number (e.g., "PostgreSQL 16.1" from full version string)
	if strings.Contains(version, "PostgreSQL") {
		parts := strings.Fields(version)
		for i, part := range parts {
			if part == "PostgreSQL" && i+1 < len(parts) {
				return strings.TrimSpace(parts[i+1]), nil
			}
		}
	}
	
	return version, nil
}

func (s *BackupService) createMySQLDumpCmd(conn *connection.StoredConnection, outputPath string) *exec.Cmd {
	binaryPath := s.findDatabaseBinaryPath(conn.Type)
	if binaryPath == "" {
		fmt.Printf("ERROR: mysqldump binary not found. Please install MySQL/MariaDB client tools.\n")
		return nil
	}

	binPath := filepath.Join(binaryPath, common.GetPlatformExecutableName(requiredTools[conn.Type]))
	
	// Enhanced mysqldump options for efficiency
	args := []string{
		"-h", conn.Host,
		"-P", fmt.Sprintf("%d", conn.Port),
		"-u", conn.Username,
		fmt.Sprintf("-p%s", conn.Password),
		"--single-transaction", // Consistent backup for InnoDB
		"--quick",              // Retrieve rows one at a time (reduces memory usage)
		"--lock-tables=false",  // Don't lock all tables (works with --single-transaction)
		"--routines",           // Include stored procedures and functions
		"--triggers",           // Include triggers
		"--events",             // Include events
		conn.DatabaseName,
	}
	
	// If output path is empty or "-", output to stdout for streaming (no -r flag)
	// Otherwise, write to file
	if outputPath != "" && outputPath != "-" {
		if strings.HasSuffix(outputPath, ".gz") {
			// Remove .gz extension for mysqldump output, we'll compress it
			args = append(args, "-r", strings.TrimSuffix(outputPath, ".gz"))
		} else {
			args = append(args, "-r", outputPath)
		}
	}
	// If outputPath is "" or "-", mysqldump will output to stdout by default
	
	cmd := exec.Command(binPath, args...)
	return cmd
}

// createMySQLDumpCmdForStreaming creates a mysqldump command that outputs to stdout
func (s *BackupService) createMySQLDumpCmdForStreaming(conn *connection.StoredConnection) *exec.Cmd {
	return s.createMySQLDumpCmd(conn, "-") // "-" means stdout
}

func (s *BackupService) createMongoDumpCmd(conn *connection.StoredConnection, outputPath string) *exec.Cmd {
	binaryPath := s.findDatabaseBinaryPath("mongodb")
	if binaryPath == "" {
		fmt.Printf("ERROR: mongodump binary not found. Please install MongoDB Database Tools.\n")
		return nil
	}

	binPath := filepath.Join(binaryPath, common.GetPlatformExecutableName(requiredTools["mongodb"]))
	args := []string{
		"--host", conn.Host,
		"--port", fmt.Sprintf("%d", conn.Port),
		"--db", conn.DatabaseName,
		"--out", filepath.Dir(outputPath),
	}

	if conn.Username != "" {
		args = append(args, "--username", conn.Username)
	}

	if conn.Password != "" {
		args = append(args, "--password", conn.Password)
	}

	return exec.Command(binPath, args...)
}

func (s *BackupService) createRedisDumpCmd(conn *connection.StoredConnection, outputPath string) *exec.Cmd {
	binaryPath := s.findDatabaseBinaryPath("redis")
	if binaryPath == "" {
		fmt.Printf("ERROR: redis-cli binary not found. Please install Redis tools.\n")
		return nil
	}

	binPath := filepath.Join(binaryPath, common.GetPlatformExecutableName(requiredTools["redis"]))
	args := []string{
		"-h", conn.Host,
		"-p", fmt.Sprintf("%d", conn.Port),
	}

	if conn.Password != "" {
		args = append(args, "-a", conn.Password)
	}

	if conn.DatabaseName != "" {
		args = append(args, "-n", conn.DatabaseName)
	}

	args = append(args, "--rdb", outputPath)

	return exec.Command(binPath, args...)
}
