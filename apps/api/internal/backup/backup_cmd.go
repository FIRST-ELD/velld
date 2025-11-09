package backup

import (
	"fmt"
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

	// Base arguments for pg_dump
	args := []string{
		"-h", conn.Host,
		"-p", fmt.Sprintf("%d", conn.Port),
		"-U", conn.Username,
		"-d", conn.DatabaseName,
		"-F", "p", // Plain text format (SQL script)
		"-f", outputPath,
		"--no-owner",      // Don't dump ownership commands (helps with TimescaleDB and cross-database restores)
		"--no-privileges", // Don't dump access privileges (helps with TimescaleDB and cross-database restores)
		"--verbose",       // Verbose output shows progress: what tables/schemas are being dumped
		// Note: --progress flag is only available for directory format (-F d), not plain format
		// For plain format, we rely on --verbose output and file size monitoring
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
	cmd := exec.Command(binPath,
		"-h", conn.Host,
		"-P", fmt.Sprintf("%d", conn.Port),
		"-u", conn.Username,
		fmt.Sprintf("-p%s", conn.Password),
		conn.DatabaseName,
		"-r", outputPath,
	)
	return cmd
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
