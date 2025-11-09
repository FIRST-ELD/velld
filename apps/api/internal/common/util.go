package common

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

func ParseTime(timeStr string) (time.Time, error) {
	formats := []string{
		time.RFC3339,                // "2006-01-02T15:04:05Z07:00"
		"2006-01-02 15:04:05-07:00", // SQLite format with timezone
		"2006-01-02 15:04:05+07:00", // SQLite format with timezone
		"2006-01-02 15:04:05",       // SQLite format without timezone
	}

	var lastErr error
	for _, format := range formats {
		t, err := time.Parse(format, timeStr)
		if err == nil {
			return t, nil
		}
		lastErr = err
	}
	return time.Time{}, fmt.Errorf("could not parse time '%s': %v", timeStr, lastErr)
}

func GetUserIDFromContext(ctx context.Context) (uuid.UUID, error) {
	claims, ok := ctx.Value("user").(jwt.MapClaims)
	if !ok {
		return uuid.Nil, fmt.Errorf("invalid user claims")
	}

	userIDStr := fmt.Sprintf("%v", claims["user_id"])
	userID, err := uuid.Parse(userIDStr)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid user ID format: %v", err)
	}

	return userID, nil
}

var CommonBinaryPaths = map[string][]string{
	"windows": {
		"C:\\Program Files\\PostgreSQL\\*\\bin",
		"C:\\Program Files\\MySQL\\*\\bin",
		"C:\\Program Files\\MariaDB*\\bin",
		"C:\\Program Files\\MongoDB\\*\\bin",
	},
	"linux": {
		"/usr/bin",
		"/usr/local/bin",
		"/opt/postgresql*/bin",
		"/opt/mysql*/bin",
	},
	"darwin": {
		"/opt/homebrew/bin",
		"/usr/local/bin",
		"/opt/homebrew/opt/postgresql@*/bin",
		"/opt/homebrew/opt/mysql@*/bin",
		"/usr/local/opt/postgresql@*/bin",  // Intel Mac Homebrew PostgreSQL versions
		"/usr/local/opt/mysql@*/bin",       // Intel Mac Homebrew MySQL versions
	},
}

func FindBinaryPath(dbType, toolName string) string {
	execName := GetPlatformExecutableName(toolName)

	// 1. Try user-defined path if provided
	// if userPath != nil && *userPath != "" {
	// 	toolPath := filepath.Join(*userPath, execName)
	// 	if _, err := os.Stat(toolPath); err == nil {
	// 		return *userPath
	// 	}
	// }

	// 2. Search common installation paths with wildcard support
	// Prioritize versioned paths (postgresql@*, mysql@*) over generic paths
	if paths, ok := CommonBinaryPaths[runtime.GOOS]; ok {
		var versionedPaths []string
		var genericPaths []string
		
		// Separate versioned and generic paths
		for _, pathPattern := range paths {
			if strings.Contains(pathPattern, "@") {
				versionedPaths = append(versionedPaths, pathPattern)
			} else {
				genericPaths = append(genericPaths, pathPattern)
			}
		}
		
		// Search versioned paths first (they're more specific)
		for _, pathPattern := range versionedPaths {
			matches, _ := filepath.Glob(pathPattern)
			// Sort matches to prefer higher version numbers
			if len(matches) > 1 {
				// Simple sort: extract version numbers and sort descending
				matches = sortPathsByVersion(matches)
			}
			for _, path := range matches {
				toolPath := filepath.Join(path, execName)
				if _, err := os.Stat(toolPath); err == nil {
					return path
				}
			}
		}
		
		// Then search generic paths
		for _, pathPattern := range genericPaths {
			matches, _ := filepath.Glob(pathPattern)
			for _, path := range matches {
				toolPath := filepath.Join(path, execName)
				if _, err := os.Stat(toolPath); err == nil {
					return path
				}
			}
		}
	}

	// 3. Try PATH environment as last resort
	if path, err := exec.LookPath(execName); err == nil {
		return filepath.Dir(path)
	}

	return ""
}

// sortPathsByVersion sorts paths containing version numbers in descending order
// e.g., ["postgresql@14", "postgresql@16"] -> ["postgresql@16", "postgresql@14"]
func sortPathsByVersion(paths []string) []string {
	// Extract version numbers and sort
	type pathVersion struct {
		path    string
		version int
	}
	
	var versions []pathVersion
	for _, path := range paths {
		// Extract version number from path like "/usr/local/opt/postgresql@16/bin"
		parts := strings.Split(path, "@")
		if len(parts) > 1 {
			versionPart := strings.Split(parts[1], "/")[0]
			// Try to extract numeric version
			var version int
			fmt.Sscanf(versionPart, "%d", &version)
			versions = append(versions, pathVersion{path: path, version: version})
		} else {
			versions = append(versions, pathVersion{path: path, version: 0})
		}
	}
	
	// Sort by version descending
	for i := 0; i < len(versions)-1; i++ {
		for j := i + 1; j < len(versions); j++ {
			if versions[i].version < versions[j].version {
				versions[i], versions[j] = versions[j], versions[i]
			}
		}
	}
	
	result := make([]string, len(versions))
	for i, v := range versions {
		result[i] = v.path
	}
	return result
}

func GetPlatformExecutableName(name string) string {
	if runtime.GOOS == "windows" {
		return name + ".exe"
	}
	return name
}

func SanitizeConnectionName(name string) string {
	name = strings.ToLower(name)

	re := regexp.MustCompile(`[^a-z0-9]+`)
	sanitized := re.ReplaceAllString(name, "_")

	sanitized = strings.Trim(sanitized, "_")

	if len(sanitized) > 200 {
		sanitized = sanitized[:200]
		sanitized = strings.TrimRight(sanitized, "_")
	}

	if sanitized == "" {
		sanitized = "backup"
	}

	return sanitized
}
