package backup

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// CalculateFileChecksums calculates both MD5 and SHA256 checksums for a file
func CalculateFileChecksums(filePath string) (md5Hash, sha256Hash string, err error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	md5Hasher := md5.New()
	sha256Hasher := sha256.New()

	// Use multi-writer to calculate both hashes in a single pass
	multiWriter := io.MultiWriter(md5Hasher, sha256Hasher)

	_, err = io.Copy(multiWriter, file)
	if err != nil {
		return "", "", fmt.Errorf("failed to read file: %w", err)
	}

	md5Hash = hex.EncodeToString(md5Hasher.Sum(nil))
	sha256Hash = hex.EncodeToString(sha256Hasher.Sum(nil))

	return md5Hash, sha256Hash, nil
}

// CalculateStreamChecksums calculates both MD5 and SHA256 checksums from a reader
// Returns a new reader that can be used to read the data while calculating checksums
func CalculateStreamChecksums(reader io.Reader) (io.Reader, func() (string, string, error)) {
	md5Hasher := md5.New()
	sha256Hasher := sha256.New()
	multiWriter := io.MultiWriter(md5Hasher, sha256Hasher)

	// Create a tee reader that writes to both hashers while reading
	teeReader := io.TeeReader(reader, multiWriter)

	getChecksums := func() (string, string, error) {
		md5Hash := hex.EncodeToString(md5Hasher.Sum(nil))
		sha256Hash := hex.EncodeToString(sha256Hasher.Sum(nil))
		return md5Hash, sha256Hash, nil
	}

	return teeReader, getChecksums
}

// VerifyFileChecksum verifies a file's checksum against an expected value
func VerifyFileChecksum(filePath string, expectedSHA256 string) error {
	if expectedSHA256 == "" {
		return fmt.Errorf("expected checksum is empty")
	}

	_, calculatedSHA256, err := CalculateFileChecksums(filePath)
	if err != nil {
		return fmt.Errorf("failed to calculate checksum: %w", err)
	}

	if calculatedSHA256 != expectedSHA256 {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expectedSHA256, calculatedSHA256)
	}

	return nil
}

