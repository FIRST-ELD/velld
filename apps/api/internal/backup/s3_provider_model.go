package backup

import (
	"time"

	"github.com/google/uuid"
)

// S3Provider represents an S3-compatible storage provider configuration
type S3Provider struct {
	ID        uuid.UUID `json:"id"`
	UserID    uuid.UUID `json:"user_id"`
	Name      string    `json:"name"`
	Endpoint  string    `json:"endpoint"`
	Region    *string   `json:"region,omitempty"`
	Bucket    string    `json:"bucket"`
	AccessKey string    `json:"access_key,omitempty"` // Omitted when returning to frontend for security
	SecretKey string    `json:"secret_key,omitempty"`  // Omitted when returning to frontend for security
	UseSSL    bool      `json:"use_ssl"`
	PathPrefix *string  `json:"path_prefix,omitempty"`
	IsDefault bool      `json:"is_default"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// S3ProviderRequest represents a request to create or update an S3 provider
type S3ProviderRequest struct {
	Name      string  `json:"name"`
	Endpoint  string  `json:"endpoint"`
	Region    *string `json:"region,omitempty"`
	Bucket    string  `json:"bucket"`
	AccessKey string  `json:"access_key"`
	SecretKey string  `json:"secret_key"`
	UseSSL    *bool   `json:"use_ssl,omitempty"`
	PathPrefix *string `json:"path_prefix,omitempty"`
	IsDefault *bool   `json:"is_default,omitempty"`
}

