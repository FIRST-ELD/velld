package backup

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/dendianugerah/velld/internal/common"
	"github.com/google/uuid"
)

type S3ProviderRepository struct {
	db *sql.DB
}

func NewS3ProviderRepository(db *sql.DB) *S3ProviderRepository {
	return &S3ProviderRepository{
		db: db,
	}
}

func (r *S3ProviderRepository) CreateS3Provider(provider *S3Provider) error {
	now := time.Now().Format(time.RFC3339)
	_, err := r.db.Exec(`
		INSERT INTO s3_providers (
			id, user_id, name, endpoint, region, bucket, access_key, secret_key,
			use_ssl, path_prefix, is_default, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		provider.ID, provider.UserID, provider.Name, provider.Endpoint,
		provider.Region, provider.Bucket, provider.AccessKey, provider.SecretKey,
		provider.UseSSL, provider.PathPrefix, provider.IsDefault, now, now)
	return err
}

func (r *S3ProviderRepository) GetS3Provider(id string, userID uuid.UUID) (*S3Provider, error) {
	var (
		regionStr      sql.NullString
		pathPrefixStr  sql.NullString
		createdAtStr   string
		updatedAtStr   string
	)
	
	provider := &S3Provider{}
	err := r.db.QueryRow(`
		SELECT id, user_id, name, endpoint, region, bucket, access_key, secret_key,
		       use_ssl, path_prefix, is_default, created_at, updated_at
		FROM s3_providers
		WHERE id = $1 AND user_id = $2`, id, userID).
		Scan(&provider.ID, &provider.UserID, &provider.Name, &provider.Endpoint,
			&regionStr, &provider.Bucket, &provider.AccessKey, &provider.SecretKey,
			&provider.UseSSL, &pathPrefixStr, &provider.IsDefault,
			&createdAtStr, &updatedAtStr)
	
	if err != nil {
		return nil, err
	}

	if regionStr.Valid {
		provider.Region = &regionStr.String
	}
	if pathPrefixStr.Valid {
		provider.PathPrefix = &pathPrefixStr.String
	}

	createdAt, err := common.ParseTime(createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing created_at: %v", err)
	}
	provider.CreatedAt = createdAt

	updatedAt, err := common.ParseTime(updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing updated_at: %v", err)
	}
	provider.UpdatedAt = updatedAt

	return provider, nil
}

func (r *S3ProviderRepository) ListS3Providers(userID uuid.UUID) ([]*S3Provider, error) {
	rows, err := r.db.Query(`
		SELECT id, user_id, name, endpoint, region, bucket, access_key, secret_key,
		       use_ssl, path_prefix, is_default, created_at, updated_at
		FROM s3_providers
		WHERE user_id = $1
		ORDER BY is_default DESC, created_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var providers []*S3Provider
	for rows.Next() {
		var (
			regionStr      sql.NullString
			pathPrefixStr  sql.NullString
			createdAtStr   string
			updatedAtStr   string
		)
		
		provider := &S3Provider{}
		err := rows.Scan(&provider.ID, &provider.UserID, &provider.Name, &provider.Endpoint,
			&regionStr, &provider.Bucket, &provider.AccessKey, &provider.SecretKey,
			&provider.UseSSL, &pathPrefixStr, &provider.IsDefault,
			&createdAtStr, &updatedAtStr)
		if err != nil {
			return nil, err
		}

		if regionStr.Valid {
			provider.Region = &regionStr.String
		}
		if pathPrefixStr.Valid {
			provider.PathPrefix = &pathPrefixStr.String
		}

		createdAt, err := common.ParseTime(createdAtStr)
		if err != nil {
			return nil, fmt.Errorf("error parsing created_at: %v", err)
		}
		provider.CreatedAt = createdAt

		updatedAt, err := common.ParseTime(updatedAtStr)
		if err != nil {
			return nil, fmt.Errorf("error parsing updated_at: %v", err)
		}
		provider.UpdatedAt = updatedAt

		providers = append(providers, provider)
	}

	return providers, rows.Err()
}

func (r *S3ProviderRepository) UpdateS3Provider(provider *S3Provider) error {
	now := time.Now().Format(time.RFC3339)
	_, err := r.db.Exec(`
		UPDATE s3_providers SET
			name = $1, endpoint = $2, region = $3, bucket = $4,
			access_key = $5, secret_key = $6, use_ssl = $7, path_prefix = $8,
			is_default = $9, updated_at = $10
		WHERE id = $11 AND user_id = $12`,
		provider.Name, provider.Endpoint, provider.Region, provider.Bucket,
		provider.AccessKey, provider.SecretKey, provider.UseSSL, provider.PathPrefix,
		provider.IsDefault, now, provider.ID, provider.UserID)
	return err
}

func (r *S3ProviderRepository) DeleteS3Provider(id string, userID uuid.UUID) error {
	_, err := r.db.Exec(`DELETE FROM s3_providers WHERE id = $1 AND user_id = $2`, id, userID)
	return err
}

func (r *S3ProviderRepository) SetDefaultProvider(userID uuid.UUID, providerID string) error {
	// First, unset all defaults for this user
	_, err := r.db.Exec(`UPDATE s3_providers SET is_default = 0 WHERE user_id = $1`, userID)
	if err != nil {
		return err
	}

	// Then set the specified provider as default
	_, err = r.db.Exec(`UPDATE s3_providers SET is_default = 1, updated_at = $1 WHERE id = $2 AND user_id = $3`,
		time.Now().Format(time.RFC3339), providerID, userID)
	return err
}

func (r *S3ProviderRepository) GetDefaultProvider(userID uuid.UUID) (*S3Provider, error) {
	var (
		regionStr      sql.NullString
		pathPrefixStr  sql.NullString
		createdAtStr   string
		updatedAtStr   string
	)
	
	provider := &S3Provider{}
	err := r.db.QueryRow(`
		SELECT id, user_id, name, endpoint, region, bucket, access_key, secret_key,
		       use_ssl, path_prefix, is_default, created_at, updated_at
		FROM s3_providers
		WHERE user_id = $1 AND is_default = 1
		LIMIT 1`, userID).
		Scan(&provider.ID, &provider.UserID, &provider.Name, &provider.Endpoint,
			&regionStr, &provider.Bucket, &provider.AccessKey, &provider.SecretKey,
			&provider.UseSSL, &pathPrefixStr, &provider.IsDefault,
			&createdAtStr, &updatedAtStr)
	
	if err == sql.ErrNoRows {
		return nil, nil // No default provider
	}
	if err != nil {
		return nil, err
	}

	if regionStr.Valid {
		provider.Region = &regionStr.String
	}
	if pathPrefixStr.Valid {
		provider.PathPrefix = &pathPrefixStr.String
	}

	createdAt, err := common.ParseTime(createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing created_at: %v", err)
	}
	provider.CreatedAt = createdAt

	updatedAt, err := common.ParseTime(updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing updated_at: %v", err)
	}
	provider.UpdatedAt = updatedAt

	return provider, nil
}

