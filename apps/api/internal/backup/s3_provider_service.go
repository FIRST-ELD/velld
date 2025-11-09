package backup

import (
	"fmt"
	"strings"

	"github.com/dendianugerah/velld/internal/common"
	"github.com/google/uuid"
)

type S3ProviderService struct {
	repo          *S3ProviderRepository
	cryptoService *common.EncryptionService
}

func NewS3ProviderService(repo *S3ProviderRepository, cryptoService *common.EncryptionService) *S3ProviderService {
	return &S3ProviderService{
		repo:          repo,
		cryptoService: cryptoService,
	}
}

func (s *S3ProviderService) CreateS3Provider(userID uuid.UUID, req *S3ProviderRequest) (*S3Provider, error) {
	// Aggressively clean credentials before storing (prevents "malformed credential" errors)
	// Use the same cleaning function used when retrieving credentials
	cleanedSecretKey := cleanS3Credential(req.SecretKey)
	cleanedAccessKey := cleanS3Credential(req.AccessKey)
	cleanedEndpoint := strings.TrimSpace(req.Endpoint) // Endpoint can have spaces in domain names
	cleanedBucket := cleanS3Credential(req.Bucket)
	
	// Encrypt secret key
	encryptedSecretKey, err := s.cryptoService.Encrypt(cleanedSecretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt secret key: %w", err)
	}

	useSSL := true
	if req.UseSSL != nil {
		useSSL = *req.UseSSL
	}

	isDefault := false
	if req.IsDefault != nil {
		isDefault = *req.IsDefault
	}

	// If this is set as default, unset other defaults
	if isDefault {
		if err := s.repo.SetDefaultProvider(userID, ""); err != nil {
			return nil, fmt.Errorf("failed to unset existing default: %w", err)
		}
	}

	provider := &S3Provider{
		ID:        uuid.New(),
		UserID:    userID,
		Name:      strings.TrimSpace(req.Name),
		Endpoint:  cleanedEndpoint,
		Region:    req.Region, // Region can be nil, so we'll trim if not nil
		Bucket:    cleanedBucket,
		AccessKey: cleanedAccessKey,
		SecretKey: encryptedSecretKey,
		UseSSL:    useSSL,
		PathPrefix: req.PathPrefix, // PathPrefix can be nil, so we'll trim if not nil
		IsDefault: isDefault,
	}
	
	// Trim region and path prefix if they're not nil
	if provider.Region != nil {
		trimmedRegion := strings.TrimSpace(*provider.Region)
		provider.Region = &trimmedRegion
	}
	if provider.PathPrefix != nil {
		trimmedPathPrefix := strings.TrimSpace(*provider.PathPrefix)
		provider.PathPrefix = &trimmedPathPrefix
	}

	if err := s.repo.CreateS3Provider(provider); err != nil {
		return nil, err
	}

	// Clear sensitive data before returning
	provider.AccessKey = ""
	provider.SecretKey = ""

	return provider, nil
}

func (s *S3ProviderService) GetS3Provider(id string, userID uuid.UUID) (*S3Provider, error) {
	provider, err := s.repo.GetS3Provider(id, userID)
	if err != nil {
		return nil, err
	}

	// Clear sensitive data before returning
	provider.AccessKey = ""
	provider.SecretKey = ""

	return provider, nil
}

func (s *S3ProviderService) ListS3Providers(userID uuid.UUID) ([]*S3Provider, error) {
	providers, err := s.repo.ListS3Providers(userID)
	if err != nil {
		return nil, err
	}

	// Clear sensitive data before returning
	for _, provider := range providers {
		provider.AccessKey = ""
		provider.SecretKey = ""
	}

	return providers, nil
}

func (s *S3ProviderService) UpdateS3Provider(id string, userID uuid.UUID, req *S3ProviderRequest) (*S3Provider, error) {
	existing, err := s.repo.GetS3Provider(id, userID)
	if err != nil {
		return nil, err
	}

	// Aggressively clean credentials before storing (prevents "malformed credential" errors)
	// Use the same cleaning function used when retrieving credentials
	cleanedAccessKey := cleanS3Credential(req.AccessKey)
	cleanedEndpoint := strings.TrimSpace(req.Endpoint) // Endpoint can have spaces in domain names
	cleanedBucket := cleanS3Credential(req.Bucket)

	// Update fields
	existing.Name = strings.TrimSpace(req.Name)
	existing.Endpoint = cleanedEndpoint
	existing.Bucket = cleanedBucket
	existing.AccessKey = cleanedAccessKey
	
	// Trim region if provided
	if req.Region != nil {
		trimmedRegion := strings.TrimSpace(*req.Region)
		existing.Region = &trimmedRegion
	} else {
		existing.Region = req.Region
	}
	
	// Trim path prefix if provided
	if req.PathPrefix != nil {
		trimmedPathPrefix := strings.TrimSpace(*req.PathPrefix)
		existing.PathPrefix = &trimmedPathPrefix
	} else {
		existing.PathPrefix = req.PathPrefix
	}

	// Encrypt new secret key if provided
	if req.SecretKey != "" {
		cleanedSecretKey := cleanS3Credential(req.SecretKey)
		encryptedSecretKey, err := s.cryptoService.Encrypt(cleanedSecretKey)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt secret key: %w", err)
		}
		existing.SecretKey = encryptedSecretKey
	}

	if req.UseSSL != nil {
		existing.UseSSL = *req.UseSSL
	}
	if req.PathPrefix != nil {
		existing.PathPrefix = req.PathPrefix
	}

	// Handle default flag
	if req.IsDefault != nil {
		isDefault := *req.IsDefault
		if isDefault && !existing.IsDefault {
			// Unset other defaults
			if err := s.repo.SetDefaultProvider(userID, ""); err != nil {
				return nil, fmt.Errorf("failed to unset existing default: %w", err)
			}
		}
		existing.IsDefault = isDefault
	}

	if err := s.repo.UpdateS3Provider(existing); err != nil {
		return nil, err
	}

	// Clear sensitive data before returning
	existing.AccessKey = ""
	existing.SecretKey = ""

	return existing, nil
}

func (s *S3ProviderService) DeleteS3Provider(id string, userID uuid.UUID) error {
	return s.repo.DeleteS3Provider(id, userID)
}

func (s *S3ProviderService) SetDefaultProvider(userID uuid.UUID, providerID string) error {
	return s.repo.SetDefaultProvider(userID, providerID)
}

func (s *S3ProviderService) GetDefaultProvider(userID uuid.UUID) (*S3Provider, error) {
	provider, err := s.repo.GetDefaultProvider(userID)
	if err != nil {
		return nil, err
	}

	if provider == nil {
		return nil, nil
	}

	// Clear sensitive data before returning
	provider.AccessKey = ""
	provider.SecretKey = ""

	return provider, nil
}

// GetS3ProviderForUpload returns the provider with decrypted credentials for upload
func (s *S3ProviderService) GetS3ProviderForUpload(id string, userID uuid.UUID) (*S3Provider, error) {
	provider, err := s.repo.GetS3Provider(id, userID)
	if err != nil {
		return nil, err
	}

	// Decrypt secret key
	decryptedSecretKey, err := s.cryptoService.Decrypt(provider.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt secret key: %w", err)
	}
	
	// Aggressively clean all credentials after decryption (prevents "malformed credential" errors)
	// This removes all whitespace, control characters, and invisible Unicode characters
	provider.AccessKey = cleanS3Credential(provider.AccessKey)
	provider.SecretKey = cleanS3Credential(decryptedSecretKey)
	provider.Endpoint = strings.TrimSpace(provider.Endpoint) // Endpoint can have spaces in domain names
	provider.Bucket = cleanS3Credential(provider.Bucket)

	return provider, nil
}

// GetS3ProviderForDownload returns the provider with decrypted credentials for download
// This is the same as GetS3ProviderForUpload but with a clearer name for download operations
func (s *S3ProviderService) GetS3ProviderForDownload(id string, userID uuid.UUID) (*S3Provider, error) {
	return s.GetS3ProviderForUpload(id, userID)
}

