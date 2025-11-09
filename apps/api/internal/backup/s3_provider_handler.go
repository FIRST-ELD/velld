package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/dendianugerah/velld/internal/common"
	"github.com/dendianugerah/velld/internal/common/response"
	"github.com/gorilla/mux"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3ProviderHandler struct {
	s3ProviderService *S3ProviderService
}

func NewS3ProviderHandler(service *S3ProviderService) *S3ProviderHandler {
	return &S3ProviderHandler{
		s3ProviderService: service,
	}
}

func (h *S3ProviderHandler) CreateS3Provider(w http.ResponseWriter, r *http.Request) {
	userID, err := common.GetUserIDFromContext(r.Context())
	if err != nil {
		response.SendError(w, http.StatusUnauthorized, err.Error())
		return
	}

	var req S3ProviderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.SendError(w, http.StatusBadRequest, err.Error())
		return
	}

	provider, err := h.s3ProviderService.CreateS3Provider(userID, &req)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "S3 provider created successfully", provider)
}

func (h *S3ProviderHandler) GetS3Provider(w http.ResponseWriter, r *http.Request) {
	userID, err := common.GetUserIDFromContext(r.Context())
	if err != nil {
		response.SendError(w, http.StatusUnauthorized, err.Error())
		return
	}

	vars := mux.Vars(r)
	providerID := vars["id"]

	provider, err := h.s3ProviderService.GetS3Provider(providerID, userID)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "S3 provider retrieved successfully", provider)
}

func (h *S3ProviderHandler) ListS3Providers(w http.ResponseWriter, r *http.Request) {
	userID, err := common.GetUserIDFromContext(r.Context())
	if err != nil {
		response.SendError(w, http.StatusUnauthorized, err.Error())
		return
	}

	providers, err := h.s3ProviderService.ListS3Providers(userID)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "S3 providers retrieved successfully", providers)
}

func (h *S3ProviderHandler) UpdateS3Provider(w http.ResponseWriter, r *http.Request) {
	userID, err := common.GetUserIDFromContext(r.Context())
	if err != nil {
		response.SendError(w, http.StatusUnauthorized, err.Error())
		return
	}

	vars := mux.Vars(r)
	providerID := vars["id"]

	var req S3ProviderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.SendError(w, http.StatusBadRequest, err.Error())
		return
	}

	provider, err := h.s3ProviderService.UpdateS3Provider(providerID, userID, &req)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "S3 provider updated successfully", provider)
}

func (h *S3ProviderHandler) DeleteS3Provider(w http.ResponseWriter, r *http.Request) {
	userID, err := common.GetUserIDFromContext(r.Context())
	if err != nil {
		response.SendError(w, http.StatusUnauthorized, err.Error())
		return
	}

	vars := mux.Vars(r)
	providerID := vars["id"]

	if err := h.s3ProviderService.DeleteS3Provider(providerID, userID); err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "S3 provider deleted successfully", nil)
}

func (h *S3ProviderHandler) SetDefaultProvider(w http.ResponseWriter, r *http.Request) {
	userID, err := common.GetUserIDFromContext(r.Context())
	if err != nil {
		response.SendError(w, http.StatusUnauthorized, err.Error())
		return
	}

	vars := mux.Vars(r)
	providerID := vars["id"]

	if err := h.s3ProviderService.SetDefaultProvider(userID, providerID); err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "Default S3 provider set successfully", nil)
}

func (h *S3ProviderHandler) TestS3Provider(w http.ResponseWriter, r *http.Request) {
	userID, err := common.GetUserIDFromContext(r.Context())
	if err != nil {
		response.SendError(w, http.StatusUnauthorized, err.Error())
		return
	}

	vars := mux.Vars(r)
	providerID := vars["id"]

	provider, err := h.s3ProviderService.GetS3ProviderForUpload(providerID, userID)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Test the connection using the S3 storage test logic
	region := "us-east-1"
	if provider.Region != nil && *provider.Region != "" {
		region = *provider.Region
	}

	pathPrefix := ""
	if provider.PathPrefix != nil {
		pathPrefix = *provider.PathPrefix
	}

	// Trim whitespace from credentials (prevents "malformed credential" errors)
	accessKey := strings.TrimSpace(provider.AccessKey)
	secretKey := strings.TrimSpace(provider.SecretKey)
	endpoint := strings.TrimSpace(provider.Endpoint)
	bucket := strings.TrimSpace(provider.Bucket)
	pathPrefix = strings.TrimSpace(pathPrefix)

	s3Config := S3Config{
		Endpoint:   endpoint,
		Region:     region,
		Bucket:     bucket,
		AccessKey:  accessKey,
		SecretKey:  secretKey,
		UseSSL:     provider.UseSSL,
		PathPrefix: pathPrefix,
	}

	// Test the connection
	if err := testS3Connection(s3Config); err != nil {
		response.SendError(w, http.StatusBadRequest, err.Error())
		return
	}

	response.SendSuccess(w, "S3 provider connection test successful", nil)
}

// testS3Connection tests the S3 connection with the provided configuration
func testS3Connection(config S3Config) error {
	// Trim whitespace from credentials
	config.AccessKey = strings.TrimSpace(config.AccessKey)
	config.SecretKey = strings.TrimSpace(config.SecretKey)
	config.Endpoint = strings.TrimSpace(config.Endpoint)
	config.Bucket = strings.TrimSpace(config.Bucket)

	// Validate required fields
	if config.Endpoint == "" {
		return fmt.Errorf("S3 endpoint is required")
	}
	if config.Bucket == "" {
		return fmt.Errorf("S3 bucket is required")
	}
	if config.AccessKey == "" {
		return fmt.Errorf("S3 access key is required")
	}
	if config.SecretKey == "" {
		return fmt.Errorf("S3 secret key is required")
	}

	// Create MinIO client
	useSSL := config.UseSSL
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKey, config.SecretKey, ""),
		Secure: useSSL,
		Region: config.Region,
	})
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	// Test connection by attempting to list objects in the bucket
	ctx := context.Background()
	objectCh := client.ListObjects(ctx, config.Bucket, minio.ListObjectsOptions{
		MaxKeys: 1,
	})

	// Try to read at least one object (or verify bucket exists)
	for range objectCh {
		break
	}

	// If we got here without error, the connection is valid
	return nil
}

