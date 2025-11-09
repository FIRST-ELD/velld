package settings

import (
	"encoding/json"
	"net/http"

	"github.com/dendianugerah/velld/internal/common"
	"github.com/dendianugerah/velld/internal/common/response"
)

type SettingsHandler struct {
	service *SettingsService
}

func NewSettingsHandler(service *SettingsService) *SettingsHandler {
	return &SettingsHandler{service: service}
}

func (h *SettingsHandler) GetSettings(w http.ResponseWriter, r *http.Request) {
	userID, err := common.GetUserIDFromContext(r.Context())
	if err != nil {
		response.SendError(w, http.StatusUnauthorized, err.Error())
		return
	}

	settings, err := h.service.GetUserSettings(userID)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "Settings retrieved successfully", settings)
}

func (h *SettingsHandler) UpdateSettings(w http.ResponseWriter, r *http.Request) {
	userID, err := common.GetUserIDFromContext(r.Context())
	if err != nil {
		response.SendError(w, http.StatusUnauthorized, err.Error())
		return
	}

	var req UpdateSettingsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.SendError(w, http.StatusBadRequest, err.Error())
		return
	}

	settings, err := h.service.UpdateUserSettings(userID, &req)
	if err != nil {
		response.SendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.SendSuccess(w, "Settings updated successfully", settings)
}

func (h *SettingsHandler) TestS3Connection(w http.ResponseWriter, r *http.Request) {
	var req TestS3ConnectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.SendError(w, http.StatusBadRequest, err.Error())
		return
	}

	err := h.service.TestS3Connection(&req)
	if err != nil {
		response.SendError(w, http.StatusBadRequest, err.Error())
		return
	}

	response.SendSuccess(w, "S3 connection test successful", nil)
}

func (h *SettingsHandler) TestTelegramConnection(w http.ResponseWriter, r *http.Request) {
	var req TestTelegramConnectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.SendError(w, http.StatusBadRequest, err.Error())
		return
	}

	chatInfo, err := TestTelegramConnection(req.BotToken, req.ChatID, h.service.GetCryptoService())
	if err != nil {
		response.SendError(w, http.StatusBadRequest, err.Error())
		return
	}

	response.SendSuccess(w, "Telegram connection test successful", chatInfo)
}

func (h *SettingsHandler) GetTelegramChats(w http.ResponseWriter, r *http.Request) {
	botToken := r.URL.Query().Get("bot_token")
	if botToken == "" {
		response.SendError(w, http.StatusBadRequest, "bot_token query parameter is required")
		return
	}

	chats, err := GetTelegramChats(botToken, h.service.GetCryptoService())
	if err != nil {
		response.SendError(w, http.StatusBadRequest, err.Error())
		return
	}

	response.SendSuccess(w, "Telegram chats retrieved successfully", chats)
}
