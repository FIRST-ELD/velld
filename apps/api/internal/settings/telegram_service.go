package settings

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// TelegramChat represents a Telegram chat
type TelegramChat struct {
	ID       string `json:"id"`
	Type     string `json:"type"` // "private", "group", "supergroup", "channel"
	Title    string `json:"title,omitempty"`
	Username string `json:"username,omitempty"`
	FirstName string `json:"first_name,omitempty"`
	LastName  string `json:"last_name,omitempty"`
}

// TelegramChatInfo represents detailed chat information
type TelegramChatInfo struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Title    string `json:"title,omitempty"`
	Username string `json:"username,omitempty"`
	FirstName string `json:"first_name,omitempty"`
	LastName  string `json:"last_name,omitempty"`
}

// TelegramAPIResponse represents a generic Telegram API response
type TelegramAPIResponse struct {
	OK     bool                   `json:"ok"`
	Result interface{}            `json:"result,omitempty"`
	ErrorCode int                 `json:"error_code,omitempty"`
	Description string            `json:"description,omitempty"`
}

// TestTelegramConnection tests the Telegram bot token and chat ID
func TestTelegramConnection(botToken string, chatID string, cryptoService interface{}) (*TelegramChatInfo, error) {
	if botToken == "" {
		return nil, fmt.Errorf("bot token is required")
	}
	if chatID == "" {
		return nil, fmt.Errorf("chat ID is required")
	}

	// Decrypt bot token if needed
	decryptedToken := botToken
	if cryptoService != nil {
		if decryptFunc, ok := cryptoService.(interface{ Decrypt(string) (string, error) }); ok {
			if decrypted, err := decryptFunc.Decrypt(botToken); err == nil {
				decryptedToken = decrypted
			}
		}
	}

	// First, test bot token by getting bot info
	botInfo, err := getBotInfo(decryptedToken)
	if err != nil {
		return nil, fmt.Errorf("invalid bot token: %w", err)
	}

	// Then, get chat info
	chatInfo, err := getChatInfo(decryptedToken, chatID)
	if err != nil {
		return nil, fmt.Errorf("invalid chat ID or bot doesn't have access: %w", err)
	}

	// Add bot username to response for confirmation
	_ = botInfo

	return chatInfo, nil
}

// GetTelegramChats gets recent chats the bot has interacted with
func GetTelegramChats(botToken string, cryptoService interface{}) ([]TelegramChat, error) {
	if botToken == "" {
		return nil, fmt.Errorf("bot token is required")
	}

	// Decrypt bot token if needed
	decryptedToken := botToken
	if cryptoService != nil {
		if decryptFunc, ok := cryptoService.(interface{ Decrypt(string) (string, error) }); ok {
			if decrypted, err := decryptFunc.Decrypt(botToken); err == nil {
				decryptedToken = decrypted
			}
		}
	}

	// Get recent updates to find chats
	chats, err := getRecentChats(decryptedToken)
	if err != nil {
		return nil, fmt.Errorf("failed to get chats: %w", err)
	}

	return chats, nil
}

// getBotInfo gets information about the bot
func getBotInfo(botToken string) (map[string]interface{}, error) {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/getMe", botToken)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var apiResp TelegramAPIResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, err
	}

	if !apiResp.OK {
		return nil, fmt.Errorf("telegram API error: %s", apiResp.Description)
	}

	botInfo, ok := apiResp.Result.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response format")
	}

	return botInfo, nil
}

// getChatInfo gets information about a specific chat
func getChatInfo(botToken string, chatID string) (*TelegramChatInfo, error) {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/getChat", botToken)

	payload := map[string]string{
		"chat_id": chatID,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var apiResp TelegramAPIResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, err
	}

	if !apiResp.OK {
		return nil, fmt.Errorf("telegram API error: %s", apiResp.Description)
	}

	chatData, ok := apiResp.Result.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response format")
	}

	chatInfo := &TelegramChatInfo{}

	if id, ok := chatData["id"].(float64); ok {
		chatInfo.ID = fmt.Sprintf("%.0f", id)
	}

	if chatType, ok := chatData["type"].(string); ok {
		chatInfo.Type = chatType
	}

	if title, ok := chatData["title"].(string); ok {
		chatInfo.Title = title
	}

	if username, ok := chatData["username"].(string); ok {
		chatInfo.Username = username
	}

	if firstName, ok := chatData["first_name"].(string); ok {
		chatInfo.FirstName = firstName
	}

	if lastName, ok := chatData["last_name"].(string); ok {
		chatInfo.LastName = lastName
	}

	return chatInfo, nil
}

// getRecentChats gets recent chats from bot updates
func getRecentChats(botToken string) ([]TelegramChat, error) {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/getUpdates?limit=100", botToken)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var apiResp TelegramAPIResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, err
	}

	if !apiResp.OK {
		return nil, fmt.Errorf("telegram API error: %s", apiResp.Description)
	}

	updates, ok := apiResp.Result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response format")
	}

	chatMap := make(map[string]*TelegramChat)

	for _, updateRaw := range updates {
		update, ok := updateRaw.(map[string]interface{})
		if !ok {
			continue
		}

		// Extract chat from message
		if message, ok := update["message"].(map[string]interface{}); ok {
			if chat, ok := message["chat"].(map[string]interface{}); ok {
				processChat(chat, chatMap)
			}
		}

		// Extract chat from channel_post (for channels)
		if channelPost, ok := update["channel_post"].(map[string]interface{}); ok {
			if chat, ok := channelPost["chat"].(map[string]interface{}); ok {
				processChat(chat, chatMap)
			}
		}
	}

	chats := make([]TelegramChat, 0, len(chatMap))
	for _, chat := range chatMap {
		chats = append(chats, *chat)
	}

	return chats, nil
}

func processChat(chat map[string]interface{}, chatMap map[string]*TelegramChat) {
	var chatID string
	if id, ok := chat["id"].(float64); ok {
		chatID = fmt.Sprintf("%.0f", id)
	} else {
		return
	}

	// Skip if already processed
	if _, exists := chatMap[chatID]; exists {
		return
	}

	tgChat := &TelegramChat{
		ID: chatID,
	}

	if chatType, ok := chat["type"].(string); ok {
		tgChat.Type = chatType
	}

	if title, ok := chat["title"].(string); ok {
		tgChat.Title = title
	}

	if username, ok := chat["username"].(string); ok {
		tgChat.Username = username
	}

	if firstName, ok := chat["first_name"].(string); ok {
		tgChat.FirstName = firstName
	}

	if lastName, ok := chat["last_name"].(string); ok {
		tgChat.LastName = lastName
	}

	chatMap[chatID] = tgChat
}

