package backup

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// sendTelegramNotification sends a notification to Telegram
func (s *BackupService) sendTelegramNotification(botToken string, chatID string, message string) error {
	if botToken == "" || chatID == "" {
		return fmt.Errorf("telegram bot token and chat ID are required")
	}

	// Decrypt bot token if needed
	decryptedToken, err := s.cryptoService.Decrypt(botToken)
	if err != nil {
		// If decryption fails, try using the token as-is (might be plain text)
		decryptedToken = botToken
	}

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", decryptedToken)

	payload := map[string]interface{}{
		"chat_id":    chatID,
		"text":       message,
		"parse_mode": "HTML",
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal telegram payload: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create telegram request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send telegram message: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram API returned status code: %d", resp.StatusCode)
	}

	return nil
}

// formatTelegramMessage formats a backup notification message for Telegram
func formatTelegramMessage(title string, databaseName string, databaseType string, status string, details map[string]interface{}) string {
	var emoji string
	var statusText string

	switch status {
	case "success":
		emoji = "✅"
		statusText = "SUCCESS"
	case "failed":
		emoji = "❌"
		statusText = "FAILED"
	default:
		emoji = "ℹ️"
		statusText = status
	}

	message := fmt.Sprintf("<b>%s %s</b>\n\n", emoji, title)
	message += fmt.Sprintf("<b>Database:</b> %s\n", databaseName)
	message += fmt.Sprintf("<b>Type:</b> %s\n", databaseType)
	message += fmt.Sprintf("<b>Status:</b> %s\n", statusText)

	if size, ok := details["size"].(int64); ok && size > 0 {
		message += fmt.Sprintf("<b>Size:</b> %s\n", formatBytes(size))
	}

	if duration, ok := details["duration"].(string); ok && duration != "" {
		message += fmt.Sprintf("<b>Duration:</b> %s\n", duration)
	}

	if errorMsg, ok := details["error"].(string); ok && errorMsg != "" {
		message += fmt.Sprintf("\n<b>Error:</b>\n<code>%s</code>\n", errorMsg)
	}

	if timestamp, ok := details["timestamp"].(string); ok {
		message += fmt.Sprintf("\n<i>%s</i>", timestamp)
	} else {
		message += fmt.Sprintf("\n<i>%s</i>", time.Now().Format("2006-01-02 15:04:05"))
	}

	return message
}

// formatBytes formats bytes to human-readable format
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

