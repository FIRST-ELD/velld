import { apiRequest } from "@/lib/api-client";
import { UpdateSettingsRequest, GetSettingsResponse, UpdateSettingsResponse } from "@/types/settings";

export async function getUserSettings(): Promise<GetSettingsResponse> {
  return apiRequest<GetSettingsResponse>('/api/settings');
}

export async function updateUserSettings(settings: UpdateSettingsRequest): Promise<UpdateSettingsResponse> {
  return apiRequest<UpdateSettingsResponse>('/api/settings', {
    method: 'PUT',
    body: JSON.stringify(settings),
  });
}

export interface TestS3ConnectionRequest {
  s3_endpoint: string;
  s3_region: string;
  s3_bucket: string;
  s3_access_key: string;
  s3_secret_key: string;
  s3_use_ssl: boolean;
  s3_path_prefix: string;
}

export async function testS3Connection(params: TestS3ConnectionRequest): Promise<void> {
  return apiRequest('/api/settings/test-s3', {
    method: 'POST',
    body: JSON.stringify(params),
  });
}

export interface TestTelegramConnectionRequest {
  telegram_bot_token: string;
  telegram_chat_id: string;
}

export interface TelegramChatInfo {
  id: string;
  type: string;
  title?: string;
  username?: string;
  first_name?: string;
  last_name?: string;
}

export interface TelegramChat {
  id: string;
  type: string;
  title?: string;
  username?: string;
  first_name?: string;
  last_name?: string;
}

export async function testTelegramConnection(params: TestTelegramConnectionRequest): Promise<{ data: TelegramChatInfo }> {
  return apiRequest<{ data: TelegramChatInfo }>('/api/settings/test-telegram', {
    method: 'POST',
    body: JSON.stringify(params),
  });
}

export async function getTelegramChats(botToken: string): Promise<{ data: TelegramChat[] }> {
  return apiRequest<{ data: TelegramChat[] }>(`/api/settings/telegram-chats?bot_token=${encodeURIComponent(botToken)}`);
}
