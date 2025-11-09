import { BackupListResponse, BackupStatsResponse, BackupDiffResponse, Backup } from '@/types/backup';
import { apiRequest } from '../api-client';

export interface GetBackupsParams {
  page?: number;
  limit?: number;
  search?: string;
}

export interface RestoreBackupParams {
  backup_id: string;
  connection_id: string;
}

export async function saveBackup(connectionId: string, s3ProviderIds?: string[]): Promise<{ id: string }> {
  const response = await apiRequest<{ data: { id: string } }>('/api/backups', {
    method: 'POST',
    body: JSON.stringify({ 
      connection_id: connectionId,
      s3_provider_ids: s3ProviderIds || [],
    }),
  });
  return { id: response.data.id };
}

export async function getBackup(backupId: string): Promise<Backup> {
  const response = await apiRequest<{ data: Backup }>(`/api/backups/${backupId}`, {
    method: 'GET',
  });
  return response.data;
}

export async function getBackups(params?: GetBackupsParams): Promise<BackupListResponse> {
  let url = `/api/backups`;

  if (params?.page && params?.limit) {
    url += `?page=${params.page}&limit=${params.limit}`;
  }

  if (params?.search) {
    url += `&search=${params.search}`;
  }

  return apiRequest<BackupListResponse>(url, {
    method: 'GET',
  });
}

export async function downloadBackup(backupId: string, providerId?: string): Promise<Blob> {
  let url = `/api/backups/${backupId}/download`;
  if (providerId) {
    url += `?provider_id=${encodeURIComponent(providerId)}`;
  }
  console.log("downloadBackup called with:", { backupId, providerId, url });
  try {
    const blob = await apiRequest<Blob>(url, {
      method: 'GET',
      responseType: 'blob',
    });
    console.log("downloadBackup success, blob size:", blob?.size);
    return blob;
  } catch (error) {
    console.error("downloadBackup error:", error);
    throw error;
  }
}

export interface BackupS3Provider {
  provider_id: string;
  object_key: string;
}

export async function getBackupS3Providers(backupId: string): Promise<BackupS3Provider[]> {
  const response = await apiRequest<{ data: BackupS3Provider[] }>(`/api/backups/${backupId}/s3-providers`, {
    method: 'GET',
  });
  return response.data || [];
}

export interface ShareableLink {
  token: string;
  expires_at: string;
  url: string;
}

export async function createShareableLink(backupId: string, providerId?: string, expiresIn?: number): Promise<ShareableLink> {
  const response = await apiRequest<{ data: ShareableLink }>(`/api/backups/${backupId}/share`, {
    method: 'POST',
    body: JSON.stringify({
      provider_id: providerId,
      expires_in: expiresIn || 24, // Default 24 hours
    }),
  });
  return response.data;
}

export interface ScheduleBackupParams {
  connection_id: string;
  cron_schedule: string;
  retention_days: number;
}

export async function scheduleBackup(params: ScheduleBackupParams): Promise<void> {
  return apiRequest('/api/backups/schedule', {
    method: 'POST',
    body: JSON.stringify(params),
  });
}

export async function updateSchedule(connectionId: string, params: Omit<ScheduleBackupParams, 'connection_id'>): Promise<void> {
  return apiRequest(`/api/backups/${connectionId}/schedule`, {
    method: 'PUT',
    body: JSON.stringify(params),
  });
}

export async function disableBackupSchedule(connectionId: string): Promise<void> {
  return apiRequest(`/api/backups/${connectionId}/schedule/disable`, {
    method: 'POST',
  });
}

export async function getBackupStats(): Promise<BackupStatsResponse> {
  return apiRequest<BackupStatsResponse>('/api/backups/stats', {
    method: 'GET',
  });
}

export async function getActiveBackups(): Promise<BackupListResponse> {
  return apiRequest<BackupListResponse>('/api/backups/active', {
    method: 'GET',
  });
}

export async function compareBackups(sourceId: string, targetId: string): Promise<BackupDiffResponse> {
  return apiRequest<BackupDiffResponse>(`/api/backups/compare/${sourceId}/${targetId}`, {
    method: 'GET',
  });
}

export async function restoreBackup(params: RestoreBackupParams): Promise<void> {
  return apiRequest('/api/backups/restore', {
    method: 'POST',
    body: JSON.stringify(params),
  });
}

export async function getBackupLogs(backupId: string): Promise<string> {
  const response = await apiRequest<{ data: { logs: string } }>(`/api/backups/${backupId}/logs/stored`, {
    method: 'GET',
  });
  return response.data.logs || '';
}

export function streamBackupLogs(backupId: string, onLog: (log: string) => void, onError?: (error: Error) => void, onClose?: () => void): () => void {
  let abortController: AbortController | null = null;
  let isClosed = false;

  const connect = async () => {
    try {
      const apiUrl = await fetch('/api/config').then(res => res.json()).then(config => config.apiUrl);
      const token = localStorage.getItem('token');
      
      const url = `${apiUrl}/api/backups/${backupId}/logs`;
      
      abortController = new AbortController();
      
      const response = await fetch(url, {
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
        signal: abortController.signal,
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const reader = response.body?.getReader();
      const decoder = new TextDecoder();

      if (!reader) {
        throw new Error('No response body');
      }

      const readStream = async () => {
        try {
          while (!isClosed) {
            const { done, value } = await reader.read();
            
            if (done) {
              if (!isClosed) {
                onClose?.();
              }
              break;
            }

            const chunk = decoder.decode(value, { stream: true });
            const lines = chunk.split('\n');

            for (const line of lines) {
              if (line.startsWith('data: ')) {
                const data = line.slice(6); // Remove 'data: ' prefix
                if (!isClosed) {
                  onLog(data);
                }
              }
            }
          }
        } catch (error) {
          if (!isClosed) {
            if (error instanceof Error && error.name === 'AbortError') {
              onClose?.();
            } else {
              onError?.(error instanceof Error ? error : new Error('Stream read error'));
            }
          }
        }
      };

      readStream();
    } catch (error) {
      if (!isClosed) {
        onError?.(error instanceof Error ? error : new Error('Failed to connect to log stream'));
      }
    }
  };

  connect();

  return () => {
    isClosed = true;
    if (abortController) {
      abortController.abort();
      abortController = null;
    }
  };
}