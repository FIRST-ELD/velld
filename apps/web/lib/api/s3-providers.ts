import { apiRequest } from "@/lib/api-client";

export interface S3Provider {
  id: string;
  user_id: string;
  name: string;
  endpoint: string;
  region?: string;
  bucket: string;
  use_ssl: boolean;
  path_prefix?: string;
  is_default: boolean;
  created_at: string;
  updated_at: string;
}

export interface S3ProviderRequest {
  name: string;
  endpoint: string;
  region?: string;
  bucket: string;
  access_key: string;
  secret_key: string;
  use_ssl?: boolean;
  path_prefix?: string;
  is_default?: boolean;
}

export async function listS3Providers(): Promise<S3Provider[]> {
  const response = await apiRequest<{ data: S3Provider[] }>('/api/s3-providers');
  return response.data || [];
}

export async function getS3Provider(id: string): Promise<S3Provider> {
  const response = await apiRequest<{ data: S3Provider }>(`/api/s3-providers/${id}`);
  return response.data;
}

export async function createS3Provider(provider: S3ProviderRequest): Promise<S3Provider> {
  const response = await apiRequest<{ data: S3Provider }>('/api/s3-providers', {
    method: 'POST',
    body: JSON.stringify(provider),
  });
  return response.data;
}

export async function updateS3Provider(id: string, provider: S3ProviderRequest): Promise<S3Provider> {
  const response = await apiRequest<{ data: S3Provider }>(`/api/s3-providers/${id}`, {
    method: 'PUT',
    body: JSON.stringify(provider),
  });
  return response.data;
}

export async function deleteS3Provider(id: string): Promise<void> {
  await apiRequest(`/api/s3-providers/${id}`, {
    method: 'DELETE',
  });
}

export async function setDefaultS3Provider(id: string): Promise<void> {
  await apiRequest(`/api/s3-providers/${id}/set-default`, {
    method: 'POST',
  });
}

export async function testS3Provider(id: string): Promise<void> {
  await apiRequest(`/api/s3-providers/${id}/test`, {
    method: 'POST',
  });
}

