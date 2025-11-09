let API_BASE: string | null = null;

async function getConfig() {
  if (!API_BASE) {
    const { apiUrl } = await fetch('/api/config').then(res => res.json());
    API_BASE = apiUrl;
  }
  return API_BASE;
}

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
    this.name = 'ApiError';
  }
}

interface ExtendedRequestInit extends RequestInit {
  responseType?: 'json' | 'blob';
}

export async function apiRequest<T>(
  endpoint: string,
  options: ExtendedRequestInit = {}
): Promise<T> {
  const apiUrl = await getConfig();
  const token = localStorage.getItem('token');
  
  // Don't set Content-Type for blob requests or if it's already set in options
  const isBlobRequest = options.responseType === 'blob';
  const headers: HeadersInit = {
    ...(!isBlobRequest && { 'Content-Type': 'application/json' }),
    ...(token && { Authorization: `Bearer ${token}` }),
    ...options.headers,
  };

  const response = await fetch(`${apiUrl}${endpoint}`, {
    ...options,
    headers,
  });

  if (!response.ok) {
    if (response.status === 401) {
      localStorage.removeItem('token');
      window.location.href = '/login';
    }
    
    // Try to parse JSON error response
    let errorMessage = `HTTP error! status: ${response.status}`;
    try {
      // For blob requests, we need to clone the response to read it
      const clonedResponse = response.clone();
      const errorText = await clonedResponse.text();
      if (errorText) {
        // Try to parse as JSON
        try {
          const parsed = JSON.parse(errorText);
          errorMessage = parsed.message || errorText;
        } catch {
          // If not JSON, use the text as-is
          errorMessage = errorText;
        }
      }
    } catch (e) {
      // If parsing fails, use default message
      console.error("Error parsing error response:", e);
    }
    
    throw new ApiError(response.status, errorMessage);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  if (options.responseType === 'blob') {
    return response.blob() as T;
  }

  return response.json();
}
