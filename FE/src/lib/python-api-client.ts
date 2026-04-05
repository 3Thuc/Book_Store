import axios, { AxiosInstance, AxiosRequestConfig } from "axios";

// Python Backend URL - running on port 8000
export const PYTHON_API_BASE_URL = 'http://127.0.0.1:8000';

const API_TIMEOUT = 30000;

export const pythonApiClient: AxiosInstance = axios.create({
  baseURL: PYTHON_API_BASE_URL,
  timeout: API_TIMEOUT,
  headers: { "Content-Type": "application/json" },
});

// Request interceptor
pythonApiClient.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem("token");
    if (token) config.headers.Authorization = `Bearer ${token}`;
    return config;
  },
  (error) => Promise.reject(error)
);

// Response interceptor
pythonApiClient.interceptors.response.use(
  (response) => response,
  async (error) => {
    if (!error.response) {
      console.error("Không thể kết nối đến Python backend");
      return Promise.reject(error);
    }
    return Promise.reject(error);
  }
);

// API request wrapper for Python backend
export const pythonApiRequest = {
  get: <T = any>(url: string, config?: AxiosRequestConfig): Promise<T> =>
    pythonApiClient.get<T>(url, config).then((res) => res.data),

  post: <T = any>(url: string, data?: any, config?: AxiosRequestConfig): Promise<T> =>
    pythonApiClient.post<T>(url, data, config).then((res) => res.data),
};
