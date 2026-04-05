import axios, { AxiosError, AxiosInstance, AxiosRequestConfig, AxiosResponse } from "axios";
import { toast } from "sonner";
import { API_BASE_URL, API_ENDPOINTS } from "./constants";

const API_TIMEOUT = 30000;

let isRefreshing = false;
let failedQueue: Array<{ resolve: (token: string) => void; reject: (err: any) => void }> = [];

const processQueue = (error: any, token: string | null = null) => {
  failedQueue.forEach((prom) => {
    if (error) prom.reject(error);
    else prom.resolve(token!);
  });
  failedQueue = [];
};

export const apiClient: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  timeout: API_TIMEOUT,
  withCredentials: true,
  headers: { "Content-Type": "application/json" },
});

// Request interceptor
apiClient.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem("token");
    if (token) config.headers.Authorization = `Bearer ${token}`;
    return config;
  },
  (error) => Promise.reject(error)
);

apiClient.interceptors.response.use(
  (response: AxiosResponse) => response,
  async (error: AxiosError<{ code?: number; message?: string }>) => {
    const originalRequest: any = error.config;
      const silent = originalRequest?.headers?.['X-SILENT'] || originalRequest?.headers?.['x-silent'];
    const status = error.response?.status;
    const code = error.response?.data?.code;

    if (!status) {
      if (!silent) toast.error("Không thể kết nối đến máy chủ. Vui lòng thử lại.");
      return Promise.reject(error);
    }

    if (originalRequest.url?.includes("/auth/login") && status === 401) {
      if (!silent) toast.error(error.response?.data?.message || "Sai email hoặc mật khẩu");
      return Promise.reject(error);
    }

    if (status === 401 && code === 1012) {
      if (!originalRequest._retry) {
        originalRequest._retry = true;

        if (isRefreshing) {
          return new Promise<string>((resolve, reject) => {
            failedQueue.push({ resolve, reject });
          }).then((newToken) => {
            originalRequest.headers["Authorization"] = `Bearer ${newToken}`;
            return apiClient(originalRequest);
          });
        }

        isRefreshing = true;

        try {
          const { data } = await axios.post(
            `${API_BASE_URL}${API_ENDPOINTS.AUTH.REFRESH}`,
            {},
            { withCredentials: true }
          );

          const newToken = data.result?.token;
          if (!newToken) throw new Error("Refresh token hết hạn");

          localStorage.setItem("token", newToken);
          apiClient.defaults.headers.common["Authorization"] = `Bearer ${newToken}`;
          processQueue(null, newToken);

          originalRequest.headers["Authorization"] = `Bearer ${newToken}`;
          return apiClient(originalRequest);
        } catch (err: any) {
          processQueue(err, null);
          
          // Clear all auth data silently
          clearAuth();
          localStorage.removeItem("currentUser");
          
          if (!silent) {
            // Only show toast and redirect if not silent
            const isRefreshTokenExpired = err.response?.status === 401 || 
                                         err.response?.data?.code === 1012 ||
                                         err.message?.includes("hết hạn");
            
            if (isRefreshTokenExpired) {
              // Token expired - clear silently and redirect to login without error toast
              window.location.href = "/login";
            } else {
              // Other errors - show message
              toast.error(
                err.response?.data?.message || "Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại."
              );
              window.location.href = "/login";
            }
          }
          
          return Promise.reject(err);
        } finally {
          isRefreshing = false;
        }
      }
    }

    if (!silent) {
      if (error.response) {
        const { status, data } = error.response;
        switch (status) {
          case 403:
            toast.error("Bạn không có quyền truy cập tài nguyên này.");
            break;
          case 404:
            toast.error(data?.message || "Không tìm thấy tài nguyên.");
            break;
          case 422:
            toast.error(data?.message || "Dữ liệu không hợp lệ.");
            break;
          case 500:
          case 502:
          case 503:
            toast.error("Lỗi máy chủ. Vui lòng thử lại sau.");
            break;
          default:
            toast.error(data?.message || "Đã xảy ra lỗi. Vui lòng thử lại.");
        }
      } else if (error.request) {
        toast.error("Không thể kết nối đến máy chủ. Vui lòng thử lại.");
      } else {
        toast.error("Đã xảy ra lỗi không xác định.");
      }
    }

    return Promise.reject(error);
  }
);

export const setAuthToken = (token: string | null) => {
  if (token) {
    localStorage.setItem("token", token);
    apiClient.defaults.headers.common["Authorization"] = `Bearer ${token}`;
    try {
      window.dispatchEvent(new CustomEvent('auth:tokenChanged', { detail: { token } }));
    } catch (e) {
      // noop
    }
  } else {
    localStorage.removeItem("token");
    delete apiClient.defaults.headers.common["Authorization"];
    try {
      window.dispatchEvent(new CustomEvent('auth:tokenChanged', { detail: { token: null } }));
    } catch (e) {
      // noop
    }
  }
};

export const clearAuth = () => {
  localStorage.removeItem("token");
  localStorage.removeItem("user");
  localStorage.removeItem("currentUser");
  delete apiClient.defaults.headers.common["Authorization"];
};

export const getAuthToken = (): string | null => localStorage.getItem("token");

// API request wrapper
export const apiRequest = {
  get: <T = any>(url: string, config?: AxiosRequestConfig): Promise<T> =>
    apiClient.get<T>(url, config).then((res) => res.data),

  post: <T = any>(url: string, data?: any, config?: AxiosRequestConfig): Promise<T> =>
    apiClient.post<T>(url, data, config).then((res) => res.data),

  put: <T = any>(url: string, data?: any, config?: AxiosRequestConfig): Promise<T> =>
    apiClient.put<T>(url, data, config).then((res) => res.data),

  patch: <T = any>(url: string, data?: any, config?: AxiosRequestConfig): Promise<T> =>
    apiClient.patch<T>(url, data, config).then((res) => res.data),

  delete: <T = any>(url: string, config?: AxiosRequestConfig): Promise<T> =>
    apiClient.delete<T>(url, config).then((res) => res.data),
};
