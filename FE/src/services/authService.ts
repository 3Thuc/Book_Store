/**
 * Auth Service
 * API calls related to authentication
 */

import { apiRequest, setAuthToken, clearAuth } from '../lib/api-client';
import { API_ENDPOINTS } from '../lib/constants';
import {
  ApiResponse,
  LoginRequest,
  LoginResponse,
  LogoutResponse,
  RegisterRequest,
  RegisterResponse,
  GoogleLoginRequest,
  GoogleLoginResponse,
} from '../types/api';
import { User } from '../types/user';

export class AuthService {

  static async login(credentials: LoginRequest): Promise<LoginResponse> {
    const response = await apiRequest.post<ApiResponse<LoginResponse>>(
      API_ENDPOINTS.AUTH.LOGIN,
      credentials
    );

    if (response.result.token) {
      setAuthToken(response.result.token);
    }

    return response.result;
  }

  static redirectToGoogle(): void {
    window.location.href = API_ENDPOINTS.AUTH.GMAIL_REDIRECT;
  }

  static async loginWithGoogle(code: string): Promise<GoogleLoginResponse> {
    const requestBody: GoogleLoginRequest = { code };
    const response = await apiRequest.post<ApiResponse<GoogleLoginResponse>>(
      API_ENDPOINTS.AUTH.GMAIL_LOGIN,
      requestBody
    );

    if (response.result.token) {
      setAuthToken(response.result.token);
    } else {
      console.error('Invalid API response structure:', response);
      throw new Error('Đăng nhập Google thất bại: Dữ liệu trả về không hợp lệ.');
    }
    return response.result;
  }



  static async register(credentials: RegisterRequest): Promise<void> {
    await apiRequest.post(API_ENDPOINTS.AUTH.REGISTER, credentials);
  }


  static async logout(): Promise<LogoutResponse> {
    const response = await apiRequest.post<ApiResponse<LogoutResponse>>(API_ENDPOINTS.AUTH.LOGOUT);
    if (response.message) {
      clearAuth();
      localStorage.removeItem('currentUser');
    }
    return response.result;
  }

  /**
   * Get current user profile
   */
  static async getCurrentUser(): Promise<ApiResponse<User>> {
    return apiRequest.get<ApiResponse<User>>(API_ENDPOINTS.AUTH.ME);
  }

  // getCurrentUser with silent option to avoid triggering global toasts during bootstrap/polling
  static async getCurrentUserSilent(): Promise<ApiResponse<User>> {
    return apiRequest.get<ApiResponse<User>>(API_ENDPOINTS.AUTH.ME, { headers: { 'X-SILENT': '1' } });
  }

  /**
   * Refresh auth token
   */
  static async refreshToken(refreshToken: string): Promise<{ token: string }> {
    const response = await apiRequest.post<ApiResponse<{ token: string }>>(
      API_ENDPOINTS.AUTH.REFRESH,
      { refreshToken }
    );

    // Update token
    if (response.result.token) {
      setAuthToken(response.result.token);
    }

    return response.result;
  }

  /**
   * Verify email
   */
  static async verifyEmail(token: string): Promise<ApiResponse<void>> {
    return apiRequest.post<ApiResponse<void>>(API_ENDPOINTS.AUTH.VERIFY_EMAIL, { token });
  }

  /**
   * Request password reset
   */
  static async forgotPassword(email: string): Promise<ApiResponse<{ message: string }>> {
    return apiRequest.post<ApiResponse<{ message: string }>>(
      API_ENDPOINTS.AUTH.FORGOT_PASSWORD,
      { email }
    );
  }

  /**
   * Reset password with token
   */
  static async resetPassword(token: string, password: string): Promise<ApiResponse<void>> {
    return apiRequest.post<ApiResponse<void>>(API_ENDPOINTS.AUTH.RESET_PASSWORD, {
      token,
      password,
    });
  }
}

export default AuthService;
