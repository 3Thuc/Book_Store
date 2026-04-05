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
  UpdateProfileRequest,
  UpdateProfileResponse,
  ChangePasswordRequest,
  LogoutResponse,
  RegisterRequest,
  RegisterResponse,
  GoogleLoginRequest,
  GoogleLoginResponse,
  AddAddressRequest,
  UpdateAddressRequest,
} from '../types/api';
import { User } from '../types/user';

export class UserService {

  static async updateProfile(credentials: UpdateProfileRequest): Promise<UpdateProfileResponse> {
    const response = await apiRequest.put<ApiResponse<UpdateProfileResponse>>(
      API_ENDPOINTS.USER.UPDATE_PROFILE,
      credentials
    );

    return response.result;
  }
  static async changePass(credentials: ChangePasswordRequest): Promise<any> {
    const response = await apiRequest.put<ApiResponse<any>>(
      API_ENDPOINTS.USER.CHANGE_PASSWORD,
      credentials
    );

    return response.result;
  }

  static async addAddress(credentials: AddAddressRequest): Promise<any> {
    const response = await apiRequest.post<ApiResponse<any>>(
      API_ENDPOINTS.USER.ADD_ADDRESS,
      credentials
    );

    return response.result;
  }

  static async getAddresses(): Promise<any[]> {
    const response = await apiRequest.get<ApiResponse<any[]>>(API_ENDPOINTS.USER.ADDRESSES);
    return response.result;
  }

  static async updateAddress(id: string, credentials: UpdateAddressRequest): Promise<any> {
    const response = await apiRequest.put<ApiResponse<any>>(
      API_ENDPOINTS.USER.UPDATE_ADDRESS(id),
      credentials
    );

    return response.result;
  }

  static async deleteAddress(id: string): Promise<any> {
    const response = await apiRequest.delete<ApiResponse<any>>(API_ENDPOINTS.USER.DELETE_ADDRESS(id));
    return response.result;
  }

  static async setDefaultAddress(id: string): Promise<any> {
    const response = await apiRequest.put<ApiResponse<any>>(API_ENDPOINTS.USER.SET_DEFAULT_ADDRESS(id));
    return response.result;
  }


  static async getMyOrders(): Promise<any[]> {
    const response = await apiRequest.get<ApiResponse<any[]>>(API_ENDPOINTS.USER.MY_ORDERS);
    return response.result;
  }


  static async getOrderDetail(orderId: string): Promise<any> {
    const response = await apiRequest.get<ApiResponse<any>>(API_ENDPOINTS.USER.ORDER_DETAIL(orderId));
    return response.result;
  }

}
export default UserService;
