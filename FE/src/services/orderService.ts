/**
 * Order Service
 * API calls related to orders
 */

import { apiRequest } from '../lib/api-client';
import { API_ENDPOINTS } from '../lib/constants';
import { Order, OrderStatus } from '../types/order';
import { 
  ApiResponse,
  PaginatedResponse,
  CreateOrderRequest,
  UpdateOrderStatusRequest,
  QueryFilters,
} from '../types/api';

export interface CreateReviewRequest {
  book_id: string;
  rating: number;
  review: string;
}

export interface UpdateReviewRequest {
  rating?: number;
  review?: string;
}

export class OrderService {
  /**
   * Get all orders for current user
   */
  static async getOrders(filters?: QueryFilters): Promise<PaginatedResponse<Order>> {
    return apiRequest.get<PaginatedResponse<Order>>(API_ENDPOINTS.ORDERS.LIST, {
      params: filters,
    });
  }

  /**
   * Get a single order by ID
   */
  static async getOrderById(id: string): Promise<ApiResponse<Order>> {
    return apiRequest.get<ApiResponse<Order>>(API_ENDPOINTS.ORDERS.DETAIL(id));
  }

  /**
   * Create a new order
   */
  static async createOrder(data: CreateOrderRequest): Promise<ApiResponse<Order>> {
    return apiRequest.post<ApiResponse<Order>>(API_ENDPOINTS.ORDERS.CREATE, data);
  }

  /**
   * Update order status
   */
  static async updateOrderStatus(
    orderId: string,
    data: UpdateOrderStatusRequest
  ): Promise<ApiResponse<Order>> {
    return apiRequest.patch<ApiResponse<Order>>(
      API_ENDPOINTS.ORDERS.UPDATE_STATUS(orderId),
      data
    );
  }

  /**
   * Cancel an order (User)
   */
  static async cancelOrder(orderId: string, reason?: string): Promise<ApiResponse<Order>> {
    return apiRequest.post<ApiResponse<Order>>(API_ENDPOINTS.USER.CANCEL_ORDER(orderId), {
      reason,
    });
  }

  /**
   * Request order return (User)
   */
  static async returnOrder(
    orderId: string,
    data: { reason: string; items?: string[] }
  ): Promise<ApiResponse<Order>> {
    return apiRequest.post<ApiResponse<Order>>(API_ENDPOINTS.USER.RETURN_ORDER(orderId), data);
  }

  /**
   * Confirm delivery (User)
   */
  static async confirmDelivery(orderId: string): Promise<ApiResponse<Order>> {
    return apiRequest.post<ApiResponse<Order>>(API_ENDPOINTS.USER.CONFIRM_DELIVERY(orderId));
  }

  /**
   * Track order status
   */
  static async trackOrder(orderId: string): Promise<ApiResponse<{
    order: Order;
    timeline: Array<{
      status: OrderStatus;
      timestamp: string;
      note?: string;
    }>;
  }>> {
    return apiRequest.get<ApiResponse<any>>(API_ENDPOINTS.ORDERS.TRACK(orderId));
  }

}

export default OrderService;
