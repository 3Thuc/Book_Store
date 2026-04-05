import { apiClient } from '../lib/api-client';

export interface NotificationResponse {
  id: string;
  type: 'new_order' | 'low_stock' | 'order_completed' | 'return_requested' | 'order_cancelled' | 'delivery_completed';
  title: string;
  message: string;
  time: string;
  isRead: boolean;
  orderId?: string;
  userName?: string;
}

export interface NotificationPageResponse {
  content: NotificationResponse[];
  totalPages: number;
  totalElements: number;
  size: number;
  number: number;
}

const notificationService = {
  /**
   * Get all notifications with pagination
   */
  getAllNotifications: async (page = 0, size = 20) => {
    const response = await apiClient.get<{ result: NotificationPageResponse }>(
      `/api/notifications?page=${page}&size=${size}`
    );

    // Map trường read từ API sang isRead cho FE
    const mapped = {
      ...response.data.result,
      content: response.data.result.content.map((n: any) => ({
        ...n,
        isRead: n.isRead !== undefined ? n.isRead : n.read,
      })),
    };
    console.log('API notifications:', mapped);
    return mapped;
  },

  /**
   * Get unread notifications
   */
  getUnreadNotifications: async () => {
    const response = await apiClient.get<{ result: NotificationResponse[] }>(
      '/api/notifications/unread'
    );
    return response.data.result;
  },

  /**
   * Get unread count
   */
  getUnreadCount: async () => {
    const response = await apiClient.get<{ result: number }>(
      '/api/notifications/unread/count'
    );
    return response.data.result;
  },

  /**
   * Mark notification as read
   */
  markAsRead: async (id: string) => {
    const response = await apiClient.put<{ result: string }>(
      `/api/notifications/${id}/read`
    );
    return response.data.result;
  },

  /**
   * Mark all notifications as read
   */
  markAllAsRead: async () => {
    const response = await apiClient.put<{ result: string }>(
      '/api/notifications/read-all'
    );
    return response.data.result;
  },

  /**
   * Delete notification
   */
  deleteNotification: async (id: string) => {
    const response = await apiClient.delete<{ result: string }>(
      `/api/notifications/${id}`
    );
    return response.data.result;
  },
};

export default notificationService;
