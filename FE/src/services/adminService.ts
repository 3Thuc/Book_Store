import { apiRequest } from '../lib/api-client';
import { API_ENDPOINTS } from '../lib/constants';

export interface GetBooksParams {
  page?: number;
  limit?: number;
  search?: string;
  category?: string;
}

// A small centralized admin API surface. Keep payload types loose for now
// (can be tightened later to your domain types).
export const adminService = {
  // Books
  getBooks: (params?: GetBooksParams) => apiRequest.get(API_ENDPOINTS.ADMIN.BOOKS, { params }),
  createBook: (payload: any) => apiRequest.post(API_ENDPOINTS.ADMIN.CREATE_BOOK, payload, { 
    headers: payload instanceof FormData ? { 'Content-Type': 'multipart/form-data' } : {} 
  }),
  updateBook: (id: string, payload: any) => apiRequest.patch(API_ENDPOINTS.ADMIN.UPDATE_BOOK(id), payload, { 
    headers: payload instanceof FormData ? { 'Content-Type': 'multipart/form-data' } : {} 
  }),
  deleteBook: (id: string) => apiRequest.delete(API_ENDPOINTS.ADMIN.DELETE_BOOK(id)),

  // Categories
  getCategories: () => apiRequest.get(API_ENDPOINTS.ADMIN.CATEGORIES),
  createCategory: (payload: any) => apiRequest.post(API_ENDPOINTS.ADMIN.CREATE_CATEGORY, payload),
  updateCategory: (id: string, payload: any) => apiRequest.patch(API_ENDPOINTS.ADMIN.UPDATE_CATEGORY(id), payload),
  deleteCategory: (id: string) => apiRequest.delete(API_ENDPOINTS.ADMIN.DELETE_CATEGORY(id)),

  // Publishers
  getPublishers: () => apiRequest.get(API_ENDPOINTS.ADMIN.PUBLISHERS),
  createPublisher: (payload: any) => apiRequest.post(API_ENDPOINTS.ADMIN.CREATE_PUBLISHER, payload),
  updatePublisher: (id: string, payload: any) => apiRequest.patch(API_ENDPOINTS.ADMIN.UPDATE_PUBLISHER(id), payload),
  deletePublisher: (id: string) => apiRequest.delete(API_ENDPOINTS.ADMIN.DELETE_PUBLISHER(id)),

  // Authors
  getAuthors: () => apiRequest.get(API_ENDPOINTS.ADMIN.AUTHORS),
  createAuthor: (payload: any) => apiRequest.post(API_ENDPOINTS.ADMIN.CREATE_AUTHOR, payload),
  updateAuthor: (id: string, payload: any) => apiRequest.patch(API_ENDPOINTS.ADMIN.UPDATE_AUTHOR(id), payload),
  deleteAuthor: (id: string) => apiRequest.delete(API_ENDPOINTS.ADMIN.DELETE_AUTHOR(id)),

  // Users
  getUsers: () => apiRequest.get(API_ENDPOINTS.ADMIN.USERS),
  createUser: (payload: any) => apiRequest.post(API_ENDPOINTS.ADMIN.USERS, payload, { headers: { 'X-SILENT': '1' } }),
  updateUser: (id: string, payload: any) => apiRequest.patch(API_ENDPOINTS.ADMIN.UPDATE_USER(id), payload),
  deleteUser: (id: string) => apiRequest.delete(API_ENDPOINTS.ADMIN.DELETE_USER(id)),
  resetUserPassword: (id: string) => apiRequest.post(API_ENDPOINTS.ADMIN.RESET_USER_PASSWORD(id)),

  // Promotions
  getPromotions: () => apiRequest.get(API_ENDPOINTS.ADMIN.PROMOTIONS),
  createPromotion: (payload: any) => apiRequest.post(API_ENDPOINTS.ADMIN.CREATE_PROMOTION, payload),
  updatePromotion: (id: string, payload: any) => apiRequest.patch(API_ENDPOINTS.ADMIN.UPDATE_PROMOTION(id), payload),
  deletePromotion: (id: string) => apiRequest.delete(API_ENDPOINTS.ADMIN.DELETE_PROMOTION(id)),

  // Inventory
  getInventory: () => apiRequest.get(API_ENDPOINTS.ADMIN.INVENTORY),
  updateStock: (bookId: string | number, payload: any) => apiRequest.patch(API_ENDPOINTS.ADMIN.UPDATE_STOCK(String(bookId)), payload),

  // Orders
  getOrders: (params?: any) => apiRequest.get(API_ENDPOINTS.ADMIN.ORDERS, { params }),
  getOrderStats: () => apiRequest.get(API_ENDPOINTS.ADMIN.ORDER_STATS),
  getOrderStatistics: () => apiRequest.get(API_ENDPOINTS.ADMIN.ORDER_STATISTICS),
  getOrderById: (id: string | number) => apiRequest.get(API_ENDPOINTS.ADMIN.ORDER_DETAIL(id)),
  getOrderTimeline: (id: string | number) => apiRequest.get(API_ENDPOINTS.ADMIN.ORDER_TIMELINE(id)),
  updateOrderStatus: (id: string | number, payload: any) => apiRequest.patch(API_ENDPOINTS.ADMIN.UPDATE_ORDER_STATUS(id), payload),
  confirmCODOrder: (id: string | number) => apiRequest.post(API_ENDPOINTS.ADMIN.CONFIRM_COD_ORDER(id), {}),
  shipOrder: (id: string | number, payload?: any) => apiRequest.post(API_ENDPOINTS.ADMIN.SHIP_ORDER(id), payload || {}),
  cancelOrder: (id: string | number, payload: any) => apiRequest.post(API_ENDPOINTS.ADMIN.CANCEL_ORDER(id), payload),
  approveReturn: (id: string | number, payload?: any) => apiRequest.post(API_ENDPOINTS.ADMIN.APPROVE_RETURN(id), payload || {}),
  rejectReturn: (id: string | number, payload: any) => apiRequest.post(API_ENDPOINTS.ADMIN.REJECT_RETURN(id), payload),
  searchOrders: (params: any) => apiRequest.get(API_ENDPOINTS.ADMIN.SEARCH_ORDERS, { params }),
  bulkUpdateOrders: (payload: any) => apiRequest.post(API_ENDPOINTS.ADMIN.BULK_UPDATE_ORDERS, payload),
  exportOrders: (params?: any) => apiRequest.get(API_ENDPOINTS.ADMIN.EXPORT_ORDERS, { params, responseType: 'blob' }),

  // Dashboard (single endpoint for all dashboard data)
  getDashboard: () => apiRequest.get(API_ENDPOINTS.ADMIN.DASHBOARD),
};

export default adminService;
