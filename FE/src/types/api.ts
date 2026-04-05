/**
 * API Response Types
 * Common types for API requests and responses
 */

/**
 * Generic API Response wrapper
 */

export interface ApiResponse<T = any> {
  code: number;
  result: T;
  message?: string;
}

/**
 * Backend paginated result shape (matches BE you've shared)
 */
export interface PageResult<T = any> {
  books: T[];
  page: number;
  size: number;
  totalElements: number;
  totalPages: number;
}

/**
 * Api Page Response wrapper using PageResult
 */
export type ApiPageResponse<T = any> = ApiResponse<PageResult<T>>;

/**
 * Create Order Request payload
 * Matches backend orders table schema
 */
export interface CreateOrderRequest {
  addressId: number;
  promoId?: number;
  paymentMethod: 'COD' | 'CreditCard'; // Backend enum: COD, CreditCard, E-Wallet
  note?: string;
  orderDetails: Array<{
    bookId: number;
    quantity: number;
  }>;
}

/**
 * Entity-specific Page Results for BE keys other than `books`
 */
export interface OrdersPageResult<T = any> {
  orders: T[];
  page: number;
  size: number;
  totalElements: number;
  totalPages: number;
}
export type OrdersPageResponse<T = any> = ApiResponse<OrdersPageResult<T>>;

export interface ReviewsPageResult<T = any> {
  reviews: T[];
  page: number;
  size: number;
  totalElements: number;
  totalPages: number;
}
export type ReviewsPageResponse<T = any> = ApiResponse<ReviewsPageResult<T>>;
/**
 * Paginated API Response
 */
export interface PaginatedResponse<T = any> {
  code: boolean;
  result: T[];
  meta: PaginationMeta;
  message?: string;
}

/**
 * Response meta result
 */
export interface ResponseMeta {
  timestamp?: string;
  requestId?: string;
  version?: string;
}

/**
 * Pagination meta result
 */
export interface PaginationMeta {
  page: number;
  limit: number;
  total: number;
  totalPages: number;
  hasMore: boolean;
  hasPrev: boolean;
  hasNext: boolean;
}

/**
 * Error Response
 */
export interface ApiError {
  result: false;
  message: string;
  errors?: Record<string, string[]>;
  code?: string;
  statusCode?: number;
}

/**
 * Query Filters
 */
export interface QueryFilters {
  page?: number;
  limit?: number;
  sort?: string;
  order?: 'asc' | 'desc';
  search?: string;
  category?: string;
  [key: string]: any;
}

/**
 * Auth API Types
 */
export interface LoginRequest {
  email: string;
  password: string;
}

export interface LoginResponse {
  token: string;
  id: number;
  fullName: string;
  email: string;
  role: 'customer' | 'admin' | 'staff';
  status: 'active' | 'inactive' | 'deleted';
  userName?: string;
  phoneNumber: string;
}

export interface GoogleLoginRequest {
  code: string;
}


export interface GoogleLoginResponse {
    token: string;
  id: number;
  fullName: string;
  email: string;
  role: 'customer' | 'admin' | 'staff';
  status: 'active' | 'inactive' | 'deleted';
  userName?: string;
  phoneNumber: string;
}

export interface RegisterRequest {
  fullName: string;
  email: string;
  password: string;
}

export interface RegisterResponse {
  token: string;
  id: number;
  name: string;
  email: string;
  role: 'customer' | 'admin' | 'staff';
  status: 'active' | 'inactive' | 'deleted';
  username?: string;
}



export interface LogoutResponse {
  success: boolean;
  message?: string;
}

/**
 * Books API Types
 */
export interface BooksListRequest extends QueryFilters {
  category?: string;
  categoryId?: number | string;
  minPrice?: number;
  maxPrice?: number;
  minRating?: number;
  author?: string;
}

export interface BookSearchRequest {
  q: string;
  page?: number;
  limit?: number;
  category?: string;
}

/**
 * Reviews API Types
 */
export interface CreateReviewRequest {
  rating: number;
  review: string;  // Thêm field này
}

export interface UpdateReviewRequest {
  rating?: number;
  review?: string;
}

/**
 * Cart API Types
 */
export interface AddToCartRequest {
  bookId: string;
  quantity: number;
}

export interface UpdateCartItemRequest {
  quantity: number;
}

/**
 * Order API Types
 */
export interface UpdateOrderStatusRequest {
  paymentStatus: 'PENDING' | 'CONFIRMED' | 'PROCESSING' | 'SHIPPING' | 'DELIVERED' | 'CANCELLED' | 'RETURNED';
  note?: string;
}

/**
 * User API Types
 */
export interface UpdateProfileRequest {
  fullName?: string;
  phoneNumber?: string;
  userName?: string;
}

export interface UpdateProfileResponse {
  fullName?: string;
  phoneNumber?: string;
  userName?: string;
}

export interface ChangePasswordRequest {
  newPassword: string;
  currentPassword: string;
}

export interface AddAddressRequest {
  recipientName: string;
  recipientPhone: string;
  address: string;
  district?: string;
  city?: string;
  isDefault?: boolean;
}

export interface UpdateAddressRequest extends Partial<AddAddressRequest> { }

/**
 * Admin API Types
 */
export interface CreateBookRequest {
  title: string;
  author: string;
  category: string;
  description: string;
  price: number;
  originalPrice?: number;
  image: string;
  isbn?: string;
  publisher?: string;
  publishedYear: number;
  pages?: number;
  language?: string;
  stock: number;
}

export interface UpdateBookRequest extends Partial<CreateBookRequest> { }

export interface UpdateStockRequest {
  quantity: number;
  operation: 'add' | 'subtract' | 'set';
  reason?: string;
}

export interface CreatePromotionRequest {
  code: string;
  name: string;
  description?: string;
  discountType: 'percentage' | 'fixed';
  discountValue: number;
  minOrderValue?: number;
  maxDiscount?: number;
  startDate: string;
  endDate: string;
  usageLimit?: number;
  isActive?: boolean;
}

export interface UpdatePromotionRequest extends Partial<CreatePromotionRequest> { }

/**
 * Loading State
 */
export interface LoadingState {
  isLoading: boolean;
  isError: boolean;
  error: Error | null;
}

/**
 * Mutation State
 */
export interface MutationState<T = any> extends LoadingState {
  isSuccess: boolean;
  result: T | null;
}
