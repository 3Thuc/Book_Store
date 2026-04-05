export const API_BASE_URL = 'https://localhost:8443/bookdb';

export const API_VERSION = 'v1';

export const API_ENDPOINTS = {
  AUTH: {
    LOGIN: '/auth/login',
    GMAIL_REDIRECT: `${API_BASE_URL}/auth/google`,
    GMAIL_LOGIN: "/auth/login/google",
    REGISTER: '/auth/register',
    LOGOUT: '/auth/logout',
    REFRESH: '/auth/refresh-token',
    ME: '/auth/me',
    VERIFY_EMAIL: '/auth/verify-email',
    FORGOT_PASSWORD: '/auth/forgot-password',
    RESET_PASSWORD: '/auth/reset-password',
  },

  BOOKS: {
    LIST: '/books',
    DETAIL: (id: string) => `/books/detail/${id}`,
    SEARCH: '/books/search',
    BY_CATEGORY: (categoryId: number) => `/books/category/${categoryId}`,
    FEATURED: '/books/featured',
    BESTSELLERS: '/books/bestsellers',
    NEW_RELEASES: '/books/new-releases',
    RECOMMENDATIONS: '/books/recommendations',
    PERSONALIZED: '/books/personalized',
  },
  CATEGORIES: {
    LIST: '/categories',
  },

  REVIEWS: {
    BY_BOOK: (bookId: string) => `/books/${bookId}/reviews`,
    CREATE: (bookId: string) => `/books/${bookId}/reviews`,
    UPDATE: (bookId: string, reviewId: string) => `/books/${bookId}/reviews/${reviewId}`,
  },

  CART: {
    GET: '/cart',
    ADD: '/cart/items',
    UPDATE: (itemId: string) => `/cart/items/${itemId}`,
    REMOVE: (itemId: string) => `/cart/items/${itemId}`,
    CLEAR: '/cart/clear',
    COUNT: '/cart/count',
  },

  ORDERS: {
    LIST: '/orders',
    DETAIL: (id: string) => `/orders/${id}`,
    CREATE: '/orders',
    UPDATE_STATUS: (id: string) => `/orders/${id}/status`,
    CANCEL: (id: string) => `/orders/${id}/cancel`,
    RETURN: (id: string) => `/orders/${id}/return`,
    TRACK: (id: string) => `/orders/${id}/track`,
  },

  USER: {
    PROFILE: '/user/profile',
    UPDATE_PROFILE: '/user/profile',
    CHANGE_PASSWORD: '/user/password',
    ADDRESSES: '/user/addresses',
    ADD_ADDRESS: '/user/addresses',
    UPDATE_ADDRESS: (id: string) => `/user/addresses/${id}`,
    DELETE_ADDRESS: (id: string) => `/user/addresses/${id}`,
    SET_DEFAULT_ADDRESS: (id: string) => `/user/addresses/${id}/default`,
    WISHLIST: '/user/wishlist',
    ADD_TO_WISHLIST: '/user/wishlist',
    REMOVE_FROM_WISHLIST: (bookId: string) => `/user/wishlist/${bookId}`,
    MY_ORDERS: '/user/orders',
    ORDER_DETAIL: (orderId: string) => `/user/orders/${orderId}`,
    CANCEL_ORDER: (orderId: string) => `/user/orders/${orderId}/cancel`,
    RETURN_ORDER: (orderId: string) => `/user/orders/${orderId}/return`,
    CONFIRM_DELIVERY: (orderId: string) => `/user/orders/${orderId}/confirm-delivery`,
    PROMOTIONS: '/user/promotions',
  },

  ADMIN: {
    BASE: '/admin',
    STATS: '/admin/stats',
    DASHBOARD: '/admin/dashboard',
    BOOKS: '/admin/books',
    BOOK_DETAIL: (id: string) => `/admin/books/${id}`,
    CREATE_BOOK: '/admin/books',
    UPDATE_BOOK: (id: string) => `/admin/books/${id}`,
    DELETE_BOOK: (id: string) => `/admin/books/${id}`,
    CATEGORIES: '/admin/categories',
    CREATE_CATEGORY: '/admin/categories',
    UPDATE_CATEGORY: (id: string) => `/admin/categories/${id}`,
    DELETE_CATEGORY: (id: string) => `/admin/categories/${id}`,
    ORDERS: '/admin/orders',
    ORDER_STATS: '/admin/orders/stats',
    ORDER_STATISTICS: '/admin/order-statistics',
    ORDER_DETAIL: (id: string | number) => `/admin/orders/${id}`,
    ORDER_TIMELINE: (id: string | number) => `/admin/orders/${id}/timeline`,
    UPDATE_ORDER_STATUS: (id: string | number) => `/admin/orders/${id}/status`,
    CONFIRM_COD_ORDER: (id: string | number) => `/admin/orders/${id}/confirm-cod`,
    SHIP_ORDER: (id: string | number) => `/admin/orders/${id}/ship`,
    CANCEL_ORDER: (id: string | number) => `/admin/orders/${id}/cancel`,
    APPROVE_RETURN: (id: string | number) => `/admin/orders/${id}/approve-return`,
    REJECT_RETURN: (id: string | number) => `/admin/orders/${id}/reject-return`,
    SEARCH_ORDERS: '/admin/orders/search',
    BULK_UPDATE_ORDERS: '/admin/orders/bulk-update',
    EXPORT_ORDERS: '/admin/orders/export',
    USERS: '/admin/users',
    UPDATE_USER: (id: string) => `/admin/users/${id}`,
    DELETE_USER: (id: string) => `/admin/users/${id}`,
    RESET_USER_PASSWORD: (id: string) => `/admin/users/${id}/reset-password`,
    INVENTORY: '/admin/inventory',
    UPDATE_STOCK: (bookId: string) => `/admin/inventory/${bookId}`,
    PROMOTIONS: '/admin/promotions',
    CREATE_PROMOTION: '/admin/promotions',
    UPDATE_PROMOTION: (id: string) => `/admin/promotions/${id}`,
    DELETE_PROMOTION: (id: string) => `/admin/promotions/${id}`,
    // Publishers and Authors admin endpoints
    PUBLISHERS: '/admin/publishers',
    CREATE_PUBLISHER: '/admin/publishers',
    UPDATE_PUBLISHER: (id: string) => `/admin/publishers/${id}`,
    DELETE_PUBLISHER: (id: string) => `/admin/publishers/${id}`,
    AUTHORS: '/admin/authors',
    CREATE_AUTHOR: '/admin/authors',
    UPDATE_AUTHOR: (id: string) => `/admin/authors/${id}`,
    DELETE_AUTHOR: (id: string) => `/admin/authors/${id}`,
    SETTINGS: '/admin/settings',
    UPDATE_SETTINGS: '/admin/settings',
  },

  PAYMENT: {
    CREATE_PAYMENT: '/payment/create',
    VERIFY_PAYMENT: '/payment/verify',
    QR_CODE: (orderId: string) => `/payment/qr/${orderId}`,
  },
} as const;

export const QUERY_KEYS = {
  AUTH: {
    ME: ['auth', 'me'],
  },
  BOOKS: {
    ALL: ['books'],
    LIST: (filters?: any) => ['books', 'list', filters],
    DETAIL: (id: string) => ['books', 'detail', id],
    SEARCH: (query: string) => ['books', 'search', query],
    CATEGORIES: ['books', 'categories'],
    BY_CATEGORY: (category: string) => ['books', 'category', category],
    FEATURED: ['books', 'featured'],
    BESTSELLERS: ['books', 'bestsellers'],
    NEW_RELEASES: ['books', 'new-releases'],
    RECOMMENDATIONS: ['books', 'recommendations'],
    PERSONALIZED: ['books', 'personalized'],
  },
  REVIEWS: {
    BY_BOOK: (bookId: string) => ['reviews', 'book', bookId],
  },
  CART: {
    GET: ['cart'],
    COUNT: ['cart', 'count'],
  },
  ORDERS: {
    LIST: ['orders'],
    DETAIL: (id: string) => ['orders', 'detail', id],
  },
  USER: {
    PROFILE: ['user', 'profile'],
    ADDRESSES: ['user', 'addresses'],
    WISHLIST: ['user', 'wishlist'],
  },
  ADMIN: {
    STATS: ['admin', 'stats'],
    BOOKS: (filters?: any) => ['admin', 'books', filters],
    ORDERS: (filters?: any) => ['admin', 'orders', filters],
    USERS: (filters?: any) => ['admin', 'users', filters],
    INVENTORY: ['admin', 'inventory'],
    PROMOTIONS: ['admin', 'promotions'],
  },
} as const;

export const PAGINATION = {
  DEFAULT_PAGE: 1,
  DEFAULT_LIMIT: 20,
  MAX_LIMIT: 100,
} as const;

export const CACHE_TIME = {
  SHORT: 1000 * 60 * 5,
  MEDIUM: 1000 * 60 * 15,
  LONG: 1000 * 60 * 60,
} as const;

export const RETRY_CONFIG = {
  MAX_RETRIES: 3,
  RETRY_DELAY: 1000,
} as const;
