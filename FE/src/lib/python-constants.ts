/**
 * Python Backend API Endpoints
 * Port 8000 - Recommendation & Search Service
 */

export const PYTHON_API_ENDPOINTS = {
  // ==========================================
  // HEALTH CHECK
  // ==========================================
  HEALTH: '/health',

  // ==========================================
  // RECOMMENDATION SERVICE
  // ==========================================
  RECOMMEND: {
    // Home
    POPULAR: '/recommend/popular',
    TRENDING: '/recommend/trending',
    TOP_RATED: '/recommend/top-rated',

    // Item-based (Similar Books)
    BOOK_RULE: (bookId: number) => `/recommend/book/${bookId}/rule`,
    BOOK_ALSO_BOUGHT: (bookId: number) => `/recommend/book/${bookId}/cb-fallback`,
    BOOK_CB: (bookId: number) => `/recommend/book/${bookId}/cb`,
    BOOK_CF: (bookId: number) => `/recommend/book/${bookId}/cf`,

    // User Personalized
    USER_RULE: (userId: number) => `/recommend/user/${userId}/rule`,
    USER_CF: (userId: number) => `/recommend/user/${userId}/cf`,
    USER_FOR_YOU: (userId: number) => `/recommend/user/${userId}/for-you`,

    // Admin
    USER_CF_REBUILD: (userId: number) => `/recommend/user/${userId}/cf/rebuild`,
    BOOK_CB_CLEAR_CACHE: (bookId: number) => `/recommend/book/${bookId}/cb/clear-cache`,
  },

  // ==========================================
  // SEARCH SERVICE
  // ==========================================
  SEARCH: {
    SUGGEST: '/books/suggest',
    SEARCH: '/books/search',
    SEARCH_BY_IMAGE: '/api/books/search-by-image',
  },

  // ==========================================
  // ADMIN SEARCH
  // ==========================================
  ADMIN: {
    INDEX_BOOK: (bookId: number) => `/admin/search/index-book/${bookId}`,
  },
};
