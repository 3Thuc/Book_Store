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
    // ─ Trang chủ ───────────────────────────────────────────
    POPULAR:   '/recommend/popular',
    TRENDING:  '/recommend/trending',
    TOP_RATED: '/recommend/top-rated',

    // ─ Trang chi tiết sách ──────────────────────────────
    /** Sách tương tự AI (SBERT k-NN + fallback popular) – đang dùng */
    BOOK_CB_FALLBACK: (bookId: number) => `/recommend/book/${bookId}/cb-fallback`,
    /** Khách hàng cũng mua (co-purchase từ đơn hàng thực tế) – đang dùng */
    BOOK_ALSO_BOUGHT: (bookId: number) => `/recommend/book/${bookId}/also`,

    // ─ Cá nhân hoá ─────────────────────────────────────────
    /** CF + Rule + Popular fallback theo user – đang dùng */
    USER_FOR_YOU: (userId: number) => `/recommend/user/${userId}/for-you`,

    // ─ Admin / Maintenance ──────────────────────────────────
    USER_CF_REBUILD:    (userId: number) => `/recommend/user/${userId}/cf/rebuild`,
    BOOK_CB_CLEAR_CACHE: (bookId: number) => `/recommend/book/${bookId}/cb/clear-cache`,
  },

  // ==========================================
  // SEARCH SERVICE
  // ==========================================
  SEARCH: {
    SUGGEST: '/books/suggest',
    SEARCH: '/books/search',
  },

  // ==========================================
  // ADMIN SEARCH
  // ==========================================
  ADMIN: {
    INDEX_BOOK: (bookId: number) => `/admin/search/index-book/${bookId}`,
  },
};
