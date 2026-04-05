/**
 * Python Backend API Service
 * Gọi các API từ Python backend (port 8000)
 */

import { pythonApiRequest } from '../lib/python-api-client';
import { PYTHON_API_ENDPOINTS } from '../lib/python-constants';

// ==========================================
// RECOMMENDATION SERVICE
// ==========================================

export interface BookRecommendation {
  book_id: number;
  title: string;
  author_name?: string;
  price?: number;
  image_url?: string;
  score?: number;
  reason?: string;
}

export class PythonRecommendService {
  // Home recommendations
  static getPopular(limit: number = 10) {
    return pythonApiRequest.get<BookRecommendation[]>(PYTHON_API_ENDPOINTS.RECOMMEND.POPULAR, {
      params: { limit }
    });
  }

  static getTrending(days: number = 7, limit: number = 10) {
    return pythonApiRequest.get<BookRecommendation[]>(PYTHON_API_ENDPOINTS.RECOMMEND.TRENDING, {
      params: { days, limit }
    });
  }

  static getTopRated(limit: number = 10) {
    return pythonApiRequest.get<BookRecommendation[]>(PYTHON_API_ENDPOINTS.RECOMMEND.TOP_RATED, {
      params: { limit }
    });
  }

  // Item-based recommendations
  static getSimilarByRule(bookId: number, limit: number = 10) {
    return pythonApiRequest.get<BookRecommendation[]>(PYTHON_API_ENDPOINTS.RECOMMEND.BOOK_RULE(bookId), {
      params: { limit }
    });
  }

  static getAlsoBought(bookId: number, limit: number = 10) {
    return pythonApiRequest.get<BookRecommendation[]>(PYTHON_API_ENDPOINTS.RECOMMEND.BOOK_ALSO_BOUGHT(bookId), {
      params: { limit }
    });
  }

  static getSimilarByCB(bookId: number, limit: number = 10) {
    return pythonApiRequest.get<BookRecommendation[]>(PYTHON_API_ENDPOINTS.RECOMMEND.BOOK_CB(bookId), {
      params: { limit }
    });
  }


  static getSimilarByCF(bookId: number, algo: string = 'CF_IMPLICIT', limit: number = 10) {
    return pythonApiRequest.get<BookRecommendation[]>(PYTHON_API_ENDPOINTS.RECOMMEND.BOOK_CF(bookId), {
      params: { algo, limit }
    });
  }

  // User personalized recommendations
  static getUserRuleBased(userId: number, limit: number = 10) {
    return pythonApiRequest.get<BookRecommendation[]>(PYTHON_API_ENDPOINTS.RECOMMEND.USER_RULE(userId), {
      params: { limit }
    });
  }

  static getUserCF(userId: number, limit: number = 20) {
    return pythonApiRequest.get<BookRecommendation[]>(PYTHON_API_ENDPOINTS.RECOMMEND.USER_CF(userId), {
      params: { limit }
    });
  }

  static getForYou(userId: number, limit: number = 20) {
    return pythonApiRequest.get<BookRecommendation[]>(PYTHON_API_ENDPOINTS.RECOMMEND.USER_FOR_YOU(userId), {
      params: { limit }
    });
  }

  // Admin functions
  static rebuildUserCF(userId: number, days: number = 90, topn: number = 50) {
    return pythonApiRequest.post(PYTHON_API_ENDPOINTS.RECOMMEND.USER_CF_REBUILD(userId), null, {
      params: { days, topn }
    });
  }

  static clearBookCBCache(bookId: number) {
    return pythonApiRequest.post(PYTHON_API_ENDPOINTS.RECOMMEND.BOOK_CB_CLEAR_CACHE(bookId));
  }
}

// ==========================================
// SEARCH SERVICE
// ==========================================

export interface SearchResult {
  book_id: number;
  title: string;
  author_name?: string;
  price?: number;
  image_url?: string;
  description?: string;
  rating?: number;
  category_name?: string;
  language?: string;
  format?: string;
  in_stock?: boolean;
  _score?: number;
}

export interface SearchResponse {
  results: SearchResult[];
  items: SearchResult[];
  total: number;
  page: number;
  limit: number;
  took_ms: number;
}

export class PythonSearchService {
  static suggest(query: string, limit: number = 10) {
    return pythonApiRequest.get<{ title: string; author_name?: string }[]>(PYTHON_API_ENDPOINTS.SEARCH.SUGGEST, {
      params: { q: query, limit }
    });
  }

  static search(params: {
    q: string;
    page?: number;
    limit?: number;
    in_stock?: boolean;
    category?: string;
    language?: string;
    fmt?: string;
    sort?: 'relevance' | 'price_asc' | 'price_desc' | 'newest' | 'rating_desc';
  }) {
    return pythonApiRequest.get<SearchResponse>(PYTHON_API_ENDPOINTS.SEARCH.SEARCH, { params });
  }

  // Admin
  static indexBook(bookId: number) {
    return pythonApiRequest.post(PYTHON_API_ENDPOINTS.ADMIN.INDEX_BOOK(bookId));
  }
}

export default { PythonRecommendService, PythonSearchService };
