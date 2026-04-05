import { apiRequest } from '../lib/api-client';
import { API_ENDPOINTS } from '../lib/constants';
import { Book } from '../types/book';
import {
  ApiResponse,
  ApiPageResponse,
  BooksListRequest,
  BookSearchRequest,
} from '../types/api';

export class BookService {
  static async getBooks(filters?: BooksListRequest): Promise<ApiPageResponse<Book>> {
    return apiRequest.get<ApiPageResponse<Book>>(API_ENDPOINTS.BOOKS.LIST, {
      params: filters,
    });
  }

  static async getBookById(id: string): Promise<ApiResponse<Book>> {
    return apiRequest.get<ApiResponse<Book>>(API_ENDPOINTS.BOOKS.DETAIL(id));
  }

  static async searchBooks(params: BookSearchRequest): Promise<ApiPageResponse<Book>> {
    return apiRequest.get<ApiPageResponse<Book>>(API_ENDPOINTS.BOOKS.SEARCH, { params });
  }

  static async getCategories(): Promise<ApiResponse<any[]>> {
    return apiRequest.get<ApiResponse<any[]>>(API_ENDPOINTS.CATEGORIES.LIST);
  }

  static async getBooksByCategory(
    categoryId: number,
    filters?: BooksListRequest
  ): Promise<ApiPageResponse<Book>> {
    return apiRequest.get<ApiPageResponse<Book>>(
      API_ENDPOINTS.BOOKS.BY_CATEGORY(categoryId),
      { params: filters }
    );
  }

  static async getFeaturedBooks(limit?: number): Promise<ApiResponse<Book[]>> {
    return apiRequest.get<ApiResponse<Book[]>>(API_ENDPOINTS.BOOKS.FEATURED, {
      params: { limit },
    });
  }

  static async getBestsellers(limit?: number): Promise<ApiResponse<Book[]>> {
    return apiRequest.get<ApiResponse<Book[]>>(API_ENDPOINTS.BOOKS.BESTSELLERS, {
      params: { limit },
    });
  }

  static async getNewReleases(limit?: number): Promise<ApiResponse<Book[]>> {
    return apiRequest.get<ApiResponse<Book[]>>(API_ENDPOINTS.BOOKS.NEW_RELEASES, {
      params: { limit },
    });
  }

  static async getRecommendations(bookId: string, limit?: number): Promise<ApiResponse<Book[]>> {
    return apiRequest.get<ApiResponse<Book[]>>(API_ENDPOINTS.BOOKS.RECOMMENDATIONS, {
      params: { bookId, limit },
    });
  }

  static async getPersonalizedRecommendations(limit?: number): Promise<ApiResponse<Book[]>> {
    return apiRequest.get<ApiResponse<Book[]>>(API_ENDPOINTS.BOOKS.PERSONALIZED, {
      params: { limit },
    });
  }
}

export default BookService;
