/**
 * Review Service
 * API calls related to book reviews with automatic fallback to mock data
 */

import { apiRequest } from '../lib/api-client';
import { API_ENDPOINTS } from '../lib/constants';
import { Review } from '../types/book';
import { 
  ApiResponse,
  CreateReviewRequest,
  UpdateReviewRequest,
  QueryFilters,
} from '../types/api';

export class ReviewService {
  /**
   * Get reviews for a specific book
   */
  static async getReviewsByBook(
    bookId: string,
    filters?: QueryFilters
  ): Promise<ApiResponse<Review[]>> {
    return apiRequest.get<ApiResponse<Review[]>>(
      API_ENDPOINTS.REVIEWS.BY_BOOK(bookId),
      { params: filters }
    );
  }

  /**
   * Create a new review for a book
   */
  static async createReview(
    bookId: string,
    data: CreateReviewRequest
  ): Promise<ApiResponse<Review>> {
    return apiRequest.post<ApiResponse<Review>>(
      API_ENDPOINTS.REVIEWS.CREATE(bookId),
      data
    );
  }

  /**
   * Update an existing review
   */
  static async updateReview(
    bookId: string,
    reviewId: string,
    data: UpdateReviewRequest
  ): Promise<ApiResponse<Review>> {
    return apiRequest.put<ApiResponse<Review>>(
      API_ENDPOINTS.REVIEWS.UPDATE(bookId, reviewId),
      data
    );
  }

}

export default ReviewService;
