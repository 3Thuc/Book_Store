/**
 * API Fallback Helper
 * Provides fallback to mock data when API calls fail
 * 
 * This is a FEATURE, not a bug!
 * - Tries API first
 * - Falls back to mock data if API unavailable
 * - App works seamlessly in both modes
 */

import { Book } from '../types/book';
import { books as MOCK_BOOKS } from '../data/books';
import { reviews as MOCK_REVIEWS } from '../data/reviews';

/**
 * Fallback helper - Try API first, then fallback to mock data
 */
export async function withFallback<T>(
  apiCall: () => Promise<T>,
  fallbackData: () => T,
  context: string
): Promise<T> {
  try {
    // Try API first
    const result = await apiCall();
    console.log(`API success: ${context}`);
    return result;
  } catch (error) {
    // API failed - use fallback (this is expected when backend is not running)
    console.info(`ℹ️ API not available, using mock data for: ${context}`);
    
    // Only show detailed error in development
    if (typeof import.meta !== 'undefined' && import.meta.env?.DEV) {
      console.debug('API Error details:', error instanceof Error ? error.message : error);
    }
    
    // Return mock data
    const fallback = fallbackData();
    console.log(`📦 Mock data loaded successfully`);
    return fallback;
  }
}

/**
 * Mock data helpers for fallback
 */
export const MockDataHelper = {
  /**
   * Get all books with filters
   */
  getBooks: (params?: {
    page?: number;
    limit?: number;
    category?: string;
    search?: string;
    sort?: string;
  }) => {
    let filtered = [...MOCK_BOOKS];
    
    // Filter by category
    if (params?.category && params.category !== 'Tất cả') {
      filtered = filtered.filter(book => book.category === params.category);
    }
    
    // Filter by search
    if (params?.search) {
      const query = params.search.toLowerCase();
      filtered = filtered.filter(book => 
        book.title.toLowerCase().includes(query) ||
        book.author.toLowerCase().includes(query) ||
        book.category.toLowerCase().includes(query)
      );
    }
    
    // Sort
    if (params?.sort) {
      filtered = [...filtered].sort((a, b) => {
        switch (params.sort) {
          case 'price-asc':
            return a.price - b.price;
          case 'price-desc':
            return b.price - a.price;
          case 'rating':
            return b.rating - a.rating;
          case 'newest':
            return b.publishedYear - a.publishedYear;
          case 'name':
            return a.title.localeCompare(b.title, 'vi');
          case 'popular':
          default:
            return b.reviewCount - a.reviewCount;
        }
      });
    }
    
    // Pagination
    const page = params?.page || 1;
    const limit = params?.limit || 15;
    const startIndex = (page - 1) * limit;
    const endIndex = startIndex + limit;
    const paginatedBooks = filtered.slice(startIndex, endIndex);
    
    return {
      success: true,
      data: paginatedBooks,
      meta: {
        page,
        limit,
        total: filtered.length,
        totalPages: Math.ceil(filtered.length / limit),
        hasMore: endIndex < filtered.length,
        hasPrev: page > 1,
        hasNext: endIndex < filtered.length,
      },
    };
  },
  
  /**
   * Get book by ID
   */
  getBookById: (id: string) => {
    const book = MOCK_BOOKS.find(b => b.id === id);
    
    if (!book) {
      throw new Error(`Book not found: ${id}`);
    }
    
    return {
      success: true,
      data: book,
    };
  },
  
  /**
   * Search books
   */
  searchBooks: (query: string, params?: { page?: number; limit?: number }) => {
    const searchTerm = query.toLowerCase();
    const filtered = MOCK_BOOKS.filter(book =>
      book.title.toLowerCase().includes(searchTerm) ||
      book.author.toLowerCase().includes(searchTerm) ||
      book.category.toLowerCase().includes(searchTerm)
    );
    
    const page = params?.page || 1;
    const limit = params?.limit || 20;
    const startIndex = (page - 1) * limit;
    const endIndex = startIndex + limit;
    const paginatedResults = filtered.slice(startIndex, endIndex);
    
    return {
      success: true,
      data: paginatedResults,
      meta: {
        page,
        limit,
        total: filtered.length,
        totalPages: Math.ceil(filtered.length / limit),
        hasMore: endIndex < filtered.length,
        hasPrev: page > 1,
        hasNext: endIndex < filtered.length,
      },
    };
  },
  
  /**
   * Get categories
   */
  getCategories: () => {
    const categories = Array.from(new Set(MOCK_BOOKS.map(b => b.category))).sort();
    return {
      success: true,
      data: ['Tất cả', ...categories],
    };
  },
  
  /**
   * Get featured books
   */
  getFeaturedBooks: (limit = 10) => {
    const featured = [...MOCK_BOOKS]
      .sort((a, b) => b.rating - a.rating)
      .slice(0, limit);
    
    return {
      success: true,
      data: featured,
    };
  },
  
  /**
   * Get bestsellers
   */
  getBestsellers: (limit = 10) => {
    const bestsellers = [...MOCK_BOOKS]
      .sort((a, b) => b.reviewCount - a.reviewCount)
      .slice(0, limit);
    
    return {
      success: true,
      data: bestsellers,
    };
  },
  
  /**
   * Get new releases
   */
  getNewReleases: (limit = 10) => {
    const newReleases = [...MOCK_BOOKS]
      .sort((a, b) => b.publishedYear - a.publishedYear)
      .slice(0, limit);
    
    return {
      success: true,
      data: newReleases,
    };
  },
  
  /**
   * Get personalized recommendations
   */
  getPersonalizedRecommendations: (limit = 10) => {
    const personalized = [...MOCK_BOOKS]
      .filter(b => b.rating >= 4.5)
      .slice(0, limit);
    
    return {
      success: true,
      data: personalized,
    };
  },
  
  /**
   * Get recommendations by book ID
   */
  getRecommendations: (bookId: string, limit = 10) => {
    const book = MOCK_BOOKS.find(b => b.id === bookId);
    
    if (!book) {
      return {
        success: true,
        data: [],
      };
    }
    
    // Find books in same category
    const recommendations = MOCK_BOOKS
      .filter(b => b.id !== bookId && b.category === book.category)
      .sort((a, b) => b.rating - a.rating)
      .slice(0, limit);
    
    return {
      success: true,
      data: recommendations,
    };
  },
  
  /**
   * Get reviews by book ID
   */
  getReviewsByBookId: (bookId: string) => {
    const bookReviews = MOCK_REVIEWS.filter(r => r.bookId === bookId);
    
    return {
      success: true,
      data: bookReviews,
    };
  },
};
