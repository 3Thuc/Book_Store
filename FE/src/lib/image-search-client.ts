/**
 * Image Search API Client
 * Handles communication with the backend image search service
 */

import { API_BASE_URL } from './constants';

export interface ImageSearchResult {
  book_id: string;
  title: string;
  author: string;
  price: number;
  category: string;
  image_url: string;
  rating: number;
  in_stock: boolean;
  similarity_score: number;
  similarity_percentage: number;
}

export interface ImageSearchResponse {
  success: boolean;
  total: number;
  count: number;
  books: ImageSearchResult[];
  took_ms: number;
}

interface ImageSearchFilters {
  category?: string;
  in_stock?: boolean;
  min_price?: number;
  max_price?: number;
}

/**
 * Search for similar books using an image
 * @param imageFile - The image file to search with
 * @param k - Number of results to return (default: 20)
 * @param filters - Optional filters
 * @returns Promise with search results
 */
export async function searchBooksByImage(
  imageFile: File,
  k: number = 20,
  filters?: ImageSearchFilters
): Promise<ImageSearchResponse> {
  try {
    const formData = new FormData();
    formData.append('image', imageFile);
    formData.append('k', k.toString());

    if (filters?.category) {
      formData.append('category', filters.category);
    }
    if (filters?.in_stock) {
      formData.append('in_stock', 'true');
    }
    if (filters?.min_price !== undefined) {
      formData.append('min_price', filters.min_price.toString());
    }
    if (filters?.max_price !== undefined) {
      formData.append('max_price', filters.max_price.toString());
    }

    const response = await fetch(`${API_BASE_URL}/api/books/search-by-image`, {
      method: 'POST',
      body: formData,
      headers: {
        // Don't set Content-Type - let browser handle multipart
      },
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || 'Image search failed');
    }

    return await response.json();
  } catch (error) {
    console.error('❌ Image search error:', error);
    throw error;
  }
}

/**
 * Check health of image search service
 * @returns Promise with health status
 */
export async function checkImageSearchHealth(): Promise<{
  status: string;
  clip_model: string;
  device: string;
  image_embedding_dimension: number;
}> {
  try {
    const response = await fetch(`${API_BASE_URL}/api/books/search-by-image/health`);

    if (!response.ok) {
      throw new Error(`Health check failed: ${response.statusText}`);
    }

    return await response.json();
  } catch (error) {
    console.error('❌ Health check error:', error);
    throw error;
  }
}
