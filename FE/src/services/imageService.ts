import { apiRequest } from '../lib/api-client';
import { API_ENDPOINTS } from '../lib/constants';

/**
 * Image Service - Handle MinIO presigned URLs
 */
export class ImageService {
  /**
   * Get presigned URL for an image path from MinIO via Spring Boot
   * @param imagePath - Relative path like "covers/books/3080/3080.jpg"
   * @returns Presigned URL that's valid for 7 days
   */
  static async getPresignedUrl(imagePath: string): Promise<string> {
    try {
      // If already a full URL, return as is
      if (imagePath.startsWith('http')) {
        return imagePath;
      }

      // Call Spring Boot to get presigned URL from MinIO
      const response = await apiRequest.get<{ url: string }>(`/img/presign`, {
        params: { path: imagePath }
      });
      
      return response.url;
    } catch (error) {
      console.error('Failed to get presigned URL:', error);
      // Fallback to direct path
      return `https://localhost:8443/${imagePath}`;
    }
  }

  /**
   * Batch get presigned URLs for multiple image paths
   */
  static async getPresignedUrls(imagePaths: string[]): Promise<Record<string, string>> {
    try {
      const response = await apiRequest.post<Record<string, string>>(`/img/presign-batch`, {
        paths: imagePaths
      });
      
      return response;
    } catch (error) {
      console.error('Failed to get presigned URLs:', error);
      // Fallback to creating direct paths
      const fallback: Record<string, string> = {};
      imagePaths.forEach(path => {
        fallback[path] = path.startsWith('http') ? path : `https://localhost:8443/${path}`;
      });
      return fallback;
    }
  }
}
