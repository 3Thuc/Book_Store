import { API_ENDPOINTS } from '../lib/constants';

/**
 * Rewrite a MinIO presigned URL to go through the Vite proxy.
 *
 * MinIO returns: http://localhost:9000/bookstore/covers/...?X-Amz-...
 * Vite proxy:   /minio/bookstore/covers/...?X-Amz-...   (→ http://localhost:9000/bookstore/...)
 *
 * This avoids mixed-content and cross-origin errors in the browser.
 */
/** Exported: rewrite any localhost:9000 MinIO URL → /minio/... proxy path */
export function toProxiedUrl(minioUrl: string): string {
  if (!minioUrl) return '';
  // Already a proxied path – return as-is
  if (minioUrl.startsWith('/minio')) return minioUrl;

  try {
    const url = new URL(minioUrl);
    // Only rewrite localhost:9000 MinIO URLs
    if (url.hostname === 'localhost' && url.port === '9000') {
      // pathname = /bookstore/covers/... → /minio/bookstore/covers/...
      return `/minio${url.pathname}${url.search}`;
    }
  } catch {
    // Relative path fallback (e.g. "covers/books/123/123.jpg")
    // Use the proxy directly since the myminio/bookstore bucket is public now
    if (minioUrl.startsWith('covers/') || minioUrl.startsWith('authors/') || minioUrl.startsWith('categories/')) {
      return `/minio/bookstore/${minioUrl}`;
    }
    // Not a valid URL – return unchanged
  }
  return minioUrl;
}

/**
 * Image Service – request presigned URLs from the Spring Boot backend
 * then rewrite them to go through the Vite proxy so the browser
 * never has to reach localhost:9000 or localhost:8443 directly.
 */
export class ImageService {
  /**
   * Get presigned URL for a single image path.
   * @param imagePath - Relative path like "covers/books/3080/3080.jpg"
   * @returns Proxied URL valid for up to 7 days
   */
  static async getPresignedUrl(imagePath: string): Promise<string> {
    try {
      if (imagePath.startsWith('http') || imagePath.startsWith('/minio')) {
        return toProxiedUrl(imagePath);
      }

      const params = new URLSearchParams({ path: imagePath });
      const res = await fetch(`/bookdb/img/presign?${params.toString()}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data: { url?: string } = await res.json();
      return data.url ? toProxiedUrl(data.url) : `/minio/bookstore/${imagePath}`;
    } catch {
      // Last-resort fallback: serve directly through proxy without presigning
      return `/minio/bookstore/${imagePath}`;
    }
  }

  /**
   * Batch-get presigned URLs for multiple image paths.
   * @param imagePaths - Array of relative paths
   * @returns Map of path → proxied URL
   */
  static async getPresignedUrls(
    imagePaths: string[]
  ): Promise<Record<string, string>> {
    if (imagePaths.length === 0) return {};

    try {
      const res = await fetch('/bookdb/img/presign-batch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ paths: imagePaths }),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data: Record<string, string | null> = await res.json();

      const result: Record<string, string> = {};
      imagePaths.forEach((p) => {
        const minioUrl = data[p];
        result[p] = minioUrl
          ? toProxiedUrl(minioUrl)
          : `/minio/bookstore/${p}`;
      });
      return result;
    } catch {
      // Fallback: all images go directly through the MinIO proxy
      const fallback: Record<string, string> = {};
      imagePaths.forEach((p) => {
        fallback[p] = p.startsWith('http')
          ? toProxiedUrl(p)
          : `/minio/bookstore/${p}`;
      });
      return fallback;
    }
  }
}
