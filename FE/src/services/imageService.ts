/**
 * imageService.ts — Dựng URL ảnh bìa (MinIO) chạy được cả DEV lẫn PRODUCTION (Vercel).
 *
 * Trước đây dùng đường proxy Vite '/minio' + '/bookdb' → CHỈ chạy local, lên Vercel bị 404.
 * Nay dựng URL public trực tiếp từ MinIO (bucket 'bookstore' đang public, đọc ẩn danh được).
 *
 * Cấu hình:
 *   - Production (Vercel): đặt biến env  VITE_MINIO_BASE_URL = https://minio.datateam.space
 *     (hoặc URL tunnel MinIO hiện tại). Vite nhúng biến này lúc build → nhớ REDEPLOY sau khi đổi.
 *   - Dev local: để trống → tự dùng proxy Vite '/minio'.
 */

// Bỏ dấu '/' cuối nếu có. Dev fallback = '/minio' (proxy Vite).
const MINIO_BASE = (((import.meta as any).env?.VITE_MINIO_BASE_URL as string) || '/minio').replace(/\/+$/, '');

/** "covers/books/1/1.jpg" → `${MINIO_BASE}/bookstore/covers/books/1/1.jpg` */
function buildPublicUrl(path: string): string {
  const clean = path.replace(/^\/+/, '');
  return `${MINIO_BASE}/bookstore/${clean}`;
}

/**
 * Chuẩn hoá MỌI dạng URL/đường dẫn ảnh MinIO về MINIO_BASE.
 * Nhận: URL đầy đủ (localhost:9000 / IP:9000 / domain), hoặc path tương đối "covers/...".
 */
export function toProxiedUrl(minioUrl: string): string {
  if (!minioUrl) return '';
  // Đã đúng base (public hoặc proxy dev) → giữ nguyên
  if (minioUrl.startsWith(MINIO_BASE) || minioUrl.startsWith('/minio')) return minioUrl;

  try {
    const url = new URL(minioUrl);
    // URL MinIO trực tiếp: lấy phần sau '/bookstore/' rồi dựng lại theo MINIO_BASE
    const m = url.pathname.match(/\/bookstore\/(.+)$/);
    if (m) return buildPublicUrl(m[1]) + url.search;
    // URL ngoài khác (không phải MinIO) → giữ nguyên
    return minioUrl;
  } catch {
    // Không parse được = path tương đối → dựng URL public
    return buildPublicUrl(minioUrl);
  }
}

/**
 * Image Service – bucket public nên KHÔNG cần presign; dựng URL trực tiếp (chạy trên Vercel).
 * Giữ nguyên chữ ký async để không phải sửa nơi gọi.
 */
export class ImageService {
  /** path tương đối "covers/books/3080/3080.jpg" → URL public đầy đủ. */
  static async getPresignedUrl(imagePath: string): Promise<string> {
    if (!imagePath) return '';
    if (
      imagePath.startsWith('http') ||
      imagePath.startsWith('/minio') ||
      imagePath.startsWith(MINIO_BASE)
    ) {
      return toProxiedUrl(imagePath);
    }
    return buildPublicUrl(imagePath);
  }

  /** Batch — trả map path → URL public. */
  static async getPresignedUrls(
    imagePaths: string[]
  ): Promise<Record<string, string>> {
    const result: Record<string, string> = {};
    for (const p of imagePaths) {
      result[p] =
        p.startsWith('http') || p.startsWith('/minio') || p.startsWith(MINIO_BASE)
          ? toProxiedUrl(p)
          : buildPublicUrl(p);
    }
    return result;
  }
}
