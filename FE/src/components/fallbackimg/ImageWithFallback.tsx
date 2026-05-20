import React, { useState } from 'react'

const ERROR_IMG_SRC =
  'data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iODgiIGhlaWdodD0iODgiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgc3Ryb2tlPSIjMDAwIiBzdHJva2UtbGluZWpvaW49InJvdW5kIiBvcGFjaXR5PSIuMyIgZmlsbD0ibm9uZSIgc3Ryb2tlLXdpZHRoPSIzLjciPjxyZWN0IHg9IjE2IiB5PSIxNiIgd2lkdGg9IjU2IiBoZWlnaHQ9IjU2IiByeD0iNiIvPjxwYXRoIGQ9Im0xNiA1OCAxNi0xOCAzMiAzMiIvPjxjaXJjbGUgY3g9IjUzIiBjeT0iMzUiIHI9IjciLz48L3N2Zz4KCg=='

/**
 * Trích xuất object-key (relative path) từ bất kỳ dạng URL MinIO nào:
 *  - http://localhost:9000/bookstore/covers/books/1/1.jpg?X-Amz...
 *  - /minio/bookstore/covers/books/1/1.jpg?X-Amz...
 */
function extractObjectPath(src: string): string | null {
  const marker = '/bookstore/'
  const idx = src.indexOf(marker)
  if (idx === -1) return null
  return src.slice(idx + marker.length).split('?')[0] // e.g. covers/books/1/1.jpg
}

export function ImageWithFallback(
  props: React.ImgHTMLAttributes<HTMLImageElement> & {
    onFinalError?: () => void
    /** Đặt true cho 4-5 ảnh đầu tiên mỗi trang để browser ưu tiên load */
    priority?: boolean
  }
) {
  const { src, alt, style, className, onFinalError, priority, loading, ...rest } = props
  const [didError, setDidError] = useState(false)
  const [retrySrc, setRetrySrc] = useState<string | undefined>(src)
  const [retryAttempted, setRetryAttempted] = useState(false)
  // Skeleton: isLoaded = false cho đến khi ảnh render xong
  const [isLoaded, setIsLoaded] = useState(false)
  const imgRef = React.useRef<HTMLImageElement>(null)

  // Reset state nếu src prop thay đổi (ví dụ: pagination sang trang mới)
  React.useEffect(() => {
    setRetrySrc(src)
    setDidError(false)
    setRetryAttempted(false)
    setIsLoaded(false)
  }, [src])

  // Kiểm tra ảnh đã được cache chưa (tránh lỗi browser cache không trigger onLoad)
  React.useEffect(() => {
    if (imgRef.current?.complete) {
      setIsLoaded(true)
    }
  }, [retrySrc, src])

  const handleError = async () => {
    // Chỉ thử recover 1 lần mỗi src
    if (retryAttempted || !src) {
      setDidError(true)
      return
    }
    setRetryAttempted(true)

    // Detect MinIO image (direct hoặc proxied)
    const isMinioUrl =
      src.includes('localhost:9000') ||
      src.startsWith('/minio/')

    if (isMinioUrl) {
      const objectPath = extractObjectPath(src)
      if (objectPath) {
        // [FAST PATH] Thay vì gọi presign API, serve trực tiếp qua proxy public bucket.
        // Tránh round-trip HTTP để lấy presigned URL mới → giảm latency per image.
        const directProxyUrl = `/minio/bookstore/${objectPath}`
        if (directProxyUrl !== retrySrc) {
          setRetrySrc(directProxyUrl)
          return
        }
      }
    }

    setDidError(true)
    onFinalError?.()
  }

  if (didError) {
    return (
      <div
        className={`inline-block bg-gray-100 dark:bg-gray-800 text-center align-middle ${className ?? ''}`}
        style={style}
      >
        <div className="flex items-center justify-center w-full h-full">
          <img src={ERROR_IMG_SRC} alt="Error loading image" {...rest} data-original-url={src} />
        </div>
      </div>
    )
  }

  return (
    <div className={`relative ${className ?? ''}`} style={style}>
      {/* Skeleton shimmer – hiện khi ảnh chưa load xong */}
      {!isLoaded && (
        <div
          className="absolute inset-0 bg-gray-200 dark:bg-gray-700 animate-pulse rounded"
          aria-hidden="true"
        />
      )}
      <img
        ref={imgRef}
        src={retrySrc || src}
        alt={alt}
        className={`w-full h-full object-cover transition-opacity duration-300 ${isLoaded ? 'opacity-100' : 'opacity-0'}`}
        // lazy cho ảnh thường, eager cho ảnh ưu tiên (đầu trang)
        loading={priority ? 'eager' : (loading ?? 'lazy')}
        // @ts-expect-error React 18.2 might not recognize fetchPriority in types, but DOM needs lowercase
        fetchpriority={priority ? 'high' : 'low'}
        decoding={priority ? 'sync' : 'async'}
        onLoad={() => setIsLoaded(true)}
        onError={handleError}
        {...rest}
      />
    </div>
  )
}
