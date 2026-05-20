/**
 * OCRUploadButton.tsx – Pill button camera/upload ảnh
 * ====================================================
 * Design: Pill button border với icon camera + label ngắn.
 * Đứng độc lập ngoài search bar → không overlap input placeholder.
 *
 * Props:
 *   compact=true  → chỉ hiện icon (dùng trong Admin form)
 *   compact=false → icon + "Tìm bằng ảnh" (dùng trong Header)
 */
import React, { useRef, useState } from 'react';
import { Camera, Loader2 } from 'lucide-react';

interface OCRUploadButtonProps {
  /** Callback khi user chọn file (trước khi gọi API) */
  onFileSelected: (file: File) => void;
  /** Đang xử lý OCR → hiển thị spinner */
  isLoading?: boolean;
  /** Tooltip text */
  title?: string;
  className?: string;
  /** true = chỉ icon (compact cho Admin form), false = icon + label */
  compact?: boolean;
  /** Text hiển thị (mặc định: "Tìm bằng ảnh") */
  label?: string;
}

export const OCRUploadButton: React.FC<OCRUploadButtonProps> = ({
  onFileSelected,
  isLoading = false,
  title = 'Tìm sách bằng ảnh bìa',
  className = '',
  compact = false,
  label = 'Tìm bằng ảnh',
}) => {
  const inputRef = useRef<HTMLInputElement>(null);
  const [isDragging, setIsDragging] = useState(false);

  const handleClick = () => {
    if (!isLoading) inputRef.current?.click();
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      onFileSelected(file);
      // Reset để có thể chọn lại cùng file
      e.target.value = '';
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    const file = e.dataTransfer.files?.[0];
    if (file && file.type.startsWith('image/')) onFileSelected(file);
  };

  return (
    <>
      {/* Hidden file input */}
      <input
        ref={inputRef}
        id="ocr-file-input"
        type="file"
        accept="image/jpeg,image/jpg,image/png,image/webp,image/bmp"
        onChange={handleFileChange}
        className="hidden"
        aria-label="Chọn ảnh bìa sách để tìm kiếm"
      />

      {/* Pill button */}
      <button
        id="ocr-upload-btn"
        onClick={handleClick}
        onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
        onDragLeave={() => setIsDragging(false)}
        onDrop={handleDrop}
        type="button"
        title={title}
        aria-label={title}
        disabled={isLoading}
        className={[
          // Base layout
          'inline-flex items-center justify-center gap-1.5',
          compact ? 'h-8 w-8 rounded-full px-0' : 'h-9 rounded-full px-3',
          // Typography
          'text-xs font-medium whitespace-nowrap select-none',
          // Border & background
          'border transition-all duration-200 ease-out outline-none',
          // States
          isDragging
            ? 'border-violet-400 bg-violet-50 dark:bg-violet-900/20 text-violet-600 scale-105'
            : 'border-border bg-background text-muted-foreground hover:border-violet-400 hover:bg-violet-50 dark:hover:bg-violet-900/20 hover:text-violet-600 dark:hover:text-violet-400',
          isLoading
            ? 'cursor-not-allowed opacity-60'
            : 'cursor-pointer hover:scale-[1.02] active:scale-95',
          className,
        ].join(' ')}
      >
        {isLoading ? (
          <Loader2 className="h-3.5 w-3.5 animate-spin flex-shrink-0" aria-hidden="true" />
        ) : (
          <Camera className="h-3.5 w-3.5 flex-shrink-0" aria-hidden="true" />
        )}

        {/* Label – ẩn khi compact=true */}
        {!compact && (
          <span>
            {isLoading ? 'Đang xử lý...' : label}
          </span>
        )}
      </button>
    </>
  );
};

export default OCRUploadButton;
