/**
 * useOCR.ts – Custom hook quản lý toàn bộ OCR flow
 * ==================================================
 * Cung cấp:
 *   - state: isLoading, error, result, selectedFile, isModalOpen
 *   - actions: handleFileSelected, closeModal, resetOCR
 *
 * Dùng ở bất kỳ component nào cần OCR search-by-cover.
 */
import { useState, useCallback } from 'react';
import { OcrService, OcrResponse } from '../../services/ocrService';

export interface UseOCRReturn {
  isLoading: boolean;
  error: string | null;
  result: OcrResponse | null;
  selectedFile: File | null;
  isModalOpen: boolean;
  handleFileSelected: (file: File) => void;
  closeModal: () => void;
  resetOCR: () => void;
}

export function useOCR(): UseOCRReturn {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<OcrResponse | null>(null);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);

  /**
   * 1. User chọn file → mở modal ngay
   * 2. Gọi OCR API trong background (modal đã mở, hiện loading)
   * 3. Nhận kết quả → cập nhật state
   */
  const handleFileSelected = useCallback(async (file: File) => {
    setSelectedFile(file);
    setIsModalOpen(true);
    setIsLoading(true);
    setError(null);
    setResult(null);

    try {
      const response = await OcrService.searchByCover(file);

      if (!response.success && response.error) {
        setError(response.error);
      } else {
        setResult(response);
      }
    } catch (err: any) {
      // Phân loại lỗi để thông báo rõ ràng hơn
      if (err.code === 'ECONNREFUSED' || err.code === 'ERR_NETWORK') {
        setError('OCR Service chưa khởi động. Vui lòng kiểm tra Docker container bookstore_ocr.');
      } else if (err.response?.status === 413) {
        setError('Ảnh quá lớn (tối đa 10MB). Hãy nén ảnh và thử lại.');
      } else if (err.response?.status === 415) {
        setError('Định dạng file không hỗ trợ. Vui lòng dùng JPEG, PNG hoặc WebP.');
      } else if (err.code === 'ECONNABORTED') {
        setError('Hết thời gian chờ (60s). Ảnh quá phức tạp hoặc server đang bận.');
      } else {
        setError(err.response?.data?.detail || err.message || 'Lỗi không xác định từ OCR service.');
      }
    } finally {
      setIsLoading(false);
    }
  }, []);

  const closeModal = useCallback(() => {
    setIsModalOpen(false);
  }, []);

  const resetOCR = useCallback(() => {
    setIsModalOpen(false);
    setIsLoading(false);
    setError(null);
    setResult(null);
    setSelectedFile(null);
  }, []);

  return {
    isLoading,
    error,
    result,
    selectedFile,
    isModalOpen,
    handleFileSelected,
    closeModal,
    resetOCR,
  };
}
