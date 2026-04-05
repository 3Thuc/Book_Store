import React, { useState, useRef } from 'react';
import { Image, X, Upload, Loader } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import './ImageSearchPanel.css';
import { pythonApiClient } from '../lib/python-api-client';
import { PYTHON_API_ENDPOINTS } from '../lib/python-constants';

interface SearchResult {
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

interface SearchResponse {
  success: boolean;
  total: number;
  count: number;
  books: SearchResult[];
  took_ms: number;
}

export const ImageSearchPanel: React.FC = () => {
  const [isOpen, setIsOpen] = useState(false);
  const [selectedImage, setSelectedImage] = useState<File | null>(null);
  const [previewUrl, setPreviewUrl] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const navigate = useNavigate();

  const handleImageSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const allowedTypes = ['image/jpeg', 'image/png', 'image/webp', 'image/gif'];
    if (!allowedTypes.includes(file.type)) {
      setError('❌ Chỉ hỗ trợ JPG, PNG, WebP, GIF');
      return;
    }
    if (file.size > 10 * 1024 * 1024) {
      setError('❌ File quá lớn (max: 10MB)');
      return;
    }

    setSelectedImage(file);
    setError(null);

    const reader = new FileReader();
    reader.onload = (e) => setPreviewUrl(e.target?.result as string);
    reader.readAsDataURL(file);
  };

  const handleSearch = async () => {
    if (!selectedImage) {
      setError('❌ Vui lòng chọn ảnh');
      return;
    }

    setIsLoading(true);
    setError(null);

    try {
      const formData = new FormData();
      formData.append('image', selectedImage);
      formData.append('k', '20');

      const res = await pythonApiClient.post(
        PYTHON_API_ENDPOINTS.SEARCH.SEARCH_BY_IMAGE,
        formData,
        { headers: { 'Content-Type': 'multipart/form-data' } }
      );

      const data: SearchResponse = res.data as SearchResponse;

      // Map image search results to the same Book format SearchResultsPage expects
      const mappedBooks = data.books.map((item) => ({
        bookId: item.book_id,
        id: item.book_id,
        title: item.title || '',
        authorName: item.author || 'Unknown',
        author: item.author || 'Unknown',
        price: item.price || 0,
        avgRating: item.rating || 0,
        rating: item.rating || 0,
        ratingCount: 0,
        description: '',
        imageUrl: item.image_url || '',
        categories: [{ categoryId: 0, categoryName: item.category || 'Khác' }],
        category: item.category || 'Khác',
        stockQuantity: item.in_stock ? 1 : 0,
        _score: item.similarity_score,
        similarity_percentage: item.similarity_percentage,
      }));

      // Close modal and navigate to search results page with image search data
      handleReset();
      setIsOpen(false);

      navigate('/search/image-results', {
        state: {
          imageSearchResults: mappedBooks,
          total: data.total,
          fromImageSearch: true,
          previewUrl,  // pass preview so page can show which image was searched
        },
      });

    } catch (err) {
      setError(`❌ ${err instanceof Error ? err.message : 'Lỗi server'}`);
    } finally {
      setIsLoading(false);
    }
  };

  const handleReset = () => {
    setSelectedImage(null);
    setPreviewUrl(null);
    setError(null);
  };

  return (
    <>
      {/* Image Search Icon */}
      <button
        onClick={() => setIsOpen(true)}
        className="image-search-icon-btn"
        title="Tìm kiếm theo ảnh"
      >
        <Image size={20} />
      </button>

      {/* Upload Modal */}
      {isOpen && (
        <div className="image-search-modal-overlay" onClick={() => { setIsOpen(false); handleReset(); }}>
          <div className="image-search-modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>🖼️ Tìm Kiếm Theo Ảnh</h2>
              <button className="close-btn" onClick={() => { setIsOpen(false); handleReset(); }}>
                <X size={24} />
              </button>
            </div>

            <div className="modal-body">
              {!previewUrl ? (
                <div className="upload-area">
                  <div className="upload-box" onClick={() => fileInputRef.current?.click()}>
                    <Upload size={48} className="upload-icon" />
                    <h3>Chọn ảnh bìa sách</h3>
                    <p>Kéo thả hoặc click để chọn file</p>
                    <span className="file-info">JPG, PNG, WebP, GIF (max 10MB)</span>
                  </div>
                  <input
                    ref={fileInputRef}
                    type="file"
                    accept="image/*"
                    onChange={handleImageSelect}
                    style={{ display: 'none' }}
                  />
                </div>
              ) : (
                <div className="preview-area">
                  <img src={previewUrl} alt="Preview" className="preview-image" />
                  <div className="preview-info">
                    <p className="filename">📄 {selectedImage?.name}</p>
                    <p className="filesize">
                      Kích thước: {((selectedImage?.size || 0) / 1024 / 1024).toFixed(2)}MB
                    </p>
                    <button className="change-btn" onClick={() => fileInputRef.current?.click()}>
                      Chọn ảnh khác
                    </button>
                  </div>
                  <input
                    ref={fileInputRef}
                    type="file"
                    accept="image/*"
                    onChange={handleImageSelect}
                    style={{ display: 'none' }}
                  />
                </div>
              )}

              {error && <div className="error-message">{error}</div>}
            </div>

            <div className="modal-footer">
              <button className="btn-cancel" onClick={() => { setIsOpen(false); handleReset(); }}>
                Hủy
              </button>
              <button
                className="btn-search"
                onClick={handleSearch}
                disabled={!selectedImage || isLoading}
              >
                {isLoading ? (
                  <><Loader size={18} className="spinner" /> Đang tìm kiếm...</>
                ) : (
                  <><Image size={18} /> Tìm Kiếm</>
                )}
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
};

export default ImageSearchPanel;