/**
 * OCRResultModal.tsx – Tìm sách bằng ảnh (Redesign v2)
 * ⚡ Premium UI: gradient header, book card với ảnh bìa lớn,
 *    badge confidence dynamic, animation mượt.
 *
 * v3 FIX: Dùng ReactDOM.createPortal → render thẳng vào document.body
 *   → Thoát khỏi stacking context của <header backdrop-blur>
 *   → position:fixed thực sự relative to viewport (không bị lệch)
 */
import React, { useEffect, useState } from 'react';
import ReactDOM from 'react-dom';
import {
  X, Search, BookOpen, AlertCircle, Loader2, Camera,
  ChevronRight, Sparkles, Zap, CheckCircle, ScanLine,
} from 'lucide-react';
import { OcrBookResult, OcrResponse, OcrService } from '../../services/ocrService';
import { ImageWithFallback } from '../fallbackimg/ImageWithFallback';
import { toProxiedUrl } from '../../services/imageService';

interface OCRResultModalProps {
  isOpen: boolean;
  onClose: () => void;
  imageFile: File | null;
  ocrResult: OcrResponse | null;
  isLoading: boolean;
  error: string | null;
  onSearchQuery?: (query: string) => void;
  /** Xem tất cả: truyền query + toàn bộ kết quả OCR đã có */
  onSearchWithResults?: (query: string, results: OcrBookResult[]) => void;
  onBookClick?: (bookId: number) => void;
  /** Gọi khi user muốn đổi ảnh khác – nhận File mới để xử lý lại OCR */
  onRetake?: (file: File) => void;
}

// ── Helpers ──────────────────────────────────────────────────────────
function isGarbledText(text: string): boolean {
  const t = text.trim();
  if (!t || t.length < 3) return true;
  const alpha = Array.from(t).filter(c => /[\wÀ-ɏḀ-ỿ]/u.test(c)).length;
  if (alpha / t.length < 0.50) return true;
  if (!(t.match(/[A-Za-zÀ-ɏḀ-ỿ]{2,}/gu) ?? []).length) return true;
  const special = Array.from(t).filter(c => /[=<>"'~`^*+|\\{}[\]@#$%&]/.test(c)).length;
  if (special > 3) return true;
  // Phát hiện OCR misread số: "IOÔ", "l00" trong cụm ngắn không phải từ thật
  const words = t.split(/\s+/);
  const suspicious = words.filter(w =>
    w.length >= 2 && w.length <= 5 &&
    /[IOl]/.test(w) && /[Ô0ỌQ]/.test(w) && !/[aeiouy]/i.test(w.replace(/[ÔỌ]/gi, ''))
  );
  if (suspicious.length > 0 && words.length <= 4) return true;
  return false;
}

/** Chấm điểm dòng text: ưu tiên tiếng Việt và độ dài */
function _scoreTextLine(line: string): number {
  if (!line || line.length < 3) return 0;
  const viChars = (line.match(/[àáảãạăắằẳẵặâấầẩẫậèéẻẽẹêếềểễệìíỉĩịòóỏõọôốồổỗộơớờởỡợùúủũụưứừửữựỳýỷỹỵđÀÁẢÃẠĂẮẰẲẴẶÂẤẦẨẪẬÈÉẺẼẸÊẾỀỂỄỆÌÍỈĨỊÒÓỎÕỌÔỐỒỔỖỘƠỚỜỞỠỢÙÚỦŨỤƯỨỪỬỮỰỲÝỶỸỴĐ]/g) ?? []).length;
  const wordCount = line.trim().split(/\s+/).filter(w => w.length >= 2).length;
  const viBonus = viChars > 2 ? 30 : (viChars > 0 ? 10 : 0);
  return wordCount * 10 + line.length + viBonus;
}

/** Lấy title hiển thị: ưu tiên title tiếng Việt dài nhất trong raw text */
function getDisplayTitle(info: OcrResponse['book_info'] | null | undefined, raw: string): string | null {
  const infoTitle = info?.title && !isGarbledText(info.title) ? info.title : null;
  // Tìm tất cả dòng hợp lệ, chọn dòng điểm cao nhất (tiếng Việt + dài)
  const candidates = raw.split(/[\n;]/).map(l => l.trim())
    .filter(l => l.length >= 4 && !isGarbledText(l));
  if (candidates.length === 0) return infoTitle;
  const best = candidates.reduce((b, c) => _scoreTextLine(c) > _scoreTextLine(b) ? c : b, candidates[0]);
  // Dùng info.title nếu tốt >= 80% best candidate
  if (infoTitle && _scoreTextLine(infoTitle) >= _scoreTextLine(best) * 0.8) return infoTitle;
  return (best ?? infoTitle)?.slice(0, 80) ?? null;
}

function buildQuery(info: OcrResponse['book_info'] | null | undefined, raw: string): string {
  const title = getDisplayTitle(info, raw);
  if (!title) return '';
  const author = info?.authors?.[0];
  return author ? `${title} ${author}` : title;
}



/** Làm sạch ký tự lỗi encoding trong tên sách từ DB (¿, η, ι, v.v.) */
function sanitizeBookTitle(title: string): string {
  if (!title) return title;
  // Xóa ký tự không phải Latin/Vietnamese/phổ biến
  // ¿ (U+00BF), ¡ (U+00A1), ký tự Hy Lạp từ sai encoding
  let cleaned = title
    .replace(/[\u00BF\u00A1]/g, '')          // ¿ ¡ → xóa
    .replace(/[\u0370-\u03FF]/g, '')          // Ký tự Hy Lạp (η, ι, ...) → xóa
    .replace(/[\uFFFD\u0000-\u0008\u000B-\u001F]/g, '') // replacement chars
    .replace(/\s{2,}/g, ' ')                 // chuẩn hóa khoảng trắng
    .trim();
  // Nếu sau khi clean còn lại quá ngắn (< 2 ký tự) → trả về gốc
  return cleaned.length >= 2 ? cleaned : title;
}

const fmtPrice = (p: number | null) =>
  p ? new Intl.NumberFormat('vi-VN', { style: 'currency', currency: 'VND' }).format(p) : null;

// ── Component ────────────────────────────────────────────────────────
export const OCRResultModal: React.FC<OCRResultModalProps> = ({
  isOpen, onClose, imageFile, ocrResult, isLoading, error,
  onSearchQuery, onSearchWithResults, onBookClick, onRetake,
}) => {
  const [preview, setPreview] = useState<string | null>(null);
  const [imgNatural, setImgNatural] = useState({ w: 0, h: 0 });
  const retakeInputRef = React.useRef<HTMLInputElement>(null);

  // States cho tính năng AI Review & Tóm tắt sách
  const [reviewOpen, setReviewOpen] = useState(false);
  const [reviewLoading, setReviewLoading] = useState(false);
  const [reviewData, setReviewData] = useState<{ summary: string; reasons: string[] } | null>(null);
  const [reviewError, setReviewError] = useState<string | null>(null);

  const handleOpenReview = async (title: string, author: string | null) => {
    setReviewOpen(true);
    setReviewLoading(true);
    setReviewError(null);
    setReviewData(null);
    try {
      const res = await OcrService.getBookReview(title, author || undefined);
      if (res.success) {
        setReviewData({ summary: res.summary, reasons: res.reasons });
      } else {
        setReviewError(res.error || 'Không thể tạo đánh giá cho sách.');
      }
    } catch (err: any) {
      setReviewError(err.message || 'Lỗi kết nối tới hệ thống AI.');
    } finally {
      setReviewLoading(false);
    }
  };


  useEffect(() => {
    if (!imageFile) { setPreview(null); return; }
    const url = URL.createObjectURL(imageFile);
    setPreview(url);
    return () => URL.revokeObjectURL(url);
  }, [imageFile]);

  useEffect(() => {
    if (!isOpen) return;
    const esc = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('keydown', esc);
    const prev = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => { document.removeEventListener('keydown', esc); document.body.style.overflow = prev; };
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  const info = ocrResult?.book_info;
  const results = ocrResult?.search_results ?? [];
  const rawText = ocrResult?.extracted_text ?? '';
  const engine = ocrResult?.engine_used ?? '';
  const matchMethod = (ocrResult as any)?.match_method ?? '';
  const confidence = info?.confidence ?? 0;

  // ✔️ Ưu tiên title từ DB chỉ khi kết quả THỰC SỰ đáng tin
  const topResult = results.length > 0 ? results[0] : null;
  const topScore = topResult?.score ?? 0;

  // v2: yêu cầu ≥2 từ có nghĩa trùng nhau HOẶC Jaccard ≥ 30%
  // Tránh false-positive khi chỉ có 1 từ chung như "sales", "marketing", "100"
  const _hasOverlap = (a: string, b: string): boolean => {
    if (!a || !b) return false;

    // Từ phổ biến không đủ để phân biệt sách (EN + VI thường gặp trên bìa)
    const STOPWORDS = new Set([
      'the', 'and', 'for', 'with', 'from', 'into', 'that', 'this', 'all', 'are', 'was',
      'hay', 'nhat', 'cua', 'cho', 'mot', 'trong', 'nguoi', 'khi', 'ban', 'cac', 'bai',
      'tat', 'cả', 'va', 've', 'co', 'la', 'sau', 'truoc', 'theo', 'dau', 'cuoc', 'nhung',
      'ideas', 'idea', 'stories', 'story', 'great', 'greatest', 'best', 'top', 'new',
      'kiem', 'toan', 'hoc', 'sach', 'viet', 'nam', 'khoa',
    ]);

    const normalize = (s: string) =>
      s.toLowerCase()
        // Xoá dấu tiếng Việt + Latin extended
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
        // Giữ chỉ chữ cái và số
        .replace(/[^a-z0-9\s]/g, '')
        .split(/\s+/)
        .filter(w => w.length >= 3 && !STOPWORDS.has(w) && !/^\d+$/.test(w));

    const wa = new Set(normalize(a));
    const wb = new Set(normalize(b));

    if (wa.size === 0 || wb.size === 0) return false;

    // Đếm từ chung có nghĩa
    let common = 0;
    for (const w of wb) {
      if (wa.has(w)) common++;
    }

    // Jaccard similarity = common / |union|
    const union = new Set([...wa, ...wb]).size;
    const jaccard = union > 0 ? common / union : 0;

    // Cần ≥2 từ chung HOẶC Jaccard ≥ 30%
    return common >= 2 || jaccard >= 0.30;
  };

  // is reliable nếu:
  // 1. Visual match (pHash) = chắc chắn 100%
  // 2. score > 50 (tăng từ 30 để tránh false-positive) VÀ ≥2 từ title trùng
  const ocrTitleRaw = info?.title ?? rawText;
  const dbTitleRaw = topResult?.title ?? '';
  const titleOverlap = _hasOverlap(ocrTitleRaw, dbTitleRaw) ||
    _hasOverlap(dbTitleRaw, ocrTitleRaw);

  const isReliableResult = matchMethod === 'visual_match' ||
    // Cần cả 3: score cao + title trùng + confidence đủ tốt
    // confidence < 0.35 ("Không chắc") → không bao giờ là reliable dù title có trùng
    (topResult !== null && topResult.book_id > 0 && topScore > 50 && titleOverlap && confidence >= 0.35);

  const dbTopTitle = isReliableResult && topResult ? sanitizeBookTitle(topResult.title) : null;
  const dbTopAuthor = isReliableResult && topResult ? topResult.author_name : null;
  const ocrTitle = getDisplayTitle(info, rawText);
  const displayTitle = dbTopTitle || ocrTitle;
  const displayAuthor = dbTopAuthor || (info?.authors?.[0] ?? null);

  // Tìm kiếm: ưu tiên title đáng tin từ DB
  const query = dbTopTitle
    ? (dbTopAuthor ? `${dbTopTitle} ${dbTopAuthor}` : dbTopTitle)
    : buildQuery(info, rawText);

  // "Xem tất cả" / nút tìm kiếm: chỉ dùng title từ DB khi kết quả ĐÁNG TIN
  // Bug cũ: luôn dùng topResult.title dù OCR đọc sai
  // VD: OCR đọc "Súg" → search → tìm "Nữ Hoàng Của King Coffee" → nút Tìm hiện sai title
  const viewAllQuery: string = (() => {
    // Chỉ dùng DB title khi kết quả THỰC SỰ đáng tin (có overlap giữa OCR và DB)
    if (isReliableResult && topResult?.title) return sanitizeBookTitle(topResult.title);
    // Fallback: dùng OCR title (không dùng DB title vì không đáng tin)
    if (ocrTitle && !isGarbledText(ocrTitle)) return ocrTitle;
    return buildQuery(info, rawText);
  })();

  // Phát hiện text OCR garbled nghiêm trọng (VD: "tte SS pen p23 ae", ". ae PGi K")
  // Khi garbled: ẩn danh sách sách không liên quan, hiện message thích hợp
  const isGarbledOCR = (() => {
    // OCR text rỗng hoặc quá ngắn
    const textToCheck = ocrTitle || rawText || '';
    if (!textToCheck.trim() || textToCheck.trim().length < 3) return true;
    // Text có nhiều ký tự đặc biệt/số không phải chữ
    const alphaRatio = textToCheck.replace(/[^A-Za-zÀ-ỹ]/g, '').length / textToCheck.length;
    if (alphaRatio < 0.4) return true;
    // Text chỉ có 1-2 từ và không có overlap với DB title
    const words = textToCheck.trim().split(/\s+/).filter(w => w.length >= 2);
    const hasRealWords = words.some(w =>
      w.length >= 4 || /[à-ỹ]/i.test(w)
    );
    if (!hasRealWords && !isReliableResult) return true;
    return false;
  })();

  // Kết quả đáng hiển thị:
  // - reliable match (overlap + score + confidence) → luôn hiện
  // - hoặc: OCR đọc được (không garbled) VÀ confidence đủ để search có ích
  const hasUsableResults = results.length > 0 && (
    isReliableResult ||
    (!isGarbledOCR && confidence >= 0.35)
  );

  // Badge
  const confBadge = confidence >= 0.75
    ? { label: 'Độ chính xác cao', color: '#10b981', bg: 'rgba(16,185,129,0.1)', border: 'rgba(16,185,129,0.3)' }
    : confidence >= 0.4
      ? { label: 'Có thể đúng', color: '#f59e0b', bg: 'rgba(245,158,11,0.1)', border: 'rgba(245,158,11,0.3)' }
      : { label: 'Không chắc', color: '#94a3b8', bg: 'rgba(148,163,184,0.1)', border: 'rgba(148,163,184,0.3)' };

  const showHighConf = isReliableResult;
  const isVisualMatch = matchMethod === 'visual_match';
  const processingMs = ocrResult?.processing_time_ms ?? 0;

  const goSearch = () => {
    const q = viewAllQuery || query;
    if (!q) return;
    // Nếu có callback cầy cấy cả results → truyền hết 15 sách đã có
    if (onSearchWithResults && results.length > 0) {
      onSearchWithResults(q, results);
    } else if (onSearchQuery) {
      onSearchQuery(q);
    }
    onClose();
  };
  const goBook = (b: OcrBookResult) => { if (b.book_id > 0 && onBookClick) { onBookClick(b.book_id); onClose(); } };

  // Handler nút đổi ảnh
  const handleRetakeClick = () => retakeInputRef.current?.click();
  const handleRetakeChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0];
    if (f && onRetake) { onRetake(f); }
    if (retakeInputRef.current) retakeInputRef.current.value = '';
  };

  // Portal target – render thảng vào document.body
  // Tại sao cần portal?
  // - Header có `backdrop-blur` CSS tạo ra stacking context mới
  // - Con cái có position:fixed sẽ bị "fixed" relative to header, không phải viewport
  // - Portal render thẳng vào body → position:fixed luôn đúng với viewport
  const portalRoot = typeof document !== 'undefined' ? document.body : null;

  const modalContent = (
    <>
      {/* Backdrop */}
      <div
        onClick={onClose}
        style={{
          position: 'fixed', inset: 0, zIndex: 9998,
          background: 'rgba(0,0,0,0.65)',
          backdropFilter: 'blur(8px)', WebkitBackdropFilter: 'blur(8px)',
        }}
      />

      {/* Center wrapper */}
      <div style={{
        position: 'fixed', inset: 0, zIndex: 9999,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        padding: '16px', pointerEvents: 'none',
        fontFamily: "'Be Vietnam Pro', system-ui, sans-serif",
      }}>
        {/* Panel */}
        <div
          onClick={e => e.stopPropagation()}
          style={{
            pointerEvents: 'auto',
            width: '100%', maxWidth: '520px', maxHeight: '92vh',
            display: 'flex', flexDirection: 'column',
            background: 'linear-gradient(180deg, #ffffff 0%, #f8f9ff 100%)',
            borderRadius: '24px',
            boxShadow: '0 32px 80px rgba(0,0,0,0.22), 0 0 0 1px rgba(0,0,0,0.06)',
            overflow: 'hidden',
            animation: 'ocr-modal-in 0.22s cubic-bezier(0.34,1.56,0.64,1)',
          }}
        >
          <style>{`
            @keyframes ocr-modal-in {
              from { opacity: 0; transform: scale(0.93) translateY(12px); }
              to   { opacity: 1; transform: scale(1) translateY(0); }
            }
            @keyframes ocr-scan-line {
              0%   { top: 8%; }
              50%  { top: 88%; }
              100% { top: 8%; }
            }
            @keyframes ocr-pulse-ring {
              0%   { transform: scale(1);   opacity: 0.6; }
              100% { transform: scale(1.5); opacity: 0; }
            }
            @keyframes fade-in {
              from { opacity: 0; }
              to   { opacity: 1; }
            }
            @keyframes slide-up {
              from { transform: translateY(100%); }
              to   { transform: translateY(0); }
            }
            .ocr-book-row { transition: background 0.15s, transform 0.12s; }
            .ocr-book-row:hover { background: rgba(109,40,217,0.06); transform: translateX(2px); }
            .ocr-book-row:active { transform: scale(0.98); }
          `}</style>


          {/* ── HEADER ───────────────────────────────── */}
          <div style={{
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            padding: '14px 16px 12px',
            background: 'linear-gradient(135deg, #6d28d9 0%, #7c3aed 50%, #8b5cf6 100%)',
            flexShrink: 0,
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
              <div style={{
                width: 36, height: 36, borderRadius: 10,
                background: 'rgba(255,255,255,0.15)',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
              }}>
                <ScanLine size={18} color="white" />
              </div>
              <div>
                <p style={{ margin: 0, fontSize: 14, fontWeight: 700, color: 'white', lineHeight: 1.2 }}>
                  Tìm sách bằng ảnh
                </p>
                <p style={{ margin: 0, fontSize: 11, color: 'rgba(255,255,255,0.7)', lineHeight: 1.2, marginTop: 1 }}>
                  {isLoading ? 'Đang nhận diện...' : isVisualMatch ? '⚡ Visual Match' : engine ? `OCR · ${engine}` : 'Kết quả nhận diện'}
                </p>
              </div>
            </div>
            {/* Right side: nút Đổi ảnh + Đóng */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              {/* Nút Đổi ảnh — chỉ hiển thị khi có onRetake */}
              {onRetake && (
                <button
                  id="ocr-retake-btn"
                  onClick={handleRetakeClick}
                  title="Chọn ảnh khác"
                  style={{
                    display: 'flex', alignItems: 'center', gap: 5,
                    padding: '5px 10px', borderRadius: 20, border: 'none',
                    background: 'rgba(255,255,255,0.18)', cursor: 'pointer',
                    color: 'white', fontSize: 11, fontWeight: 600,
                    transition: 'background 0.15s',
                  }}
                  onMouseEnter={e => (e.currentTarget.style.background = 'rgba(255,255,255,0.28)')}
                  onMouseLeave={e => (e.currentTarget.style.background = 'rgba(255,255,255,0.18)')}
                >
                  <Camera size={12} />
                  Đổi ảnh
                </button>
              )}
              <button
                id="ocr-modal-close-btn"
                onClick={onClose}
                style={{
                  width: 30, height: 30, borderRadius: '50%', border: 'none',
                  background: 'rgba(255,255,255,0.15)', cursor: 'pointer',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  color: 'white', transition: 'background 0.15s',
                }}
                onMouseEnter={e => (e.currentTarget.style.background = 'rgba(255,255,255,0.25)')}
                onMouseLeave={e => (e.currentTarget.style.background = 'rgba(255,255,255,0.15)')}
                aria-label="Đóng"
              >
                <X size={14} />
              </button>
            </div>
          </div>

          {/* Hidden file input cho retake */}
          <input
            ref={retakeInputRef}
            type="file"
            accept="image/jpeg,image/png,image/webp,image/bmp"
            style={{ display: 'none' }}
            onChange={handleRetakeChange}
          />

          {/* ── IMAGE PREVIEW ────────────────────────── */}
          <div style={{
            position: 'relative', flexShrink: 0,
            background: 'linear-gradient(180deg, #1e1b4b 0%, #312e81 100%)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            minHeight: 160, maxHeight: 280, overflow: 'hidden',
          }}>
            {preview ? (
              <img
                src={preview}
                alt="Ảnh tải lên"
                onLoad={e => {
                  const img = e.target as HTMLImageElement;
                  setImgNatural({ w: img.naturalWidth, h: img.naturalHeight });
                }}
                style={{
                  maxWidth: '100%', maxHeight: 280,
                  objectFit: 'contain',
                  display: 'block',
                }}
              />
            ) : (
              <Camera size={40} color="rgba(255,255,255,0.2)" />
            )}

            {/* Scanning animation overlay */}
            {isLoading && preview && (
              <div style={{
                position: 'absolute', inset: 0,
                background: 'rgba(0,0,0,0.4)',
                backdropFilter: 'blur(1px)',
                display: 'flex', flexDirection: 'column',
                alignItems: 'center', justifyContent: 'center', gap: 12,
              }}>
                {/* Scan line */}
                <div style={{
                  position: 'absolute', left: '5%', right: '5%', height: 2,
                  background: 'linear-gradient(90deg, transparent, #a78bfa, #7c3aed, #a78bfa, transparent)',
                  animation: 'ocr-scan-line 1.8s ease-in-out infinite',
                  borderRadius: 2, boxShadow: '0 0 12px rgba(167,139,250,0.8)',
                }} />
                {/* Corner brackets */}
                {(['TL', 'TR', 'BL', 'BR'] as const).map(pos => (
                  <span key={pos} style={{
                    position: 'absolute',
                    top: pos.startsWith('T') ? 10 : undefined,
                    bottom: pos.startsWith('B') ? 10 : undefined,
                    left: pos.endsWith('L') ? 10 : undefined,
                    right: pos.endsWith('R') ? 10 : undefined,
                    width: 20, height: 20,
                    borderTop: pos.startsWith('T') ? '2px solid #a78bfa' : undefined,
                    borderBottom: pos.startsWith('B') ? '2px solid #a78bfa' : undefined,
                    borderLeft: pos.endsWith('L') ? '2px solid #a78bfa' : undefined,
                    borderRight: pos.endsWith('R') ? '2px solid #a78bfa' : undefined,
                  }} />
                ))}
                <div style={{
                  background: 'rgba(109,40,217,0.8)', borderRadius: 20,
                  padding: '6px 14px', display: 'flex', alignItems: 'center', gap: 8,
                  backdropFilter: 'blur(4px)',
                }}>
                  <Loader2 size={14} color="white" style={{ animation: 'spin 1s linear infinite' }} />
                  <span style={{ fontSize: 12, color: 'white', fontWeight: 600 }}>Đang phân tích...</span>
                </div>
              </div>
            )}

            {/* Visual match badge */}
            {!isLoading && isVisualMatch && (
              <div style={{
                position: 'absolute', top: 8, right: 8,
                background: 'rgba(16,185,129,0.9)', borderRadius: 20,
                padding: '4px 10px', display: 'flex', alignItems: 'center', gap: 5,
                backdropFilter: 'blur(4px)',
              }}>
                <Zap size={11} color="white" fill="white" />
                <span style={{ fontSize: 11, color: 'white', fontWeight: 700 }}>Nhận diện tức thì</span>
              </div>
            )}

            {/* Processing time chip */}
            {!isLoading && processingMs > 0 && (
              <div style={{
                position: 'absolute', bottom: 8, left: 8,
                background: 'rgba(0,0,0,0.5)', borderRadius: 20,
                padding: '3px 8px', backdropFilter: 'blur(4px)',
              }}>
                <span style={{ fontSize: 10, color: 'rgba(255,255,255,0.8)', fontFamily: 'monospace' }}>
                  {processingMs < 1000 ? `${processingMs}ms` : `${(processingMs / 1000).toFixed(1)}s`}
                </span>
              </div>
            )}
          </div>

          {/* ── CONTENT BODY (scrollable) ─────────────── */}
          <div style={{ overflowY: 'auto', flex: 1, minHeight: 0 }}>

            {/* Loading text */}
            {isLoading && (
              <div style={{ padding: '20px 16px', textAlign: 'center' }}>
                <p style={{ margin: 0, fontSize: 13, fontWeight: 600, color: '#374151' }}>
                  Đang nhận diện ảnh sách...
                </p>
                <p style={{ margin: '4px 0 0', fontSize: 11, color: '#9ca3af' }}>
                  Thường mất 3–8 giây
                </p>
              </div>
            )}

            {/* Error */}
            {!isLoading && error && (
              <div style={{ padding: '12px 16px' }}>
                <div style={{
                  display: 'flex', gap: 10, padding: '12px 14px',
                  borderRadius: 14, background: '#fef2f2',
                  border: '1px solid #fecaca',
                }}>
                  <AlertCircle size={15} color="#ef4444" style={{ flexShrink: 0, marginTop: 1 }} />
                  <div>
                    <p style={{ margin: 0, fontSize: 12, fontWeight: 700, color: '#b91c1c' }}>
                      Không nhận diện được ảnh
                    </p>
                    <p style={{ margin: '2px 0 0', fontSize: 11, color: '#6b7280', lineHeight: 1.4 }}>{error}</p>
                  </div>
                </div>
                <p style={{ textAlign: 'center', fontSize: 11, color: '#9ca3af', marginTop: 8 }}>
                  💡 Thử ảnh chụp thẳng góc, đủ ánh sáng, bìa sách rõ nét
                </p>
              </div>
            )}

            {/* ✅ Success */}
            {!isLoading && !error && ocrResult && (
              <>
                {/* Book info strip */}
                {displayTitle ? (
                  <div style={{
                    display: 'flex', alignItems: 'flex-start', gap: 10,
                    padding: '12px 16px', borderBottom: '1px solid #f0f0f8',
                  }}>
                    <div style={{
                      width: 28, height: 28, borderRadius: 8, flexShrink: 0,
                      background: 'linear-gradient(135deg, #ede9fe, #ddd6fe)',
                      display: 'flex', alignItems: 'center', justifyContent: 'center',
                      marginTop: 1,
                    }}>
                      <Sparkles size={13} color="#7c3aed" />
                    </div>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <p style={{
                        margin: 0, fontSize: 14, fontWeight: 700,
                        color: '#111827', lineHeight: 1.3,
                        display: '-webkit-box', WebkitLineClamp: 2,
                        WebkitBoxOrient: 'vertical', overflow: 'hidden',
                      }}>
                        {displayTitle}
                      </p>
                      {info?.authors && info.authors.length > 0 && (
                        <p style={{ margin: '2px 0 0', fontSize: 12, color: '#6b7280' }}>
                          {info.authors.join(' · ')}
                        </p>
                      )}
                      {info?.isbn && (
                        <p style={{ margin: '2px 0 0', fontSize: 10, color: '#9ca3af', fontFamily: 'monospace' }}>
                          ISBN {info.isbn}
                        </p>
                      )}
                      {displayTitle && (
                        <button
                          onClick={() => handleOpenReview(displayTitle, displayAuthor)}
                          style={{
                            marginTop: 8, display: 'flex', alignItems: 'center', gap: 4,
                            padding: '4px 10px', borderRadius: 12, border: 'none',
                            background: 'linear-gradient(135deg, #ede9fe 0%, #ddd6fe 100%)',
                            color: '#6d28d9', fontSize: 11, fontWeight: 700,
                            cursor: 'pointer', transition: 'all 0.15s',
                          }}
                        >
                          <Sparkles size={11} color="#6d28d9" fill="#6d28d9" />
                          AI Review & Tóm tắt ✨
                        </button>
                      )}
                    </div>
                    {confidence > 0 && (
                      <div style={{
                        flexShrink: 0, display: 'flex', alignItems: 'center', gap: 4,
                        padding: '4px 8px', borderRadius: 20,
                        background: confBadge.bg, border: `1px solid ${confBadge.border}`,
                      }}>
                        <CheckCircle size={10} color={confBadge.color} />
                        <span style={{ fontSize: 10, fontWeight: 700, color: confBadge.color, whiteSpace: 'nowrap' }}>
                          {confBadge.label}
                        </span>
                      </div>
                    )}
                  </div>
                ) : (
                  <div style={{
                    display: 'flex', alignItems: 'center', gap: 8,
                    padding: '10px 16px', borderBottom: '1px solid #f0f0f8',
                  }}>
                    <AlertCircle size={13} color="#f59e0b" />
                    <p style={{ margin: 0, fontSize: 12, color: '#6b7280' }}>
                      {isGarbledOCR
                        ? 'Ảnh khó đọc – thử chụp rõ hơn hoặc đổi góc'
                        : 'Chưa nhận diện được tiêu đề'}
                      {!isGarbledOCR && hasUsableResults && <span style={{ color: '#9ca3af' }}> · {results.length} gợi ý bên dưới</span>}
                    </p>
                  </div>
                )}

                {/* Results: chỉ hiện khi có kết quả đáng tin – ẩn khi OCR garbled */}
                {hasUsableResults ? (
                  <div>
                    {/* Section header */}
                    <div style={{
                      display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                      padding: '10px 16px 6px',
                    }}>
                      <span style={{
                        fontSize: 10, fontWeight: 800, color: '#9ca3af',
                        letterSpacing: '0.08em', textTransform: 'uppercase',
                      }}>
                        {isReliableResult ? `${results.length} SÁCH LIÊN QUAN` : 'KẾ́T QUẢ TÌM KIẾM'}
                      </span>
                      {(viewAllQuery || query) && onSearchQuery && (
                        <button
                          id="ocr-see-all-btn"
                          onClick={goSearch}
                          style={{
                            fontSize: 12, fontWeight: 700, color: '#7c3aed',
                            background: 'none', border: 'none', cursor: 'pointer',
                            padding: '2px 0', textDecoration: 'none',
                          }}
                          onMouseEnter={e => (e.currentTarget.style.textDecoration = 'underline')}
                          onMouseLeave={e => (e.currentTarget.style.textDecoration = 'none')}
                        >
                          Xem tất cả →
                        </button>
                      )}
                    </div>

                    {/* Book list */}
                    <div style={{ padding: '0 10px 8px' }}>
                      {results.slice(0, 4).map((b: OcrBookResult, idx) => (
                        <BookCard key={b.book_id > 0 ? b.book_id : `${b.title}-${idx}`}
                          book={b} rank={idx + 1}
                          onPress={b.book_id > 0 ? () => goBook(b) : undefined}
                        />
                      ))}
                    </div>
                  </div>
                ) : (
                  <div style={{ padding: '28px 16px', textAlign: 'center' }}>
                    <div style={{
                      width: 52, height: 52, borderRadius: '50%',
                      background: '#f5f3ff', margin: '0 auto 12px',
                      display: 'flex', alignItems: 'center', justifyContent: 'center',
                    }}>
                      <BookOpen size={22} color="#c4b5fd" />
                    </div>
                    <p style={{ margin: 0, fontSize: 13, fontWeight: 600, color: '#4b5563' }}>
                      Không tìm thấy sách phù hợp
                    </p>
                    <p style={{ margin: '4px 0 0', fontSize: 12, color: '#9ca3af' }}>
                      {isGarbledOCR
                        ? 'Ảnh khó đọc – thử chụp thẳng, rõ nét hơn'
                        : 'Thử chụp ảnh rõ hơn hoặc tìm thủ công'}
                    </p>
                  </div>
                )}
              </>
            )}
          </div>

          {/* ── FOOTER ───────────────────────────────── */}
          {!isLoading && (
            <div style={{
              flexShrink: 0, padding: '12px 14px 14px',
              borderTop: '1px solid #f0f0f8',
              background: 'white',
            }}>
              {/* Khi garbled OCR: không hiện nút Tìm với title sai, chỉ hiện Đóng */}
              {(viewAllQuery || query) && onSearchQuery && !isGarbledOCR ? (
                <button
                  id="ocr-search-btn"
                  onClick={goSearch}
                  style={{
                    width: '100%', display: 'flex', alignItems: 'center',
                    justifyContent: 'center', gap: 8,
                    padding: '12px 20px', borderRadius: 14, border: 'none',
                    background: 'linear-gradient(135deg, #6d28d9 0%, #7c3aed 100%)',
                    color: 'white', fontSize: 13, fontWeight: 700,
                    cursor: 'pointer', letterSpacing: '0.01em',
                    boxShadow: '0 4px 16px rgba(109,40,217,0.35)',
                    fontFamily: "'Be Vietnam Pro', sans-serif",
                    transition: 'transform 0.12s, box-shadow 0.12s',
                  }}
                  onMouseEnter={e => {
                    e.currentTarget.style.transform = 'translateY(-1px)';
                    e.currentTarget.style.boxShadow = '0 6px 20px rgba(109,40,217,0.45)';
                  }}
                  onMouseLeave={e => {
                    e.currentTarget.style.transform = 'translateY(0)';
                    e.currentTarget.style.boxShadow = '0 4px 16px rgba(109,40,217,0.35)';
                  }}
                >
                  <Search size={15} />
                  Tìm: &ldquo;{(viewAllQuery || query).slice(0, 32)}{(viewAllQuery || query).length > 32 ? '…' : ''}&rdquo;
                </button>
              ) : (
                <button
                  onClick={onClose}
                  style={{
                    width: '100%', padding: '12px', borderRadius: 14, border: 'none',
                    background: '#f5f3ff', color: '#7c3aed', fontSize: 13,
                    fontWeight: 700, cursor: 'pointer',
                    fontFamily: "'Be Vietnam Pro', sans-serif",
                  }}
                >
                  Đóng cửa sổ
                </button>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Review Drawer Overlay (WOW Feature) */}
      {reviewOpen && (
        <div style={{
          position: 'fixed', inset: 0, zIndex: 10000,
          background: 'rgba(0,0,0,0.5)', backdropFilter: 'blur(4px)',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          padding: '16px',
          animation: 'fade-in 0.2s ease-out',
        }}
          onClick={() => setReviewOpen(false)}
        >
          <div style={{
            width: '100%', maxWidth: '520px', background: 'white',
            borderRadius: 24,
            padding: '20px 16px 24px', boxShadow: '0 12px 48px rgba(0,0,0,0.25)',
            maxHeight: '85vh', display: 'flex', flexDirection: 'column',
            animation: 'fade-in 0.2s ease-out',
            fontFamily: "'Be Vietnam Pro', system-ui, sans-serif",
          }}
            onClick={e => e.stopPropagation()}
          >
            {/* Header Drawer */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <Sparkles size={16} color="#7c3aed" fill="#7c3aed" />
                <span style={{ fontSize: 15, fontWeight: 800, color: '#111827' }}>AI Review & Tóm tắt sách</span>
              </div>
              <button
                onClick={() => setReviewOpen(false)}
                style={{
                  width: 28, height: 28, borderRadius: '50%', border: 'none',
                  background: '#f3f4f6', cursor: 'pointer',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  color: '#4b5563',
                }}
              >
                <X size={14} />
              </button>
            </div>

            {/* Scrollable content */}
            <div style={{ overflowY: 'auto', flex: 1, minHeight: 0 }}>
              {reviewLoading && (
                <div style={{ padding: '32px 0', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 12 }}>
                  <Loader2 size={24} color="#7c3aed" style={{ animation: 'spin 1s linear infinite' }} />
                  <span style={{ fontSize: 13, color: '#6b7280', fontWeight: 600 }}>Gemma 4 đang phân tích tác phẩm...</span>
                </div>
              )}

              {reviewError && (
                <div style={{ padding: '16px', background: '#fef2f2', border: '1px solid #fecaca', borderRadius: 12, color: '#b91c1c', fontSize: 12 }}>
                  {reviewError}
                </div>
              )}

              {reviewData && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
                  {/* Tóm tắt */}
                  <div>
                    <h4 style={{ margin: '0 0 6px', fontSize: 11, fontWeight: 800, color: '#4b5563', letterSpacing: '0.05em' }}>TÓM TẮT SÁCH</h4>
                    <p style={{ margin: 0, fontSize: 13, color: '#374151', lineHeight: 1.5, background: '#f9fafb', padding: '12px 14px', borderRadius: 14, border: '1px solid #f3f4f6' }}>
                      {reviewData.summary}
                    </p>
                  </div>

                  {/* Lý do nên đọc */}
                  <div>
                    <h4 style={{ margin: '0 0 8px', fontSize: 11, fontWeight: 800, color: '#4b5563', letterSpacing: '0.05em' }}>3 LÝ DO NÊN ĐỌC</h4>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                      {reviewData.reasons.map((r, i) => (
                        <div key={i} style={{ display: 'flex', gap: 10, alignItems: 'flex-start' }}>
                          <div style={{
                            width: 20, height: 20, borderRadius: '50%',
                            background: '#ecfdf5', border: '1px solid #a7f3d0',
                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                            flexShrink: 0, marginTop: 1,
                          }}>
                            <CheckCircle size={11} color="#059669" />
                          </div>
                          <span style={{ fontSize: 12.5, color: '#1f2937', lineHeight: 1.4 }}>{r}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </>
  );

  return portalRoot ? ReactDOM.createPortal(modalContent, portalRoot) : null;
};

// ── BookCard ─────────────────────────────────────────────────────────
interface BookCardProps {
  book: OcrBookResult;
  rank: number;
  onPress?: () => void;
}

const rankColors = ['#7c3aed', '#3b82f6', '#10b981', '#f59e0b'];

const BookCard: React.FC<BookCardProps> = ({ book, rank, onPress }) => {
  return (
    <div
      id={`ocr-book-${book.book_id}`}
      role={onPress ? 'button' : undefined}
      tabIndex={onPress ? 0 : undefined}
      onClick={onPress}
      onKeyDown={onPress ? e => { if (e.key === 'Enter') onPress(); } : undefined}
      className="ocr-book-row"
      style={{
        display: 'flex', alignItems: 'center', gap: 10,
        padding: '8px 8px', borderRadius: 14,
        cursor: onPress ? 'pointer' : 'default',
        opacity: onPress ? 1 : 0.5,
        marginBottom: 4,
      }}
    >
      {/* Rank badge */}
      <div style={{
        width: 22, height: 22, borderRadius: 7, flexShrink: 0,
        background: rankColors[(rank - 1) % rankColors.length],
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        fontSize: 11, fontWeight: 800, color: 'white',
      }}>
        {rank}
      </div>

      {/* Text info */}
      <div style={{ flex: 1, minWidth: 0 }}>
        <p style={{
          margin: 0, fontSize: 13, fontWeight: 600, color: '#111827',
          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
          lineHeight: 1.3,
        }}>
          {sanitizeBookTitle(book.title)}
        </p>
        {book.author_name && (
          <p style={{
            margin: '1px 0 0', fontSize: 11, color: '#6b7280',
            overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
          }}>
            {book.author_name}
          </p>
        )}
        {book.price && (
          <p style={{ margin: '2px 0 0', fontSize: 12, fontWeight: 700, color: '#7c3aed' }}>
            {fmtPrice(book.price)}
          </p>
        )}
      </div>

      {onPress && (
        <ChevronRight size={15} color="#c4b5fd" style={{ flexShrink: 0 }} />
      )}
    </div>
  );
};

export default OCRResultModal;
