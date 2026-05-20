/**
 * chatService.ts – Giao tiếp với Chatbot Backend (port 8004).
 * Hỗ trợ 2 chế độ:
 *   - sendChatMessage()   : POST /message  (JSON, dùng cho staff/admin)
 *   - streamChatMessage() : POST /stream   (SSE, dùng cho customer – chữ xuất hiện từng token)
 */

import { getAuthToken } from '../lib/api-client';

const CHATBOT_BASE = 'http://localhost:8004';

export interface ChatMessage {
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
  isError?: boolean;
  navigate_buttons?: NavigateButton[];
}

export interface NavigateButton {
  label: string;
  url: string;
  /** Navigation types: book | order | page
   *  Clarify-First types: confirm_yes | confirm_no | quick_reply */
  type: 'book' | 'order' | 'page' | 'confirm_yes' | 'confirm_no' | 'quick_reply';
  metadata?: Record<string, any>;
}

export interface ChatResponse {
  answer: string;
  intent?: string;
  confidence?: number;
  session_id?: string;
  navigate_buttons?: NavigateButton[];
}

/** Chọn endpoint đúng theo role */
function getChatEndpoint(role?: string, stream = false): string {
  const suffix = stream ? '/stream' : '/message';
  if (role === 'admin') return `${CHATBOT_BASE}/api/admin/chat${suffix}`;
  if (role === 'staff') return `${CHATBOT_BASE}/api/staff/chat${suffix}`;
  return `${CHATBOT_BASE}/api/chat${suffix}`;
}

/** Payload chung cho cả 2 chế độ */
function buildBody(message: string, sessionId: string, userRole?: string, userId?: string) {
  const body: Record<string, any> = {
    message,
    session_id: sessionId,
    role: userRole ?? 'customer',
  };
  if (userId) {
    const parsedId = parseInt(userId, 10);
    if (!isNaN(parsedId)) body.user_id = parsedId;
  }
  return body;
}

function buildHeaders(): Record<string, string> {
  const token = getAuthToken();
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  return headers;
}

// ── Non-streaming (staff / admin / fallback) ─────────────────────────────────
export async function sendChatMessage(
  message: string,
  sessionId: string,
  userRole?: string,
  userId?: string
): Promise<ChatResponse> {
  const response = await fetch(getChatEndpoint(userRole, false), {
    method: 'POST',
    headers: buildHeaders(),
    body: JSON.stringify(buildBody(message, sessionId, userRole, userId)),
  });

  if (!response.ok) throw new Error(`Chatbot server error: ${response.status}`);
  return response.json();
}

// ── Streaming (customer – SSE via fetch + ReadableStream) ────────────────────
export interface StreamCallbacks {
  /** Gọi mỗi khi nhận 1 token mới.
   *  isComplete=true → BE gửi full text đã clean (SET thay vì append) */
  onToken: (token: string, isComplete?: boolean) => void;
  /** Gọi khi stream kết thúc, kèm btns + sources */
  onDone: (btns: NavigateButton[], sources: string[]) => void;
  /** Gọi khi có lỗi kết nối */
  onError: (err: Error) => void;
}

export async function streamChatMessage(
  message: string,
  sessionId: string,
  userRole: string | undefined,
  userId: string | undefined,
  callbacks: StreamCallbacks
): Promise<void> {
  try {
    const response = await fetch(getChatEndpoint(userRole, true), {
      method: 'POST',
      headers: buildHeaders(),
      body: JSON.stringify(buildBody(message, sessionId, userRole, userId)),
    });

    if (!response.ok) throw new Error(`Stream error: ${response.status}`);
    if (!response.body) throw new Error('ReadableStream not supported');

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });

      // SSE: tách các message "data: ...\n\n"
      const parts = buffer.split('\n\n');
      buffer = parts.pop() ?? '';   // phần cuối có thể chưa hoàn chỉnh

      for (const part of parts) {
        const line = part.trim();
        if (!line.startsWith('data: ')) continue;
        try {
          const payload = JSON.parse(line.slice(6));
          if (payload.type === 'token') {
            // complete:true → BE đã tổng hợp + clean toàn bộ text (set, không append)
            // complete vắng mặt / false → append từng token vào message đang build
            callbacks.onToken(payload.content ?? '', payload.complete === true);
          } else if (payload.type === 'done') {
            callbacks.onDone(payload.btns ?? [], payload.sources ?? []);
          }
        } catch {
          // ignore malformed JSON
        }
      }
    }
  } catch (err) {
    callbacks.onError(err instanceof Error ? err : new Error(String(err)));
  }
}

/** Tạo session_id */
export function generateSessionId(userId?: string): string {
  const base = userId || Math.random().toString(36).substring(2, 10);
  return `session_${base}_${Date.now()}`;
}

// ── OCR Bridge ────────────────────────────────────────────────────────────────
// Luồng: File ảnh → OCR Service :8005 → extracted_text → inject vào prompt
// Format prompt: "[Ảnh OCR: "...text..."]\nCâu hỏi của user"

/**
 * Gọi OCR Service để trích xuất text từ ảnh.
 * Trả về extracted_text hoặc null nếu thất bại.
 */
export async function extractTextFromImage(imageFile: File): Promise<{
  text: string;
  bookTitle: string | null;
  authors: string[];
  confidence: number;
} | null> {
  try {
    const formData = new FormData();
    formData.append('file', imageFile);

    const response = await fetch('http://127.0.0.1:8005/api/ocr/search-by-cover', {
      method: 'POST',
      body: formData,
      signal: AbortSignal.timeout(60_000), // 60s timeout
    });

    if (!response.ok) return null;

    const data = await response.json();
    if (!data.success) return null;

    return {
      text: data.extracted_text ?? '',
      bookTitle: data.book_info?.title ?? null,
      authors: data.book_info?.authors ?? [],
      confidence: data.book_info?.confidence ?? 0,
    };
  } catch {
    return null; // OCR service offline hoặc lỗi → vẫn gửi message gốc
  }
}

/**
 * Build enhanced prompt chứa OCR context.
 *
 * - Nếu có bookTitle rõ ràng → "[Ảnh OCR: \"Tên sách - Tác giả\"]"
 * - Nếu chỉ có raw text nhưng confidence ≥ 0.3 → inject raw text ngắn
 * - Nếu confidence < 0.3 hoặc text garbled → fallback chung "người dùng gửi ảnh sách"
 */
export function buildOCREnhancedPrompt(
  userMessage: string,
  ocrResult: { text: string; bookTitle: string | null; authors: string[]; confidence: number }
): string {
  const parts: string[] = [];

  if (ocrResult.bookTitle) {
    // ưu tiên title sạch: chính xác nhất
    const authStr = ocrResult.authors.length > 0 ? ` - ${ocrResult.authors.join(', ')}` : '';
    parts.push(`[Ảnh OCR: "${ocrResult.bookTitle}${authStr}"]`);
  } else if (ocrResult.confidence >= 0.3 && ocrResult.text.trim()) {
    // Text có độ tin cậy đủ cao → inject raw text (cắt ngắn)
    const truncated = ocrResult.text.trim().slice(0, 200);
    const hasMeaningfulText = truncated.replace(/[^a-zA-Z\u00C0-\u024F\u1EA0-\u1EF9]/g, '').length > 5;
    if (hasMeaningfulText) {
      parts.push(`[Ảnh OCR: "${truncated}${ocrResult.text.length > 200 ? '...' : ''}"]`);
    } else {
      parts.push('[Người dùng gởi ảnh bìa sách (không đọc rõ chữ)]');
    }
  } else {
    // Confidence thấp hoặc không text → message chung
    parts.push('[Người dùng gởi ảnh bìa sách - nội dung chưa nhận diện được rõ]');
  }

  if (userMessage.trim()) {
    parts.push(userMessage.trim());
  }

  return parts.join('\n');
}

/**
 * Gửi message kèm ảnh (với OCR bridge).
 * Dùng cho cả non-streaming (admin/staff) và streaming (customer).
 *
 * @param imageFile   File ảnh đã chọn
 * @param userMessage Câu hỏi của user (có thể trống)
 * @returns           { enhancedPrompt, ocrResult } để hiển thị trong UI
 */
export async function prepareImageMessage(imageFile: File, userMessage: string): Promise<{
  enhancedPrompt: string;
  ocrResult: { text: string; bookTitle: string | null; authors: string[]; confidence: number } | null;
  displayText: string;  // Text hiển thị trong bubble user (ngắn gọn)
}> {
  const ocrResult = await extractTextFromImage(imageFile);

  if (!ocrResult) {
    // OCR thất bại → gửi message gốc, không có context ảnh
    return {
      enhancedPrompt: userMessage || 'Tôi vừa gửi một ảnh sách.',
      ocrResult: null,
      displayText: userMessage || '📷 [Ảnh sách]',
    };
  }

  const enhancedPrompt = buildOCREnhancedPrompt(userMessage, ocrResult);

  // Text hiển thị trong bubble: gọn, thân thiện
  let bookPart: string;
  if (ocrResult.bookTitle) {
    bookPart = `📷 "${ocrResult.bookTitle}"`;
  } else if (ocrResult.confidence >= 0.3) {
    bookPart = '📷 [Ảnh sách]';
  } else {
    bookPart = '📷 [Ảnh sách - chất lượng thấp]';
  }
  const displayText = userMessage.trim()
    ? `${bookPart} – ${userMessage.trim()}`
    : bookPart;

  return { enhancedPrompt, ocrResult, displayText };
}

