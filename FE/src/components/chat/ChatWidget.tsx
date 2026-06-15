import React, { useState, useRef, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';
import { sendChatMessage, streamChatMessage, generateSessionId, ChatMessage, NavigateButton, prepareImageMessage } from '../../services/chatService';
import { toast } from 'sonner';

// ── Icons (inline SVG) ──────────────────────────────────────────────────────
const SendIcon = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="m22 2-7 20-4-9-9-4Z" /><path d="M22 2 11 13" />
  </svg>
);
const CloseIcon = () => (
  <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
    <path d="M18 6 6 18M6 6l12 12" />
  </svg>
);
const MinimizeIcon = () => (
  <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
    <path d="M5 12h14" />
  </svg>
);
const ChatFabIcon = () => (
  <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
  </svg>
);
const AttachIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="m21.44 11.05-9.19 9.19a6 6 0 0 1-8.49-8.49l8.57-8.57A4 4 0 1 1 18 8.84l-8.59 8.57a2 2 0 0 1-2.83-2.83l8.49-8.48" />
  </svg>
);
const NewChatIcon = () => (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
    <path d="M12 20h9" /><path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4 12.5-12.5z" />
  </svg>
);

// ── Typing dots ──────────────────────────────────────────────────────────────
const TypingDots = () => (
  <div style={{ display: 'flex', gap: '5px', alignItems: 'center', padding: '2px 0' }}>
    {[0, 1, 2].map(i => (
      <span key={i} style={{
        width: 7, height: 7, borderRadius: '50%',
        background: '#a5b4fc',
        display: 'inline-block',
        animation: `chatDot 1.2s ${i * 0.2}s ease-in-out infinite`,
      }} />
    ))}
  </div>
);

// ── Simple inline Markdown renderer (no heavy deps) ──────────────────────────
function renderMarkdown(text: string, roleColor: string = '#6366f1'): React.ReactNode[] {
  const lines = text.split('\n');
  const nodes: React.ReactNode[] = [];

  lines.forEach((line, i) => {
    // Spacer for empty lines
    if (line.trim() === '') {
      nodes.push(<div key={i} style={{ height: 8 }} />);
      return;
    }

    // Numbered list (e.g. "1. Book Title")
    const matchNum = line.match(/^(\d+)\.\s(.*)/);
    if (matchNum) {
      nodes.push(
        <div key={i} style={{ display: 'flex', gap: 8, marginBottom: 6, marginTop: i > 0 ? 8 : 0 }}>
          <span style={{ fontWeight: 600, color: roleColor, opacity: 0.9, flexShrink: 0, minWidth: 16 }}>
            {matchNum[1]}.
          </span>
          <span style={{ flex: 1 }}>{inlineMarkdown(matchNum[2])}</span>
        </div>
      );
      return;
    }

    // Bullet list (e.g. "- item")
    const matchBullet = line.match(/^[-•*]\s(.*)/);
    if (matchBullet) {
      nodes.push(
        <div key={i} style={{ display: 'flex', gap: 8, marginBottom: 6 }}>
          <span style={{ color: roleColor, flexShrink: 0 }}>•</span>
          <span style={{ flex: 1 }}>{inlineMarkdown(matchBullet[1])}</span>
        </div>
      );
      return;
    }

    // Bold heading line (##)
    if (/^#{1,3}\s/.test(line)) {
      nodes.push(
        <div key={i} style={{ fontWeight: 700, fontSize: '1.05em', marginBottom: 8, marginTop: i > 0 ? 12 : 0, color: '#1e293b' }}>
          {inlineMarkdown(line.replace(/^#{1,3}\s/, ''))}
        </div>
      );
      return;
    }

    // Indented lines (e.g. "   Giá: 120,000")
    const matchIndent = line.match(/^(\s{2,})(.*)/);
    if (matchIndent) {
      nodes.push(
        <div key={i} style={{ paddingLeft: 24, marginBottom: 8, fontSize: '0.95em', color: '#475569' }}>
          {inlineMarkdown(matchIndent[2])}
        </div>
      );
      return;
    }

    // Normal text
    nodes.push(<div key={i} style={{ marginBottom: 6 }}>{inlineMarkdown(line)}</div>);
  });

  return nodes;
}

function inlineMarkdown(text: string): React.ReactNode {
  const parts: React.ReactNode[] = [];
  // Handle **bold**, *italic*, `code`, ~~strikethrough~~
  const regex = /(\*\*(.+?)\*\*|\*(.+?)\*|`(.+?)`|~~(.+?)~~)/g;
  let last = 0;
  let match: RegExpExecArray | null;

  while ((match = regex.exec(text)) !== null) {
    if (match.index > last) parts.push(text.slice(last, match.index));
    if (match[2]) parts.push(<strong key={match.index}>{match[2]}</strong>);
    else if (match[3]) parts.push(<em key={match.index}>{match[3]}</em>);
    else if (match[4]) parts.push(
      <code key={match.index} style={{ background: 'rgba(0,0,0,0.08)', borderRadius: 4, padding: '1px 5px', fontFamily: 'monospace', fontSize: '0.9em' }}>{match[4]}</code>
    );
    else if (match[5]) parts.push(
      <s key={match.index} style={{ opacity: 0.55, color: '#64748b' }}>{match[5]}</s>
    );
    last = match.index + match[0].length;
  }
  if (last < text.length) parts.push(text.slice(last));
  return parts.length === 1 ? parts[0] : parts;
}

// ── Role config ──────────────────────────────────────────────────────────────
const ROLE_CONFIG: Record<string, { label: string; color: string; quickReplies: string[] }> = {
  admin: {
    label: 'Admin AI',
    color: '#f59e0b',
    quickReplies: ['Tổng quan dashboard', 'Doanh thu tuần này', 'Thống kê người dùng', 'Sách rating thấp', 'Khuyến mãi đang chạy'],
  },
  staff: {
    label: 'Staff AI',
    color: '#10b981',
    quickReplies: ['Đơn hàng đang chờ xử lý', 'Kiểm tra tồn kho sách', 'Doanh thu hôm nay', 'Khách hàng cần hỗ trợ', 'Sách bán chạy nhất'],
  },
  customer: {
    label: 'Trợ lý BookStore',
    color: '#6366f1',
    quickReplies: ['📚 Sách tâm lý hay nhất', '🔥 Sách đang hot', '📦 Kiểm tra đơn hàng', '🎁 Gợi ý sách tặng quà', '💡 Sách kinh tế & khởi nghiệp'],
  },
  guest: {
    label: 'Trợ lý BookStore',
    color: '#6366f1',
    // Quick replies phù hợp cho khách chưa đăng nhập (không có đơn hàng/điểm thưởng)
    quickReplies: ['🔥 Sách đang hot', '📖 Sách văn học hay', '🎁 Gợi ý sách tặng quà', '💰 Khuyến mãi hiện tại', '📚 Sách phát triển bản thân'],
  },
};

function getRoleConfig(role?: string, isGuest?: boolean) {
  if (role === 'admin') return ROLE_CONFIG.admin;
  if (role === 'staff') return ROLE_CONFIG.staff;
  if (isGuest) return ROLE_CONFIG.guest;
  return ROLE_CONFIG.customer;
}

// ── Message Bubble (no avatar icon) ─────────────────────────────────────────
// React.memo: tránh re-render toàn bộ bubble khi user gõ phím (chỉ input thay đổi)
const MessageBubble = React.memo<{ msg: ChatMessage & { _imagePreview?: string }; isBot: boolean; roleColor: string; onImageClick?: (url: string) => void }>((({ msg, isBot, roleColor, onImageClick }) => {
  const timeStr = msg.timestamp.toLocaleTimeString('vi-VN', { hour: '2-digit', minute: '2-digit' });

  return (
    <div style={{
      display: 'flex',
      justifyContent: isBot ? 'flex-start' : 'flex-end',
      marginBottom: 16,
    }}>
      <div style={{ maxWidth: '85%' }}>
        {/* Ảnh preview trong bubble người dùng */}
        {!isBot && (msg as any)._imagePreview && (
          <div style={{
            marginBottom: 6,
            display: 'flex',
            justifyContent: 'flex-end',
          }}>
            <img
              src={(msg as any)._imagePreview}
              alt="Ảnh đã gửi"
              onClick={() => onImageClick?.((msg as any)._imagePreview)}
              style={{
                maxWidth: 220,
                maxHeight: 200,
                borderRadius: 12,
                objectFit: 'cover',
                boxShadow: '0 2px 12px rgba(0,0,0,0.18)',
                border: '2px solid rgba(255,255,255,0.4)',
                cursor: 'zoom-in',
                transition: 'transform 0.15s',
              }}
              onMouseEnter={e => { e.currentTarget.style.transform = 'scale(1.03)'; }}
              onMouseLeave={e => { e.currentTarget.style.transform = 'scale(1)'; }}
            />
          </div>
        )}
        {/* Text bubble – ẩn nếu user chỉ gửi ảnh không có text */}
        {(msg.content || isBot) && (
          <div style={{
            padding: '12px 16px',
            borderRadius: isBot ? '4px 18px 18px 18px' : '18px 4px 18px 18px',
            background: msg.isError
              ? '#fef2f2'
              : isBot
                ? '#f4f7f9'
                : `linear-gradient(135deg, ${roleColor}, #8b5cf6)`,
            color: msg.isError ? '#dc2626' : isBot ? '#1e293b' : '#fff',
            fontSize: 14,
            lineHeight: 1.6,
            boxShadow: isBot ? '0 1px 3px rgba(0,0,0,0.06)' : '0 2px 8px rgba(99,102,241,0.25)',
            border: msg.isError ? '1px solid #fecaca' : isBot ? '1px solid rgba(0,0,0,0.04)' : 'none',
            wordBreak: 'break-word',
            letterSpacing: '0.1px',
          }}>
            {isBot ? renderMarkdown(msg.content, roleColor) : msg.content}
          </div>
        )}
        <div style={{
          fontSize: 10.5, marginTop: 4, textAlign: isBot ? 'left' : 'right',
          color: 'var(--chat-time, #94a3b8)', padding: '0 4px',
        }}>
          {timeStr}
        </div>
      </div>
    </div>
  );
}));
MessageBubble.displayName = 'MessageBubble';

// ── Main Chat Widget ─────────────────────────────────────────────────────────
const ChatWidget: React.FC = () => {
  const { user } = useAuth();
  const navigate = useNavigate();
  const [isOpen, setIsOpen] = useState(false);
  const [isMinimized, setIsMinimized] = useState(false);
  const CHAT_STORAGE_KEY = `chat_msgs_${user?.id || 'guest'}`;
  const CHAT_SESSION_KEY = `chat_session_${user?.id || 'guest'}`;
  const [messages, setMessages] = useState<ChatMessage[]>(() => {
    // Persist chat history qua navigation và F5 (đồng bộ với session backend)
    try {
      const saved = sessionStorage.getItem(CHAT_STORAGE_KEY);
      if (saved) {
        const parsed = JSON.parse(saved) as ChatMessage[];
        return parsed.map(m => ({ ...m, timestamp: new Date(m.timestamp) }));
      }
    } catch { /* ignore */ }
    return [];
  });
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  // [FIX] Persist session_id vào sessionStorage: F5 không reset context backend
  const [sessionId, setSessionId] = useState(() => {
    try {
      const saved = sessionStorage.getItem(`chat_session_${user?.id || 'guest'}`);
      if (saved) return saved;
    } catch { /* ignore */ }
    const newId = generateSessionId(user?.id);
    try { sessionStorage.setItem(`chat_session_${user?.id || 'guest'}`, newId); } catch { /* ignore */ }
    return newId;
  });
  const [hasNewMessage, setHasNewMessage] = useState(false);
  const [showQuickReplies, setShowQuickReplies] = useState(true);

  // Tự động đóng chatbot khi giỏ hàng (Sheet) mở để tránh chồng lấn UI
  useEffect(() => {
    const checkOverlayAndClose = () => {
      const isCartOpen = !!document.querySelector('[data-slot="sheet-content"]');
      if (isCartOpen && isOpen) {
        setIsOpen(false);
      }
    };

    checkOverlayAndClose();

    const observer = new MutationObserver(checkOverlayAndClose);
    observer.observe(document.body, { childList: true, subtree: true });

    return () => {
      observer.disconnect();
    };
  }, [isOpen]);

  // ── Message history navigation (ArrowUp / ArrowDown) ─────────────────────
  const [inputHistory, setInputHistory] = useState<string[]>([]);
  const historyIndexRef = useRef<number>(-1);  // -1 = not browsing history
  const inputDraftRef = useRef<string>('');    // lưu bản nháp khi bắt đầu browse

  // ── Image attachment state ────────────────────────────────────────────
  const [attachedImage, setAttachedImage] = useState<File | null>(null);
  const [imagePreviewUrl, setImagePreviewUrl] = useState<string | null>(null);
  const [isOCRProcessing, setIsOCRProcessing] = useState(false);
  const imageInputRef = useRef<HTMLInputElement>(null);
  // ── Lightbox state ────────────────────────────────────────────────────
  const [lightboxUrl, setLightboxUrl] = useState<string | null>(null);
  const [lightboxZoom, setLightboxZoom] = useState(1.0);
  const lightboxRef = useRef<HTMLDivElement>(null);

  const openLightbox = (url: string) => { setLightboxUrl(url); setLightboxZoom(1.0); };
  const closeLightbox = () => { setLightboxUrl(null); setLightboxZoom(1.0); };

  // Khoá cuộn trang nền khi mở Lightbox (cả body và documentElement)
  useEffect(() => {
    if (lightboxUrl) {
      document.body.style.overflow = 'hidden';
      document.documentElement.style.overflow = 'hidden';
    } else {
      document.body.style.overflow = '';
      document.documentElement.style.overflow = '';
    }
    return () => {
      document.body.style.overflow = '';
      document.documentElement.style.overflow = '';
    };
  }, [lightboxUrl]);

  // Tự động focus vào Lightbox khi được mở để bắt các sự kiện bàn phím (như phím ESC)
  useEffect(() => {
    if (lightboxUrl && lightboxRef.current) {
      lightboxRef.current.focus();
    }
  }, [lightboxUrl]);

  // Đóng Lightbox khi nhấn phím ESC (Escape)
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        closeLightbox();
      }
    };
    if (lightboxUrl) {
      window.addEventListener('keydown', handleKeyDown);
    }
    return () => {
      window.removeEventListener('keydown', handleKeyDown);
    };
  }, [lightboxUrl]);

  // Chặn cuộn trang mặc định (passive: false) trên Lightbox và thực hiện zoom
  useEffect(() => {
    const handleNativeWheel = (e: WheelEvent) => {
      e.preventDefault();
      setLightboxZoom(z => Math.min(5, Math.max(0.3, z - e.deltaY * 0.001)));
    };
    const el = lightboxRef.current;
    if (el && lightboxUrl) {
      el.addEventListener('wheel', handleNativeWheel, { passive: false });
    }
    return () => {
      if (el) {
        el.removeEventListener('wheel', handleNativeWheel);
      }
    };
  }, [lightboxUrl]);

  // Hỗ trợ dán ảnh từ clipboard (Ctrl+V) bất cứ lúc nào khi khung chat đang mở
  useEffect(() => {
    const handleGlobalPaste = (e: ClipboardEvent) => {
      if (!isOpen || isMinimized || isLoading) return;

      // Nếu đang gõ ở input/textarea khác ngoài khung chat, không được chặn và tự động dán vào chat
      const activeEl = document.activeElement;
      if (activeEl && (activeEl.tagName === 'INPUT' || activeEl.tagName === 'TEXTAREA') && activeEl.id !== 'chat-input') {
        return;
      }

      const items = e.clipboardData?.items;
      if (!items) return;
      for (let i = 0; i < items.length; i++) {
        if (items[i].type.startsWith('image/')) {
          e.preventDefault();
          const file = items[i].getAsFile();
          if (file) {
            const namedFile = new File([file], `clipboard_${Date.now()}.png`, { type: file.type });
            handleImageAttach(namedFile);
          }
          break;
        }
      }
    };
    window.addEventListener('paste', handleGlobalPaste);
    return () => {
      window.removeEventListener('paste', handleGlobalPaste);
    };
  }, [isOpen, isMinimized, isLoading]);

  // Tạo / huỷ object URL khi attach/detach ảnh
  useEffect(() => {
    if (!attachedImage) { setImagePreviewUrl(null); return; }
    const url = URL.createObjectURL(attachedImage);
    setImagePreviewUrl(url);
    return () => URL.revokeObjectURL(url);
  }, [attachedImage]);

  const handleImageAttach = (file: File) => {
    setAttachedImage(file);
    // Auto-focus nút Gửi sau khi attach ảnh → user nhấn Enter/Space là gửi ngay
    setTimeout(() => sendBtnRef.current?.focus(), 50);
  };

  const removeAttachedImage = () => {
    setAttachedImage(null);
    if (imageInputRef.current) imageInputRef.current.value = '';
  };

  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const sendBtnRef = useRef<HTMLButtonElement>(null);  // ← auto-focus sau khi attach ảnh

  // Auto-resize input textarea
  useEffect(() => {
    if (inputRef.current) {
      inputRef.current.style.height = 'auto';
      const scrollHeight = inputRef.current.scrollHeight;
      inputRef.current.style.height = `${Math.min(scrollHeight, 100)}px`;
      inputRef.current.style.overflowY = scrollHeight > 100 ? 'auto' : 'hidden';
    }
  }, [input]);

  // useMemo: roleConfig chỉ thay đổi khi user login/logout, không tính lại khi gõ phím
  const roleConfig = React.useMemo(() => getRoleConfig(user?.role, !user), [user?.role, user]);
  const { label, color: roleColor, quickReplies } = roleConfig;

  // Welcome message
  useEffect(() => {
    if (isOpen && messages.length === 0) {
      const greeting = user
        ? `Xin chào **${user.fullName || user.userName}**!\n\nTôi là **${label}** của BookStore. Bạn cần hỗ trợ gì không?`
        : `Xin chào! Tôi là **${label}** của BookStore.\n\nTôi có thể giúp bạn tìm sách, kiểm tra đơn hàng, gợi ý sách phù hợp và nhiều hơn nữa. Bạn cần gì?`;

      setMessages([{ role: 'assistant', content: greeting, timestamp: new Date() }]);
    }
  }, [isOpen]);

  // [FIX] Persist messages vào sessionStorage mỗi khi cập nhật
  useEffect(() => {
    if (messages.length === 0) return;
    try {
      sessionStorage.setItem(CHAT_STORAGE_KEY, JSON.stringify(messages.slice(-50)));
    } catch { /* quota exceeded, ignore */ }
  }, [messages]);


  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, isLoading]);

  // [FIX] Scroll xuống cuối NGAY KHI mở chat (instant)
  // Cần thiết khi reload trang: messages đã có từ sessionStorage,
  // messages state không thay đổi → useEffect trên không trigger
  useEffect(() => {
    if (isOpen && !isMinimized) {
      // Dùng requestAnimationFrame để chờ DOM render xong trước khi scroll
      requestAnimationFrame(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'instant' });
      });
    }
  }, [isOpen, isMinimized]);

  // Focus input khi mở chat hoặc khi bot trả lời xong
  useEffect(() => {
    if (isOpen && !isMinimized && !isLoading) {
      // Tránh lag/giật focus: chỉ gọi focus nếu user chưa tự click vào input
      if (document.activeElement !== inputRef.current) {
        setTimeout(() => {
          inputRef.current?.focus();
        }, 10); // giảm từ 150ms xuống 10ms để phản hồi tức thì
      }
    }
  }, [isOpen, isMinimized, isLoading]);

  const handleOpen = () => { setIsOpen(true); setIsMinimized(false); setHasNewMessage(false); };

  // Bắt đầu cuộc trò chuyện mới: xóa messages + tạo session_id mới + hiện welcome
  const handleNewConversation = useCallback(() => {
    const newId = generateSessionId(user?.id);
    try {
      sessionStorage.removeItem(CHAT_STORAGE_KEY);
      sessionStorage.setItem(CHAT_SESSION_KEY, newId);
    } catch { /* ignore */ }
    setSessionId(newId);
    const greeting = user
      ? `Xin chào **${user.fullName || user.userName}**!\n\nTôi là **Trợ lý BookStore** của BookStore. Bạn cần hỗ trợ gì không?`
      : `Xin chào! Tôi là **Trợ lý BookStore** của BookStore.\n\nTôi có thể giúp bạn tìm sách, kiểm tra đơn hàng, gợi ý sách phù hợp và nhiều hơn nữa. Bạn cần gì?`;
    setMessages([{ role: 'assistant', content: greeting, timestamp: new Date() }]);
    setShowQuickReplies(true);
    setInput('');
    setTimeout(() => inputRef.current?.focus(), 50);
  }, [user, CHAT_STORAGE_KEY, CHAT_SESSION_KEY]);


  const sendMessage = useCallback(async (text: string, imageFile?: File) => {
    const hasImage = imageFile || attachedImage;
    const trimmed = text.trim();
    if (!trimmed && !hasImage) return;
    if (isLoading) return;

    const imageToSend = imageFile || attachedImage;
    setShowQuickReplies(false);
    setInput('');
    removeAttachedImage();
    // Lưu vào history (bỏ qua nếu trùng tin nhắn liền trước)
    if (trimmed) {
      setInputHistory(prev => {
        if (prev.length > 0 && prev[prev.length - 1] === trimmed) return prev;
        return [...prev.slice(-49), trimmed]; // giữ tối đa 50 tin nhắn
      });
      historyIndexRef.current = -1;
      inputDraftRef.current = '';
    }

    // ── Nếu có ảnh: dùng endpoint /upload-image (OCR thật sự phía BE) ──────
    if (imageToSend) {
      setIsOCRProcessing(true);
      // Tạo object URL để hiển thị ảnh preview ngay trong bubble
      const localPreviewUrl = URL.createObjectURL(imageToSend);
      setMessages(prev => [...prev, {
        role: 'user',
        content: trimmed,   // Chỉ hiện text nếu user thực sự gõ, không tự điền
        timestamp: new Date(),
        _imagePreview: localPreviewUrl,
      } as any]);
      setIsLoading(true);
      setIsOCRProcessing(false);

      // Streaming từ endpoint /upload-image
      const streamingId = Date.now();
      setMessages(prev => [...prev, {
        role: 'assistant', content: '', timestamp: new Date(), _streamingId: streamingId,
      } as any]);

      try {
        const formData = new FormData();
        formData.append('file', imageToSend);
        formData.append('session_id', sessionId);
        formData.append('message', trimmed);
        formData.append('role', user?.role ?? 'customer');
        if (user?.id) formData.append('user_id', user.id);

        const response = await fetch('https://book101.datateam.space/api/chat/upload-image', {
          method: 'POST',
          body: formData,
        });

        if (!response.ok || !response.body) throw new Error('Upload failed');
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        let receivedDone = false;
        let isFirstToken = true;

        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const parts = buffer.split('\n\n');
          buffer = parts.pop() ?? '';
          for (const part of parts) {
            const line = part.trim();
            if (!line.startsWith('data: ')) continue;
            try {
              const payload = JSON.parse(line.slice(6));
              if (payload.type === 'token') {
                if (isFirstToken) {
                  setIsOCRProcessing(false);
                  isFirstToken = false;
                }
                setMessages(prev => prev.map((m: any) =>
                  m._streamingId === streamingId
                    ? { ...m, content: payload.complete ? payload.content : m.content + payload.content }
                    : m
                ));
              } else if (payload.type === 'done') {
                if (isFirstToken) setIsOCRProcessing(false);
                receivedDone = true;
                setMessages(prev => prev.map((m: any) =>
                  m._streamingId === streamingId
                    ? { ...m, navigate_buttons: payload.btns ?? [], _streamingId: undefined }
                    : m
                ));
                setIsLoading(false);
                if (isMinimized) setHasNewMessage(true);
              }
            } catch { /* ignore malformed */ }
          }
        }

        // Stream đóng mà không có 'done' event → cleanup để không bị stuck
        if (!receivedDone) {
          setMessages(prev => prev.map((m: any) =>
            m._streamingId === streamingId
              ? {
                ...m,
                content: m.content || '⚠️ Phản hồi bị ngắt. Vui lòng thử lại.',
                isError: !m.content,
                _streamingId: undefined,
              }
              : m
          ));
          setIsLoading(false);
        }
      } catch {
        setMessages(prev => prev.map((m: any) =>
          m._streamingId === streamingId
            ? { ...m, content: '⚠️ Không thể xử lý ảnh. Vui lòng thử lại.', isError: true, _streamingId: undefined }
            : m
        ));
        setIsLoading(false);
      }
      return;
    }

    // ── Message thuần văn bản bình thường ──────────────────────────────────
    setMessages(prev => [...prev, { role: 'user', content: trimmed, timestamp: new Date() }]);
    setIsLoading(true);

    const isCustomer = !user?.role || user.role === 'customer';

    if (isCustomer) {
      // ── Streaming path (customer / guest) ──────────────────────────────
      // Thêm bubble rỗng ngay lập tức, sau đó nối token vào từng chữ
      const streamingId = Date.now();
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: '',
        timestamp: new Date(),
        _streamingId: streamingId,
      } as any]);

      await streamChatMessage(trimmed, sessionId, user?.role, user?.id, {
        onToken: (token: string, isComplete?: boolean) => {
          setMessages(prev => prev.map((m: any) =>
            m._streamingId === streamingId
              // isComplete=true: BE gửi toàn bộ text đã clean → SET (không cộng dồn)
              // isComplete=false/undefined: token từng mảnh → append bình thường
              ? { ...m, content: isComplete ? token : m.content + token }
              : m
          ));
        },
        onDone: (btns, _sources) => {
          setMessages(prev => prev.map((m: any) =>
            m._streamingId === streamingId
              ? { ...m, navigate_buttons: btns, _streamingId: undefined }
              : m
          ));
          setIsLoading(false);
          if (isMinimized) setHasNewMessage(true);
        },
        onError: (err: Error) => {
          setMessages(prev => prev.map((m: any) =>
            m._streamingId === streamingId
              ? { ...m, content: '⚠️ Không thể kết nối đến máy chủ chatbot. Vui lòng thử lại sau.', isError: true, _streamingId: undefined }
              : m
          ));
          setIsLoading(false);
        },
      });
    } else {
      // ── Non-streaming path (staff / admin) ─────────────────────────────
      try {
        const res = await sendChatMessage(trimmed, sessionId, user?.role, user?.id);
        const botAnswer = res.answer || 'Xin lỗi, tôi không hiểu. Bạn có thể hỏi lại không?';
        setMessages(prev => [...prev, {
          role: 'assistant',
          content: botAnswer,
          timestamp: new Date(),
          navigate_buttons: res.navigate_buttons || [],
        }]);
        if (isMinimized) setHasNewMessage(true);

        // Chỉ trigger optimistic update khi bot XÁC NHẬN thành công (có ✅)
        // KHÔNG trigger khi bot chỉ đang hiển thị thông tin đơn hàng
        const isConfirmationSuccess = botAnswer.includes('✅') && (
          botAnswer.includes('thành công') || botAnswer.includes('success')
        );

        if (isConfirmationSuccess) {
          // Trích xuất Order ID: "đơn #9264" hoặc "#9264"
          const orderIdMatch = botAnswer.match(/#(\d+)/);

          // Trích xuất trạng thái mới: chỉ match → (Unicode) hoặc -> (2 ký tự liền)
          const statusMatch =
            botAnswer.match(/(?:→|->)\s*`?([\w_]+)`?/) ||
            botAnswer.match(/Tr[aạ]ng th[aá]i[:\s]+[\w_]+\s*(?:→|->)\s*`?([\w_]+)`?/i);

          const orderId = orderIdMatch ? parseInt(orderIdMatch[1]) : null;
          const newStatus = statusMatch ? statusMatch[1].trim() : null;

          const isInventoryUpdate = botAnswer.includes('Đã cập nhật tồn kho thành công');
          const isOrderUpdate = botAnswer.includes('Đã cập nhật đơn') || botAnswer.includes('Xác nhận cập nhật');

          // User lock/unlock: "Đã khóa/mở khóa tài khoản thành công!"
          const isUserStatusUpdate = botAnswer.includes('Đã khóa tài khoản thành công') ||
            botAnswer.includes('Đã mở khóa tài khoản thành công');
          // User role change: "Đã đổi role thành công!"
          const isUserRoleUpdate = botAnswer.includes('Đã đổi role thành công');

          // Trích xuất user ID từ "(ID: 3)" trong câu bot trả lời
          const userIdMatch = botAnswer.match(/\(ID:\s*(\d+)\)/);
          const changedUserId = userIdMatch ? userIdMatch[1] : null;

          const isUnlock = botAnswer.toLowerCase().includes('mở khóa');
          const newUserStatus = isUnlock ? 'active' : 'locked';
          const roleMatch = botAnswer.match(/Role:\s*`[\w_]+`\s*→\s*`([\w_]+)`/);
          const newRole = roleMatch ? roleMatch[1] as any : null;

          // Chỉ dispatch khi parse được đủ thông tin
          if ((orderId && newStatus) || isInventoryUpdate || isOrderUpdate) {
            window.dispatchEvent(new CustomEvent('bookstore:data-changed', {
              detail: {
                source: 'chatbot',
                role: user?.role,
                orderId,
                newStatus,
                type: isInventoryUpdate ? 'inventory' : 'order'
              }
            }));
            if (isInventoryUpdate) {
              toast.success('Đã cập nhật tồn kho! Đang đồng bộ giao diện...');
            } else if (orderId) {
              toast.success(`Đã cập nhật trạng thái đơn #${orderId}!`);
            }
          }

          if (isUserStatusUpdate && changedUserId) {
            window.dispatchEvent(new CustomEvent('bookstore:data-changed', {
              detail: { source: 'chatbot', role: user?.role, userId: changedUserId, newUserStatus, type: 'user_status' }
            }));
            toast.success(isUnlock ? `Đã mở khóa tài khoản user #${changedUserId}!` : `Đã khóa tài khoản user #${changedUserId}!`);
          }

          if (isUserRoleUpdate && changedUserId && newRole) {
            window.dispatchEvent(new CustomEvent('bookstore:data-changed', {
              detail: { source: 'chatbot', role: user?.role, userId: changedUserId, newRole, type: 'user_role' }
            }));
            toast.success(`Đã đổi role user #${changedUserId} thành công!`);
          }
        }
      } catch {
        setMessages(prev => [...prev, {
          role: 'assistant',
          content: '⚠️ Không thể kết nối đến máy chủ chatbot. Vui lòng thử lại sau.',
          timestamp: new Date(),
          isError: true,
        }]);
      } finally {
        setIsLoading(false);
      }
    }
  }, [isLoading, sessionId, user, isMinimized, attachedImage, imagePreviewUrl]);

  // useMemo: toàn bộ danh sách message chỉ tính lại khi messages/roleColor/sendMessage thay đổi
  // PHẢI đặt SAU sendMessage để tránh Temporal Dead Zone (TDZ) error
  const renderedMessages = React.useMemo(() =>
    messages.map((msg, i) => {
      if ((msg as any)._streamingId && !msg.content) return null;
      return (
        <div key={i}>
          <MessageBubble msg={msg} isBot={msg.role === 'assistant'} roleColor={roleColor} onImageClick={openLightbox} />
          {msg.role === 'assistant' && msg.navigate_buttons && msg.navigate_buttons.length > 0 && (
            <div style={{ marginTop: -8, marginBottom: 12, paddingLeft: 4 }}>
              {msg.navigate_buttons.filter(b => b.type === 'confirm_yes' || b.type === 'confirm_no').length > 0 && (
                <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 6 }}>
                  {msg.navigate_buttons.filter(b => b.type === 'confirm_yes' || b.type === 'confirm_no').map((btn, bi) => (
                    <button key={`c${bi}`} onClick={() => sendMessage(btn.label)}
                      style={{ padding: '6px 16px', borderRadius: 20, border: `1.5px solid ${btn.type === 'confirm_yes' ? '#10b981' : '#ef4444'}`, background: btn.type === 'confirm_yes' ? 'rgba(16,185,129,0.08)' : 'rgba(239,68,68,0.08)', color: btn.type === 'confirm_yes' ? '#059669' : '#dc2626', fontSize: 13, fontWeight: 600, cursor: 'pointer', transition: 'all .15s' }}
                      onMouseEnter={e => { e.currentTarget.style.opacity = '0.8'; }}
                      onMouseLeave={e => { e.currentTarget.style.opacity = '1'; }}>
                      {btn.label}
                    </button>
                  ))}
                </div>
              )}
              {msg.navigate_buttons.filter(b => b.type === 'quick_reply').length > 0 && (
                <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 6 }}>
                  {msg.navigate_buttons.filter(b => b.type === 'quick_reply').map((btn, bi) => (
                    <button key={`q${bi}`} onClick={() => sendMessage(btn.label)}
                      style={{ padding: '4px 12px', borderRadius: 20, border: `1.5px solid ${roleColor}`, background: 'transparent', color: roleColor, fontSize: 12, fontWeight: 500, cursor: 'pointer', transition: 'opacity .15s', whiteSpace: 'nowrap' }}
                      onMouseEnter={e => { e.currentTarget.style.opacity = '0.75'; }}
                      onMouseLeave={e => { e.currentTarget.style.opacity = '1'; }}>
                      {btn.label}
                    </button>
                  ))}
                </div>
              )}
              {msg.navigate_buttons.filter(b => b.type === 'book' || b.type === 'order' || b.type === 'page').length > 0 && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
                  {msg.navigate_buttons.filter(b => b.type === 'book' || b.type === 'order' || b.type === 'page').map((btn, bi) => {
                    const ac = btn.type === 'book'
                      ? '#6366f1'
                      : btn.type === 'order'
                        ? '#10b981'
                        : '#94a3b8';

                    const bg = btn.type === 'book'
                      ? 'rgba(99,102,241,0.05)'
                      : btn.type === 'order'
                        ? 'rgba(16,185,129,0.05)'
                        : 'rgba(0,0,0,0.02)';

                    const tc = btn.type === 'book'
                      ? '#3730a3'
                      : btn.type === 'order'
                        ? '#047857'
                        : '#475569';

                    const lbl = btn.label.replace(/^[\u{1F300}-\u{1FFFF}\u{2600}-\u{26FF}\u{2700}-\u{27BF}]\s*/u, '');

                    const handleClick = () => navigate(btn.url);

                    return (
                      <button key={`n${bi}`} onClick={handleClick}
                        style={{ display: 'block', textAlign: 'left', padding: '8px 12px 8px 14px', borderRadius: 8, border: `1px solid ${ac}22`, borderLeft: `3px solid ${ac}`, background: bg, color: tc, fontSize: 12.5, fontWeight: 500, cursor: 'pointer', transition: 'all .15s', width: '100%', lineHeight: 1.45 }}
                        onMouseEnter={e => { e.currentTarget.style.background = `${ac}14`; e.currentTarget.style.transform = 'translateX(2px)'; }}
                        onMouseLeave={e => { e.currentTarget.style.background = bg; e.currentTarget.style.transform = 'translateX(0)'; }}>
                        {lbl}
                      </button>
                    );
                  })}
                </div>
              )}
            </div>
          )}
        </div>
      );
    }),
    [messages, roleColor, sendMessage, navigate]
  );


  // ── Paste handler: detect ảnh từ clipboard (Ctrl+V) ────────────────────
  const handlePaste = useCallback((e: React.ClipboardEvent<HTMLTextAreaElement>) => {
    const items = e.clipboardData?.items;
    if (!items) return;
    for (let i = 0; i < items.length; i++) {
      if (items[i].type.startsWith('image/')) {
        e.preventDefault();
        const file = items[i].getAsFile();
        if (file) {
          // Đặt tên file mặc định nếu clipboard không có tên
          const namedFile = new File([file], `clipboard_${Date.now()}.png`, { type: file.type });
          handleImageAttach(namedFile);
        }
        break;
      }
    }
  }, []);

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(input); return; }

    // ArrowUp: lùi về tin nhắn cũ hơn
    if (e.key === 'ArrowUp' && inputHistory.length > 0) {
      e.preventDefault();
      const nextIdx = historyIndexRef.current < inputHistory.length - 1
        ? historyIndexRef.current + 1
        : historyIndexRef.current;
      if (historyIndexRef.current === -1) inputDraftRef.current = input; // lưu bản nháp
      historyIndexRef.current = nextIdx;
      setInput(inputHistory[inputHistory.length - 1 - nextIdx]);
      // Di chuyển cursor về cuối sau khi set giá trị
      setTimeout(() => {
        const ta = inputRef.current;
        if (ta) { ta.selectionStart = ta.selectionEnd = ta.value.length; }
      }, 0);
      return;
    }

    // ArrowDown: tiến về tin nhắn mới hơn / bản nháp
    if (e.key === 'ArrowDown' && historyIndexRef.current >= 0) {
      e.preventDefault();
      const nextIdx = historyIndexRef.current - 1;
      historyIndexRef.current = nextIdx;
      if (nextIdx === -1) {
        setInput(inputDraftRef.current); // khôi phục bản nháp
      } else {
        setInput(inputHistory[inputHistory.length - 1 - nextIdx]);
      }
      setTimeout(() => {
        const ta = inputRef.current;
        if (ta) { ta.selectionStart = ta.selectionEnd = ta.value.length; }
      }, 0);
      return;
    }
  };

  return (
    <>
      {/* Global styles */}
      <style>{`
        @keyframes chatDot {
          0%, 80%, 100% { transform: translateY(0); opacity: .5; }
          40% { transform: translateY(-5px); opacity: 1; }
        }
        @keyframes chatSlideUp {
          from { opacity: 0; transform: translateY(16px) scale(.97); }
          to   { opacity: 1; transform: translateY(0) scale(1); }
        }
        @keyframes chatPulse {
          0%, 100% { box-shadow: 0 4px 20px rgba(99,102,241,0.5); }
          50%       { box-shadow: 0 4px 28px rgba(99,102,241,0.8); }
        }
        .chat-messages::-webkit-scrollbar { width: 4px; }
        .chat-messages::-webkit-scrollbar-thumb { background: rgba(0,0,0,.15); border-radius: 4px; }
        .chat-input-ta::-webkit-scrollbar { display: none; }
        .chat-qr-btn:hover { opacity: 0.85; }
      `}</style>

      {/* ── FAB button ──────────────────────────────────────────── */}
      {!isOpen && (
        <button
          id="chat-widget-fab"
          onClick={handleOpen}
          aria-label="Mở chatbot hỗ trợ"
          title={label}
          style={{
            position: 'fixed', bottom: 28, right: 28, zIndex: 40,
            width: 56, height: 56, borderRadius: '50%', border: 'none',
            background: `linear-gradient(135deg, ${roleColor}, #8b5cf6)`,
            color: '#fff', cursor: 'pointer',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            boxShadow: '0 4px 20px rgba(99,102,241,0.5)',
            animation: 'chatPulse 3s ease-in-out infinite',
            transition: 'transform .18s, box-shadow .18s',
          }}
          onMouseEnter={e => { e.currentTarget.style.transform = 'scale(1.08)'; }}
          onMouseLeave={e => { e.currentTarget.style.transform = 'scale(1)'; }}
        >
          <ChatFabIcon />
          {hasNewMessage && (
            <span style={{
              position: 'absolute', top: 3, right: 3,
              width: 13, height: 13, borderRadius: '50%',
              background: '#ef4444', border: '2px solid #fff',
            }} />
          )}
        </button>
      )}

      {/* ── Chat window ─────────────────────────────────────────── */}
      {isOpen && (
        <div
          id="chat-widget-window"
          style={{
            position: 'fixed', bottom: 28, right: 28, zIndex: 45,
            width: 390, borderRadius: 18,
            height: isMinimized ? 'auto' : 560,
            boxShadow: '0 24px 64px rgba(0,0,0,0.16), 0 4px 16px rgba(0,0,0,0.10)',
            display: 'flex', flexDirection: 'column',
            background: 'var(--chat-bg, #ffffff)',
            border: '1px solid rgba(0,0,0,0.07)',
            animation: 'chatSlideUp .25s ease',
            overflow: 'hidden',
          }}
        >
          {/* Header */}
          <div style={{
            background: `linear-gradient(135deg, ${roleColor} 0%, #8b5cf6 100%)`,
            padding: '13px 16px',
            display: 'flex', alignItems: 'center', gap: 10,
            color: '#fff',
          }}>
            <div style={{ flex: 1 }}>
              <div style={{ fontWeight: 700, fontSize: 14.5 }}>{label}</div>
              <div style={{ fontSize: 11.5, opacity: .88, marginTop: 2, display: 'flex', alignItems: 'center', gap: 5 }}>
                <span style={{ width: 7, height: 7, borderRadius: '50%', background: '#4ade80', display: 'inline-block' }} />
                Đang hoạt động
              </div>
            </div>
            <button
              id="chat-new-conversation-btn"
              onClick={handleNewConversation}
              aria-label="Cuộc trò chuyện mới"
              title="Bắt đầu cuộc trò chuyện mới"
              style={{
                background: 'none',
                border: 'none',
                color: '#fff',
                cursor: 'pointer',
                padding: 5,
                opacity: .85,
                lineHeight: 1,
                borderRadius: 6,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                transition: 'opacity .15s',
              }}
              onMouseEnter={e => { e.currentTarget.style.opacity = '1'; }}
              onMouseLeave={e => { e.currentTarget.style.opacity = '.85'; }}>
              <NewChatIcon />
            </button>
            <button id="chat-minimize-btn" onClick={() => setIsMinimized(m => !m)} aria-label="Thu nhỏ"
              style={{ background: 'none', border: 'none', color: '#fff', cursor: 'pointer', padding: 5, opacity: .85, lineHeight: 1, borderRadius: 6 }}>
              <MinimizeIcon />
            </button>
            <button id="chat-close-btn" onClick={() => setIsOpen(false)} aria-label="Đóng"
              style={{ background: 'none', border: 'none', color: '#fff', cursor: 'pointer', padding: 5, opacity: .85, lineHeight: 1, borderRadius: 6 }}>
              <CloseIcon />
            </button>
          </div>

          {/* Body */}
          {!isMinimized && (
            <>
              {/* Guest banner – chỉ hiện khi chưa đăng nhập */}
              {!user && (
                <div style={{
                  display: 'flex', alignItems: 'center', gap: 10,
                  padding: '8px 14px',
                  background: 'linear-gradient(90deg, rgba(99,102,241,0.07) 0%, rgba(139,92,246,0.07) 100%)',
                  borderBottom: '1px solid rgba(99,102,241,0.12)',
                  fontSize: 12,
                  color: '#6366f1',
                }}>
                  <span style={{ flexShrink: 0 }}>&#128274;</span>
                  <span style={{ flex: 1 }}>
                    <strong>Đăng nhập</strong> để theo dõi đơn hàng &amp; nhận gợi ý cá nhân hóa
                  </span>
                  <a
                    href="/login"
                    style={{
                      background: '#6366f1', color: '#fff',
                      borderRadius: 20, padding: '3px 10px',
                      fontSize: 11, fontWeight: 600,
                      textDecoration: 'none', flexShrink: 0,
                      whiteSpace: 'nowrap',
                    }}
                  >
                    Đăng nhập
                  </a>
                </div>
              )}

              {/* Messages area */}
              <div
                className="chat-messages"
                style={{
                  flex: 1, overflowY: 'auto', padding: '16px 16px 8px',
                  background: 'var(--chat-messages-bg, #f8fafc)',
                  minHeight: 0,
                }}
              >
                {/* Messages area — dùng renderedMessages (memoized) để tránh re-render khi gõ */}
                {renderedMessages}

                {/* Typing indicator */}
                {isLoading && (
                  <div style={{ marginBottom: 12 }}>
                    <div style={{
                      display: 'inline-flex',
                      alignItems: 'center',
                      gap: '8px',
                      padding: '10px 14px',
                      borderRadius: '4px 18px 18px 18px',
                      background: 'var(--chat-bot-bubble, #f1f5f9)',
                      boxShadow: '0 1px 3px rgba(0,0,0,0.07)',
                    }}>
                      {isOCRProcessing && <span style={{ fontSize: '12.5px', color: '#475569', fontWeight: 500 }}>🔍 Đang nhận dạng ảnh bìa sách...</span>}
                      <TypingDots />
                    </div>
                  </div>
                )}
                <div ref={messagesEndRef} />
              </div>

              {/* Quick replies */}
              {showQuickReplies && messages.length <= 1 && (
                <div style={{
                  padding: '8px 12px 8px',
                  display: 'flex', flexWrap: 'wrap', gap: 6,
                  background: 'var(--chat-messages-bg, #f8fafc)',
                  borderTop: '1px solid rgba(0,0,0,0.05)',
                }}>
                  {quickReplies.map(qr => (
                    <button
                      key={qr}
                      className="chat-qr-btn"
                      onClick={() => sendMessage(qr)}
                      style={{
                        padding: '5px 12px',
                        borderRadius: 20,
                        border: `1.5px solid ${roleColor}`,
                        background: 'transparent',
                        color: roleColor,
                        fontSize: 11.5,
                        cursor: 'pointer',
                        fontWeight: 500,
                        transition: 'opacity .15s',
                        whiteSpace: 'nowrap',
                      }}
                    >
                      {qr}
                    </button>
                  ))}
                </div>
              )}

              {/* Input area */}
              <div style={{
                padding: '10px 12px 10px',
                background: 'var(--chat-bg, #ffffff)',
                borderTop: '1px solid rgba(0,0,0,0.07)',
              }}>
                {/* Image attachment preview strip */}
                {imagePreviewUrl && (
                  <div style={{
                    display: 'flex', alignItems: 'center', gap: 8,
                    padding: '6px 8px', marginBottom: 6,
                    background: 'rgba(99,102,241,0.06)',
                    borderRadius: 10, border: '1.5px solid rgba(99,102,241,0.18)',
                  }}>
                    <img
                      src={imagePreviewUrl}
                      alt="Ảnh đính kèm"
                      style={{ width: 40, height: 40, objectFit: 'cover', borderRadius: 6, flexShrink: 0 }}
                    />
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontSize: 11.5, fontWeight: 600, color: '#6366f1' }}>
                        {isOCRProcessing ? '🔍 Đang nhận diện ảnh...' : '📷 Ảnh đính kèm'}
                      </div>
                      <div style={{
                        fontSize: 10.5, color: '#94a3b8', marginTop: 1,
                        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap'
                      }}>
                        {attachedImage?.name.slice(0, 28)}{(attachedImage?.name.length ?? 0) > 28 ? '...' : ''}
                      </div>
                    </div>
                    <button
                      onClick={removeAttachedImage}
                      style={{
                        background: 'none', border: 'none', cursor: 'pointer',
                        color: '#94a3b8', padding: 4, borderRadius: 6, flexShrink: 0, lineHeight: 1
                      }}
                      title="Xóa ảnh"
                    >
                      <CloseIcon />
                    </button>
                  </div>
                )}

                {/* Textarea row */}
                <div style={{ display: 'flex', alignItems: 'flex-end', gap: 6 }}>
                  {/* Hidden file input */}
                  <input
                    ref={imageInputRef}
                    type="file"
                    accept="image/jpeg,image/jpg,image/png,image/webp,image/bmp"
                    style={{ display: 'none' }}
                    onChange={(e) => {
                      const file = e.target.files?.[0];
                      if (file) handleImageAttach(file);
                    }}
                    aria-label="Chọn ảnh đính kèm"
                  />

                  {/* Attach button */}
                  <button
                    id="chat-attach-btn"
                    onClick={() => imageInputRef.current?.click()}
                    disabled={isLoading}
                    title="Đính kèm ảnh sách (để nhận diện OCR)"
                    aria-label="Đính kèm ảnh"
                    style={{
                      width: 34, height: 34, borderRadius: '50%',
                      border: attachedImage ? `2px solid ${roleColor}` : '1.5px solid rgba(0,0,0,0.15)',
                      background: attachedImage ? `rgba(99,102,241,0.10)` : 'transparent',
                      color: attachedImage ? roleColor : '#94a3b8',
                      cursor: isLoading ? 'not-allowed' : 'pointer',
                      display: 'flex', alignItems: 'center', justifyContent: 'center',
                      flexShrink: 0, transition: 'all .18s',
                    }}
                    onMouseEnter={e => { if (!isLoading) e.currentTarget.style.color = roleColor; }}
                    onMouseLeave={e => { if (!attachedImage) e.currentTarget.style.color = '#94a3b8'; }}
                  >
                    <AttachIcon />
                  </button>

                  {/* Textarea */}
                  <textarea
                    ref={inputRef}
                    id="chat-input"
                    className="chat-input-ta"
                    value={input}
                    onChange={e => setInput(e.target.value)}
                    onKeyDown={handleKeyDown}
                    onPaste={handlePaste}
                    placeholder="Nhắn tin hoặc dán ảnh (Ctrl+V)..."
                    rows={1}
                    disabled={isLoading}
                    style={{
                      flex: 1, resize: 'none',
                      border: '1.5px solid rgba(0,0,0,0.12)',
                      borderRadius: 12,
                      padding: '9px 12px',
                      fontSize: 13.5, outline: 'none',
                      background: 'var(--chat-input-bg, #f8fafc)',
                      color: 'var(--chat-text, #1e293b)',
                      fontFamily: 'inherit',
                      lineHeight: 1.5,
                      maxHeight: 100, overflowY: 'hidden',
                      scrollbarWidth: 'none',
                      transition: 'border-color .15s',
                    }}
                    onFocus={e => (e.currentTarget.style.borderColor = roleColor)}
                    onBlur={e => (e.currentTarget.style.borderColor = 'rgba(0,0,0,0.12)')}
                  />

                  {/* Send button */}
                  <button
                    ref={sendBtnRef}
                    id="chat-send-btn"
                    onClick={() => sendMessage(input)}
                    disabled={isLoading || (!input.trim() && !attachedImage)}
                    aria-label="Gửi tin nhắn"
                    style={{
                      width: 40, height: 40, borderRadius: '50%', border: 'none',
                      background: (input.trim() || attachedImage)
                        ? `linear-gradient(135deg, ${roleColor}, #8b5cf6)`
                        : '#e2e8f0',
                      color: (input.trim() || attachedImage) ? '#fff' : '#94a3b8',
                      cursor: (input.trim() || attachedImage) && !isLoading ? 'pointer' : 'not-allowed',
                      display: 'flex', alignItems: 'center', justifyContent: 'center',
                      flexShrink: 0, transition: 'all .18s',
                      boxShadow: (input.trim() || attachedImage) ? '0 2px 8px rgba(99,102,241,0.35)' : 'none',
                    }}
                  >
                    <SendIcon />
                  </button>
                </div>
              </div>

              {/* Footer */}
              <div style={{
                textAlign: 'center', fontSize: 10.5, color: '#cbd5e1',
                padding: '3px 12px 8px',
                background: 'var(--chat-bg, #ffffff)',
              }}>
                Powered by BookStore AI
              </div>
            </>
          )}
        </div>
      )}
      {/* ── Lightbox Modal ──────────────────────────────────────────────── */}
      {lightboxUrl && (
        <div
          ref={lightboxRef}
          onClick={closeLightbox}
          tabIndex={0}
          style={{
            position: 'fixed', inset: 0, zIndex: 9999,
            background: 'rgba(0,0,0,0.82)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            cursor: lightboxZoom > 1 ? 'move' : 'zoom-out',
            animation: 'chatSlideUp .15s ease',
            userSelect: 'none',
            outline: 'none',
            touchAction: 'none',
          }}
        >
          <img
            src={lightboxUrl}
            alt="Xem ảnh"
            onClick={e => e.stopPropagation()}
            onDoubleClick={() => setLightboxZoom(1.0)}
            style={{
              maxWidth: '90vw', maxHeight: '88vh',
              borderRadius: lightboxZoom > 1 ? 0 : 14,
              boxShadow: '0 24px 80px rgba(0,0,0,0.6)',
              objectFit: 'contain',
              userSelect: 'none',
              transform: `scale(${lightboxZoom})`,
              transformOrigin: 'center center',
              transition: lightboxZoom === 1 ? 'transform 0.2s ease' : 'none',
              cursor: lightboxZoom > 1 ? 'move' : 'zoom-in',
            }}
          />
          {/* Điều khiển zoom */}
          <div style={{
            position: 'absolute', top: 18, left: '50%',
            transform: 'translateX(-50%)',
            display: 'flex', alignItems: 'center', gap: 10,
            background: 'rgba(0,0,0,0.55)', borderRadius: 30,
            padding: '5px 14px', backdropFilter: 'blur(6px)',
          }}>
            <button onClick={e => { e.stopPropagation(); setLightboxZoom(z => Math.max(0.3, z - 0.2)); }}
              style={{ background: 'none', border: 'none', color: '#fff', fontSize: 18, cursor: 'pointer', lineHeight: 1, padding: '0 4px' }}>−</button>
            <span style={{ color: '#fff', fontSize: 12, fontWeight: 600, minWidth: 40, textAlign: 'center' }}>
              {Math.round(lightboxZoom * 100)}%
            </span>
            <button onClick={e => { e.stopPropagation(); setLightboxZoom(z => Math.min(5, z + 0.2)); }}
              style={{ background: 'none', border: 'none', color: '#fff', fontSize: 18, cursor: 'pointer', lineHeight: 1, padding: '0 4px' }}>+</button>
            <div style={{ width: 1, height: 16, background: 'rgba(255,255,255,0.3)', margin: '0 4px' }} />
            <button onClick={e => { e.stopPropagation(); setLightboxZoom(1.0); }}
              style={{ background: 'none', border: 'none', color: 'rgba(255,255,255,0.7)', fontSize: 11, cursor: 'pointer', padding: '0 2px' }}>1:1</button>
          </div>
          <button
            onClick={closeLightbox}
            style={{
              position: 'absolute', top: 18, right: 22,
              background: 'rgba(255,255,255,0.12)',
              border: '1.5px solid rgba(255,255,255,0.25)',
              borderRadius: '50%', width: 40, height: 40,
              color: '#fff', cursor: 'pointer',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              fontSize: 20, fontWeight: 300, lineHeight: 1,
              backdropFilter: 'blur(4px)',
              transition: 'background 0.15s',
            }}
            onMouseEnter={e => { e.currentTarget.style.background = 'rgba(255,255,255,0.22)'; }}
            onMouseLeave={e => { e.currentTarget.style.background = 'rgba(255,255,255,0.12)'; }}
            aria-label="Đóng ảnh"
          >
            ✕
          </button>
          <div style={{
            position: 'absolute', bottom: 20,
            color: 'rgba(255,255,255,0.45)', fontSize: 11.5,
            pointerEvents: 'none',
          }}>
            Cuộn chuột để zoom • Double-click để về 100% • Click ngoài để đóng
          </div>
        </div>
      )}
    </>
  );
};

export default ChatWidget;
