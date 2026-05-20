import React, { useState, useEffect } from 'react';

/**
 * PageLoader – loading screen hiện trong khi React.lazy() đang tải chunks.
 *
 * Vấn đề cũ: dùng setInterval để cập nhật progress.
 * Khi browser đang parse JS bundle lớn, main thread bị block
 * → setInterval không chạy → thanh bar đứng yên.
 *
 * Fix mới: dùng CSS @keyframes chạy trên compositor thread.
 * CSS animation KHÔNG bị block bởi JS → bar luôn chạy mượt.
 * Khi module load xong, React unmount component → animation dừng tự nhiên.
 */
const PageLoader: React.FC = () => {
  // done=true: component biết rằng đã được giữ đủ thời gian, sẵn sàng hide
  const [done, setDone] = useState(false);

  useEffect(() => {
    // Chỉ dùng để "fill to 100%" sau khi React.lazy resolve
    // (thực tế component sẽ unmount khi lazy load xong, nhưng
    //  setDone giúp animate mượt hơn nếu component vẫn còn mount)
    const t = window.setTimeout(() => setDone(true), 2800);
    return () => window.clearTimeout(t);
  }, []);

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        minHeight: '100vh',
        gap: '20px',
        backgroundColor: 'var(--background, #fff)',
      }}
    >
      {/* Book icon — pulse animation via CSS, không dùng JS */}
      <svg
        width="48"
        height="48"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinecap="round"
        strokeLinejoin="round"
        style={{
          opacity: 0.55,
          color: '#94a3b8',
          animation: 'plPulse 2s ease-in-out infinite',
        }}
      >
        <path d="M4 19.5v-15A2.5 2.5 0 0 1 6.5 2H20v20H6.5a2.5 2.5 0 0 1 0-5H20" />
      </svg>

      <div style={{ fontSize: '13px', color: '#64748b', fontWeight: 500 }}>
        Đang tải thư viện giao diện...
      </div>

      {/* Progress bar track */}
      <div
        style={{
          width: 220,
          height: 4,
          backgroundColor: '#e2e8f0',
          borderRadius: 4,
          overflow: 'hidden',
        }}
      >
        {/* Dual animation: fill (0→91%) + shimmer vô tận
            → bar luôn có chuyển động ngay cả khi fill dừng lại */}
        <div
          style={{
            height: '100%',
            borderRadius: 4,
            backgroundColor: done ? '#0f172a' : undefined,
            backgroundImage: done
              ? 'none'
              : 'linear-gradient(90deg, #0f172a 0%, #475569 45%, #0f172a 100%)',
            backgroundSize: '300% 100%',
            width: done ? '100%' : undefined,
            transition: done ? 'width 0.35s ease' : undefined,
            animation: done ? 'none' : 'plFill 2.6s cubic-bezier(0.15, 0.05, 0.02, 1) forwards, plShimmer 1.2s ease-in-out infinite',
          }}
        />
      </div>

      <style>{`
        @keyframes plFill {
          0%   { width: 0%;  }
          8%   { width: 22%; }
          20%  { width: 40%; }
          38%  { width: 58%; }
          55%  { width: 70%; }
          70%  { width: 79%; }
          83%  { width: 85%; }
          93%  { width: 88%; }
          100% { width: 91%; }
        }
        @keyframes plShimmer {
          0%   { background-position: 150% center; }
          100% { background-position: -150% center; }
        }
        @keyframes plPulse {
          0%, 100% { opacity: 0.55; transform: scale(1); }
          50%       { opacity: 0.25; transform: scale(1.06); }
        }
      `}</style>
    </div>
  );
};

export default PageLoader;
