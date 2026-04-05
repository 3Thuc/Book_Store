import React, { useEffect, useState, useRef } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { toast } from 'sonner';

const LoadingScreen = () => {
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    const duration = 1500;
    const steps = 60;
    const increment = 100 / steps;
    const intervalTime = duration / steps;

    const interval = setInterval(() => {
      setProgress((prev) => {
        const next = prev + increment;
        if (next >= 100) {
          clearInterval(interval);
          return 100;
        }
        return next;
      });
    }, intervalTime);

    return () => clearInterval(interval);
  }, []);

  return (
    <div className="loading-screen">
      <div className="loading-icon">
        <svg
          width="48"
          height="48"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          className="text-foreground"
        >
          <path d="M4 19.5v-15A2.5 2.5 0 0 1 6.5 2H20v20H6.5a2.5 2.5 0 0 1 0-5H20" />
        </svg>
      </div>
      <div className="loading-progress-container">
        <div
          className="loading-progress-bar"
          style={{ width: `${progress}%` }}
        ></div>
      </div>
    </div>
  );
};

const PageLoader = () => <LoadingScreen />;

export const GoogleCallbackPage: React.FC = () => {
  const { loginWithGoogle } = useAuth();
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const [error, setError] = useState<string | null>(null);

  const hasCalledApi = useRef(false);

  useEffect(() => {
    const googleCode = searchParams.get('code');
    const googleError = searchParams.get('error');

    if (googleError) {
      toast.error('Đăng nhập Google bị từ chối.');
      setError(
        'Đăng nhập Google thất bại. Đang chuyển hướng về trang đăng nhập...'
      );
      setTimeout(() => navigate('/login'), 3000);
      return;
    }

    if (googleCode && !hasCalledApi.current) {
      hasCalledApi.current = true;

      toast.info('Đang xác thực với Google...');

      const handleCallback = async (code: string) => {
        try {
          const success = await loginWithGoogle(code);

          if (success) {
            toast.success('Đăng nhập Google thành công!');
            navigate('/');
          } else {
            toast.error('Xác thực Google thất bại.');
            setError(
              'Xác thực thất bại. Đang chuyển hướng về trang đăng nhập...'
            );
            setTimeout(() => navigate('/login'), 3000);
          }
        } catch (err) {
          console.error('Google callback error:', err);
          toast.error('Lỗi khi đăng nhập bằng Google.');
          setError('Đã xảy ra lỗi. Đang chuyển hướng về trang đăng nhập...');
          setTimeout(() => navigate('/login'), 3000);
        }
      };

      handleCallback(googleCode);
    } else if (!googleCode && !googleError) {
      toast.error('URL không hợp lệ.');
      setTimeout(() => navigate('/login'), 1000);
    }
  }, [loginWithGoogle, navigate, searchParams]);

  if (error) {
    return (
      <div className="flex items-center justify-center min-h-screen text-red-500">
        {error}
      </div>
    );
  }

  return <PageLoader />;
};

export default GoogleCallbackPage;
