import React, { useEffect, useState, useRef } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { toast } from 'sonner';

import PageLoader from '../components/PageLoader';

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
