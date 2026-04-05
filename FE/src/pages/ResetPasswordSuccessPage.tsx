import React, { useEffect } from 'react';
import { CheckCircle2, Mail, ArrowLeft, Book } from 'lucide-react';
import { Button } from '../components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card';
import { ThemeToggle } from '../components/ThemeToggle';
import { useNavigate, useLocation } from 'react-router-dom';

export const ResetPasswordSuccessPage: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const email = location.state?.email;

  // Redirect if no email in state
  useEffect(() => {
    if (!email) {
      navigate('/forgot-password', { replace: true });
    }
  }, [email, navigate]);

  if (!email) {
    return null;
  }

  return (
    <div className="min-h-screen flex flex-col bg-gradient-to-br from-slate-50 via-blue-50/30 to-purple-50/20 dark:from-slate-950 dark:via-blue-950/30 dark:to-purple-950/20">
      {/* Header */}
      <header className="border-b border-border bg-background/80 backdrop-blur-lg sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-3">
          <div className="flex items-center justify-between">
            <div 
              className="flex items-center space-x-2 cursor-pointer"
              onClick={() => navigate('/')}
            >
              <Book className="h-8 w-8 text-primary" />
              <span className="text-xl font-bold text-foreground">BookStore</span>
            </div>
            <ThemeToggle />
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="flex-1 flex items-center justify-center px-4 py-12 relative overflow-hidden">
        {/* Background Decorations */}
        <div className="absolute inset-0 overflow-hidden pointer-events-none">
          <div className="absolute -top-40 -right-40 w-80 h-80 bg-gradient-to-br from-purple-400/20 to-pink-400/20 dark:from-purple-600/10 dark:to-pink-600/10 rounded-full blur-3xl"></div>
          <div className="absolute -bottom-40 -left-40 w-80 h-80 bg-gradient-to-br from-blue-400/20 to-cyan-400/20 dark:from-blue-600/10 dark:to-cyan-600/10 rounded-full blur-3xl"></div>
        </div>
        
        <div className="relative z-10 w-full max-w-md">
        <Card className="w-full bg-white/80 dark:bg-slate-900/80 backdrop-blur-xl border-white/60 dark:border-slate-700/60">
          <CardHeader className="space-y-1 text-center">
            <div className="mx-auto mb-4 flex h-16 w-16 items-center justify-center rounded-full bg-green-100 dark:bg-green-900/20">
              <CheckCircle2 className="h-10 w-10 text-green-600 dark:text-green-500" />
            </div>
            <CardTitle className="text-2xl">Email đã được gửi!</CardTitle>
            <CardDescription className="text-base">
              Chúng tôi đã gửi hướng dẫn đặt lại mật khẩu đến email của bạn
            </CardDescription>
          </CardHeader>

          <CardContent className="space-y-6">
            {/* Email Info */}
            <div className="bg-accent rounded-lg p-4 flex items-center gap-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-full bg-background">
                <Mail className="h-5 w-5 text-primary" />
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-sm text-muted-foreground">Email được gửi đến</p>
                <p className="font-medium truncate">{email}</p>
              </div>
            </div>

            {/* Instructions */}
            <div className="space-y-3">
              <h3 className="font-medium">Bước tiếp theo:</h3>
              <ol className="space-y-2 text-sm text-muted-foreground">
                <li className="flex gap-2">
                  <span className="flex h-5 w-5 items-center justify-center rounded-full bg-primary/10 text-primary shrink-0">
                    1
                  </span>
                  <span>Kiểm tra hộp thư đến của bạn</span>
                </li>
                <li className="flex gap-2">
                  <span className="flex h-5 w-5 items-center justify-center rounded-full bg-primary/10 text-primary shrink-0">
                    2
                  </span>
                  <span>Mở email từ BookStore</span>
                </li>
                <li className="flex gap-2">
                  <span className="flex h-5 w-5 items-center justify-center rounded-full bg-primary/10 text-primary shrink-0">
                    3
                  </span>
                  <span>Nhấp vào liên kết đặt lại mật khẩu</span>
                </li>
                <li className="flex gap-2">
                  <span className="flex h-5 w-5 items-center justify-center rounded-full bg-primary/10 text-primary shrink-0">
                    4
                  </span>
                  <span>Tạo mật khẩu mới và đăng nhập</span>
                </li>
              </ol>
            </div>

            {/* Actions */}
            <div className="space-y-3 pt-4">
              <Button 
                className="w-full"
                onClick={() => navigate('/login')}
              >
                Quay lại đăng nhập
              </Button>

              <Button 
                variant="outline"
                className="w-full"
                onClick={() => navigate('/forgot-password', { replace: true })}
              >
                Gửi lại email
              </Button>
            </div>

            {/* Help Text */}
            <div className="border-t pt-4 space-y-2">
              <p className="text-sm text-muted-foreground text-center">
                Không nhận được email?
              </p>
              <ul className="text-xs text-muted-foreground space-y-1">
                <li>• Kiểm tra thư mục spam hoặc rác</li>
                <li>• Đảm bảo bạn đã nhập đúng địa chỉ email</li>
                <li>• Email có thể mất vài phút để đến</li>
              </ul>
            </div>
          </CardContent>
        </Card>
        
        {/* Footer Info */}
        <div className="mt-8 text-center text-sm text-muted-foreground">
          <p>Cần trợ giúp? Liên hệ{' '}
            <a href="mailto:support@bookstore.com" className="text-primary hover:underline">
              support@bookstore.com
            </a>
          </p>
        </div>
        </div>
      </div>
    </div>
  );
};
