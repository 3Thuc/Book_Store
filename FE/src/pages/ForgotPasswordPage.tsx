import React, { useState } from 'react';
import { ArrowLeft, Mail, CheckCircle2, Book } from 'lucide-react';
import { Button } from '../components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Alert, AlertDescription } from '../components/ui/alert';
import { ThemeToggle } from '../components/ThemeToggle';
import { useNavigate } from 'react-router-dom';
import { toast } from 'sonner';
import AuthService from '../services/authService';

export const ForgotPasswordPage: React.FC = () => {
  const navigate = useNavigate();
  const [email, setEmail] = useState('');
  const [error, setError] = useState('');
  const [isLoading, setIsLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');

    if (!email.trim()) {
      setError('Vui lòng nhập địa chỉ email');
      return;
    }

    // Validate email format
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      setError('Địa chỉ email không hợp lệ');
      return;
    }

    setIsLoading(true);

    try {
      const resp = await AuthService.forgotPassword(email);

      const successMsg = resp?.result?.message || resp?.message || 'Yêu cầu đặt lại mật khẩu đã được gửi. Vui lòng kiểm tra email.';
      toast.success(successMsg);

      navigate('/reset-password-success', {
        state: { email },
        replace: true,
      });
    } catch (err: any) {
      const msg = err?.response?.data?.message || err?.message || 'Đã xảy ra lỗi, vui lòng thử lại';
      setError(msg);
    } finally {
      setIsLoading(false);
    }
  };

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
            <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-full bg-primary/10">
              <Mail className="h-6 w-6 text-primary" />
            </div>
            <CardTitle className="text-2xl">Quên mật khẩu?</CardTitle>
            <CardDescription>
              Nhập địa chỉ email của bạn và chúng tôi sẽ gửi hướng dẫn đặt lại mật khẩu
            </CardDescription>
          </CardHeader>

          <CardContent>
            <form onSubmit={handleSubmit} className="space-y-4">
              {error && (
                <Alert variant="destructive">
                  <AlertDescription>{error}</AlertDescription>
                </Alert>
              )}

              <div className="space-y-2">
                <Label htmlFor="email">Địa chỉ email</Label>
                <div className="relative">
                  <Mail className="absolute left-3 top-3 h-4 w-4 text-muted-foreground" />
                  <Input
                    id="email"
                    type="email"
                    placeholder="example@email.com"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    className="pl-10"
                    disabled={isLoading}
                    autoFocus
                  />
                </div>
              </div>

              <Button 
                type="submit" 
                className="w-full"
                disabled={isLoading}
              >
                {isLoading ? (
                  <>
                    <div className="mr-2 h-4 w-4 animate-spin rounded-full border-2 border-background border-t-transparent" />
                    Đang xử lý...
                  </>
                ) : (
                  'Gửi yêu cầu'
                )}
              </Button>

              <div className="text-center text-sm text-muted-foreground">
                Nhớ mật khẩu?{' '}
                <button
                  type="button"
                  onClick={() => navigate('/login')}
                  className="text-primary hover:underline"
                >
                  Đăng nhập ngay
                </button>
              </div>
            </form>
          </CardContent>
        </Card>
        
        {/* Back to Login Link */}
        <div className="mt-6 text-center">
          <Button
            variant="ghost"
            onClick={() => navigate('/login')}
            className="text-muted-foreground hover:text-primary hover:bg-slate-100"
          >
            <ArrowLeft className="h-4 w-4 mr-2" />
            Quay lại đăng nhập
          </Button>
        </div>
        
        {/* Footer Info */}
        <div className="mt-8 text-center text-sm text-muted-foreground">
          <p> Mẹo: Kiểm tra cả thư mục spam nếu không thấy email</p>
        </div>
        </div>
      </div>
    </div>
  );
};
