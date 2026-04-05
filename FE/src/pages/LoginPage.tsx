import React, { useState, useMemo } from 'react';
import { Book, Mail, Lock, Eye, EyeOff } from 'lucide-react';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { ThemeToggle } from '../components/ThemeToggle';
import { useAuth } from '../context/AuthContext';
import { useNavigate } from 'react-router-dom';
import { toast } from 'sonner';
import { calculatePasswordStrength } from '../utils/passwordStrength';

interface LoginPageProps {
  onLoginSuccess?: () => void;
  onLogoClick: () => void;
}

interface LoginData {
  email: string;
  password: string;
}

interface SignupData {
  name: string;
  email: string;
  password: string;
  confirmPassword: string;
}

export const LoginPage: React.FC<LoginPageProps> = ({ onLoginSuccess, onLogoClick }) => {
  const navigate = useNavigate();
  const [isSignIn, setIsSignIn] = useState<boolean>(true);
  const [loginData, setLoginData] = useState<LoginData>({ email: '', password: '' });
  const [signupData, setSignupData] = useState<SignupData>({
    name: '',
    email: '',
    password: '',
    confirmPassword: ''
  });
  const [showPassword, setShowPassword] = useState<boolean>(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState<boolean>(false);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const { login, signup, redirectToGoogle } = useAuth();

  const passwordStrength = useMemo(
    () => calculatePasswordStrength(signupData.password),
    [signupData.password]
  );

  const handleToggleForm = () => {
    setIsSignIn(!isSignIn);
  };

  const handleLogin = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setIsLoading(true);
    try {
      const success = await login(loginData.email, loginData.password);
      if (success) {
        toast.success('Đăng nhập thành công!');
        onLoginSuccess?.();
      } else {
        toast.error('Email hoặc mật khẩu không đúng');
      }
    } catch (error) {
    } finally {
      setIsLoading(false);
    }
  };

  const handleSignup = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();

    if (signupData.password !== signupData.confirmPassword) {
      toast.error('Mật khẩu xác nhận không khớp');
      return;
    }

    if (signupData.password.length < 6) {
      toast.error('Mật khẩu phải có ít nhất 6 ký tự');
      return;
    }

    if (passwordStrength.level !== 'strong') {
      toast.error('Mật khẩu chưa đủ mạnh. Vui lòng chọn mật khẩu mạnh (ít nhất 12 ký tự, chữ hoa, chữ thường, số và ký tự đặc biệt).');
      return;
    }

    setIsLoading(true);

    try {
      const success = await signup(signupData.name, signupData.email, signupData.password);
      if (success) {
        toast.success('Đăng ký thành công! Vui lòng kiểm tra email để xác nhận tài khoản.');
      }
    } catch (error) {
      toast.error('Có lỗi xảy ra khi đăng ký');
    } finally {
      setIsLoading(false);
    }
  };


  const handleSocialLogin = () => {
    toast.info('Tính năng đăng nhập mạng xã hội sẽ sớm được cập nhật');
  };

  return (
    <div className="min-h-screen flex flex-col bg-gradient-to-br from-slate-50 via-blue-50/30 to-purple-50/20 dark:from-slate-950 dark:via-blue-950/30 dark:to-purple-950/20">
      {/* Header */}
      <header className="border-b border-border bg-background/80 backdrop-blur-lg sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-3">
          <div className="flex items-center justify-between">
            <div
              className="flex items-center space-x-2 cursor-pointer"
              onClick={onLogoClick}
            >
              <Book className="h-8 w-8 text-primary" />
              <span className="text-xl font-bold text-foreground">BookStore</span>
            </div>
            <ThemeToggle />
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="flex-1 flex items-center justify-center p-4 md:p-6 lg:p-8 relative overflow-hidden">
        {/* Background Decorations */}
        <div className="absolute inset-0 overflow-hidden pointer-events-none">
          <div className="absolute -top-40 -right-40 w-80 h-80 bg-gradient-to-br from-purple-400/20 to-pink-400/20 dark:from-purple-600/10 dark:to-pink-600/10 rounded-full blur-3xl"></div>
          <div className="absolute -bottom-40 -left-40 w-80 h-80 bg-gradient-to-br from-blue-400/20 to-cyan-400/20 dark:from-blue-600/10 dark:to-cyan-600/10 rounded-full blur-3xl"></div>
        </div>

        <div className="w-full max-w-7xl relative z-10">
          {/* Login/Signup Container */}
          <div className="relative bg-white/80 dark:bg-slate-900/80 backdrop-blur-xl rounded-3xl shadow-2xl shadow-slate-900/10 dark:shadow-black/30 overflow-hidden border border-white/60 dark:border-slate-700/60" style={{ height: '520px' }}>

            {/* Sliding Overlay Panel */}
            <div
              className={`absolute top-0 bottom-0 w-1/2 bg-gradient-to-br from-primary via-primary/95 to-primary/90 transition-all duration-700 ease-in-out z-20 ${isSignIn ? 'left-1/2 rounded-l-[80px]' : 'left-0 rounded-r-[80px]'
                }`}
            >
              <div className="h-full flex flex-col items-center justify-center px-12 text-white dark:text-black relative overflow-hidden">
                {/* Decorative Elements */}
                <div className="absolute inset-0 overflow-hidden opacity-10">
                  <div className="absolute top-10 left-10 w-32 h-32 border border-white/30 dark:border-black/30 rounded-full"></div>
                  <div className="absolute bottom-10 right-10 w-40 h-40 border border-white/20 dark:border-black/20 rounded-full"></div>
                  <div className="absolute top-1/2 left-1/4 w-24 h-24 border border-white/25 dark:border-black/25 rounded-full"></div>
                </div>

                <div className="text-center space-y-4 relative z-10">
                  {/* Icon */}
                  <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-white/10 dark:bg-black/10 backdrop-blur-sm border border-white/20 dark:border-black/20 mb-2">
                    <Book className="h-8 w-8 text-white dark:text-black" />
                  </div>

                  {isSignIn ? (
                    <>
                      <h2 className="text-3xl">Chào mừng bạn!</h2>
                      <p className="text-white/90 dark:text-black/90 leading-relaxed px-4 text-sm">
                        Đăng ký tài khoản để bắt đầu khám phá và mua sắm hàng ngàn đầu sách hay
                      </p>
                      <Button
                        onClick={handleToggleForm}
                        variant="outline"
                        className="mt-4 h-11 px-10 bg-transparent border-2 border-white dark:border-black text-white dark:text-black hover:!bg-black hover:!text-white dark:hover:!bg-white dark:hover:!text-black transition-all duration-300 rounded-full"
                      >
                        Đăng ký ngay
                      </Button>
                    </>
                  ) : (
                    <>
                      <h2 className="text-3xl">Chào mừng trở lại!</h2>
                      <p className="text-white/90 dark:text-black/90 leading-relaxed px-4 text-sm">
                        Đăng nhập để tiếp tục khám phá kho sách và trải nghiệm mua sắm tuyệt vời
                      </p>
                      <Button
                        onClick={handleToggleForm}
                        variant="outline"
                        className="mt-4 h-11 px-10 bg-transparent border-2 border-white dark:border-black text-white dark:text-black hover:!bg-black hover:!text-white dark:hover:!bg-white dark:hover:!text-black transition-all duration-300 rounded-full"
                      >
                        Đăng nhập ngay
                      </Button>
                    </>
                  )}
                </div>
              </div>
            </div>

            {/* Forms Container */}
            <div className="relative h-full flex">

              {/* Sign In Form */}
              <div
                className={`absolute top-0 bottom-0 w-1/2 transition-all duration-700 ease-in-out ${isSignIn ? 'left-0 opacity-100 pointer-events-auto' : 'left-0 opacity-0 pointer-events-none'
                  }`}
              >
                <div className="h-full flex flex-col items-center justify-center px-12">
                  <div className="w-full max-w-sm space-y-5">
                    {/* Header */}
                    <div className="text-center space-y-1">
                      <h1 className="text-3xl bg-gradient-to-r from-primary via-primary/90 to-primary/80 bg-clip-text text-transparent">
                        Đăng nhập
                      </h1>
                      <p className="text-sm text-muted-foreground">
                        Chào mừng bạn quay lại với BookStore
                      </p>
                    </div>

                    <form onSubmit={handleLogin} className="space-y-3">
                      {/* Email Field */}
                      <div className="space-y-1">
                        <label className="text-sm text-foreground/80">Email</label>
                        <div className="relative group">
                          <Mail className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground z-10 group-focus-within:text-primary transition-colors" />
                          <Input
                            type="email"
                            placeholder="your.email@example.com"
                            value={loginData.email}
                            onChange={(e) => setLoginData({ ...loginData, email: e.target.value })}
                            required
                            className="h-10 pl-10 bg-slate-50/50 dark:bg-slate-800/50 border-slate-200 dark:border-slate-700 focus:bg-white dark:focus:bg-slate-900 focus:border-primary transition-all text-sm rounded-xl"
                          />
                        </div>
                      </div>

                      {/* Password Field */}
                      <div className="space-y-1">
                        <label className="text-sm text-foreground/80">Mật khẩu</label>
                        <div className="relative group">
                          <Lock className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground z-10 group-focus-within:text-primary transition-colors" />
                          <Input
                            type={showPassword ? 'text' : 'password'}
                            placeholder="••••••••"
                            value={loginData.password}
                            onChange={(e) => setLoginData({ ...loginData, password: e.target.value })}
                            required
                            className="h-10 pl-10 pr-10 bg-slate-50/50 border-slate-200 focus:bg-white focus:border-primary transition-all text-sm rounded-xl"
                          />
                          <button
                            type="button"
                            onClick={() => setShowPassword(!showPassword)}
                            className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground z-10 transition-colors"
                          >
                            {showPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                          </button>
                        </div>
                        <div className="flex justify-end">
                          <button
                            type="button"
                            onClick={() => navigate('/forgot-password')}
                            className="text-xs text-primary hover:underline transition-all"
                          >
                            Quên mật khẩu?
                          </button>
                        </div>
                      </div>

                      {/* Submit Button */}
                      <Button
                        type="submit"
                        className="w-full h-11 bg-gradient-to-r from-primary via-primary/95 to-primary/90 hover:from-primary/90 hover:to-primary/80 text-primary-foreground rounded-xl shadow-lg shadow-primary/20 hover:shadow-xl hover:shadow-primary/30 transition-all duration-300 mt-1"
                        disabled={isLoading}
                      >
                        {isLoading ? 'Đang đăng nhập...' : 'Đăng nhập'}
                      </Button>
                    </form>
                    {/* Divider */}
                    <div className="relative pt-1">
                      <div className="absolute inset-0 flex items-center">
                        <div className="w-full border-t border-slate-200 dark:border-slate-700"></div>
                      </div>
                      <div className="relative flex justify-center text-xs">
                        <span className="px-3 bg-white/80 dark:bg-slate-900/80 text-muted-foreground">hoặc đăng nhập với</span>
                      </div>
                    </div>
                    {/* Social Login Icons */}
                    <div className="flex justify-center">
                      <button
                        onClick={() => { redirectToGoogle(); }} 
                        type="button"
                        className="group w-full h-11 rounded-lg border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-800 hover:bg-slate-50 dark:hover:bg-slate-700 flex items-center justify-center gap-3 transition-all duration-300 hover:shadow-md"
                      >
                        <svg className="w-5 h-5" viewBox="0 0 24 24">
                          <path fill="#EA4335" d="M5.26620003,9.76452941 C6.19878754,6.93863203 8.85444915,4.90909091 12,4.90909091 C13.6909091,4.90909091 15.2181818,5.50909091 16.4181818,6.49090909 L19.9090909,3 C17.7818182,1.14545455 15.0545455,0 12,0 C7.27006974,0 3.1977497,2.69829785 1.23999023,6.65002441 L5.26620003,9.76452941 Z" />
                          <path fill="#34A853" d="M16.0407269,18.0125889 C14.9509167,18.7163016 13.5660892,19.0909091 12,19.0909091 C8.86648613,19.0909091 6.21911939,17.076871 5.27698177,14.2678769 L1.23746264,17.3349879 C3.19279051,21.2936293 7.26500293,24 12,24 C14.9328362,24 17.7353462,22.9573905 19.834192,20.9995801 L16.0407269,18.0125889 Z" />
                          <path fill="#4A90E2" d="M19.834192,20.9995801 C22.0291676,18.9520994 23.4545455,15.903663 23.4545455,12 C23.4545455,11.2909091 23.3454545,10.5272727 23.1818182,9.81818182 L12,9.81818182 L12,14.4545455 L18.4363636,14.4545455 C18.1187732,16.013626 17.2662994,17.2212117 16.0407269,18.0125889 L19.834192,20.9995801 Z" />
                          <path fill="#FBBC05" d="M5.27698177,14.2678769 C5.03832634,13.556323 4.90909091,12.7937589 4.90909091,12 C4.90909091,11.2182781 5.03443647,10.4668121 5.26620003,9.76452941 L1.23999023,6.65002441 C0.43658717,8.26043162 0,10.0753848 0,12 C0,13.9195484 0.444780743,15.7301709 1.23746264,17.3349879 L5.27698177,14.2678769 Z" />
                        </svg>
                        <span className="text-sm font-medium text-slate-700 dark:text-slate-300">Sign in with Google</span>
                      </button>
                    </div>
                  </div>
                </div>
              </div>

              {/* Sign Up Form */}
              <div
                className={`absolute top-0 bottom-0 w-1/2 transition-all duration-700 ease-in-out ${!isSignIn ? 'left-1/2 opacity-100 pointer-events-auto' : 'left-1/2 opacity-0 pointer-events-none'
                  }`}
              >
                <div className="h-full flex flex-col items-center justify-center px-12">
                  <div className="w-full max-w-sm space-y-4">
                    {/* Header */}
                    <div className="text-center space-y-1">
                      <h1 className="text-3xl bg-gradient-to-r from-primary via-primary/90 to-primary/80 bg-clip-text text-transparent">
                        Đăng ký
                      </h1>
                      <p className="text-sm text-muted-foreground">
                        Tạo tài khoản mới để bắt đầu
                      </p>
                    </div>

                    <form onSubmit={handleSignup} className="space-y-2">
                      {/* Name Field */}
                      <div className="space-y-1">
                        <label className="text-sm text-foreground/80">Họ và tên</label>
                        <Input
                          type="text"
                          placeholder="Nguyễn Văn A"
                          value={signupData.name}
                          onChange={(e) => setSignupData({ ...signupData, name: e.target.value })}
                          required
                          className="h-9 px-4 bg-slate-50/50 dark:bg-slate-800/50 border-slate-200 dark:border-slate-700 focus:bg-white dark:focus:bg-slate-900 focus:border-primary transition-all text-sm rounded-xl"
                        />
                      </div>

                      {/* Email Field */}
                      <div className="space-y-1">
                        <label className="text-sm text-foreground/80">Email</label>
                        <div className="relative group">
                          <Mail className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground z-10 group-focus-within:text-primary transition-colors" />
                          <Input
                            type="email"
                            placeholder="your.email@example.com"
                            value={signupData.email}
                            onChange={(e) => setSignupData({ ...signupData, email: e.target.value })}
                            required
                            className="h-9 pl-10 bg-slate-50/50 dark:bg-slate-800/50 border-slate-200 dark:border-slate-700 focus:bg-white dark:focus:bg-slate-900 focus:border-primary transition-all text-sm rounded-xl"
                          />
                        </div>
                      </div>

                      {/* Password Field */}
                      <div className="space-y-1">
                        <label className="text-sm text-foreground/80">Mật khẩu</label>
                        <div className="relative group">
                          <Lock className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground z-10 group-focus-within:text-primary transition-colors" />
                          <Input
                            type={showPassword ? 'text' : 'password'}
                            placeholder="••••••••"
                            value={signupData.password}
                            onChange={(e) => setSignupData({ ...signupData, password: e.target.value })}
                            required
                            className="h-9 pl-10 pr-10 bg-slate-50/50 border-slate-200 focus:bg-white focus:border-primary transition-all text-sm rounded-xl"
                          />
                          <button
                            type="button"
                            onClick={() => setShowPassword(!showPassword)}
                            className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground z-10 transition-colors"
                          >
                            {showPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                          </button>
                        </div>
                        {signupData.password && (
                          <div className="mt-2 space-y-1">
                            <div className="flex items-center gap-2">
                              <div className="flex-1 bg-slate-200 rounded-full overflow-hidden" style={{ height: '6px' }}>
                                <div
                                  className="rounded-full transition-all duration-300 ease-out"
                                  style={{
                                    width: `${passwordStrength.score}%`,
                                    height: '100%',
                                    backgroundColor: passwordStrength.color
                                  }}
                                />
                              </div>
                              <span
                                className="text-xs transition-colors duration-300"
                                style={{ color: passwordStrength.color }}
                              >
                                {passwordStrength.text}
                              </span>
                            </div>
                          </div>
                        )}
                      </div>

                      {/* Confirm Password Field */}
                      <div className="space-y-1">
                        <label className="text-sm text-foreground/80">Xác nhận mật khẩu</label>
                        <div className="relative group">
                          <Lock className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground z-10 group-focus-within:text-primary transition-colors" />
                          <Input
                            type={showConfirmPassword ? 'text' : 'password'}
                            placeholder="••••••••"
                            value={signupData.confirmPassword}
                            onChange={(e) => setSignupData({ ...signupData, confirmPassword: e.target.value })}
                            required
                            className="h-9 pl-10 pr-10 bg-slate-50/50 border-slate-200 focus:bg-white focus:border-primary transition-all text-sm rounded-xl"
                          />
                          <button
                            type="button"
                            onClick={() => setShowConfirmPassword(!showConfirmPassword)}
                            className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground z-10 transition-colors"
                          >
                            {showConfirmPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                          </button>
                        </div>
                      </div>

                      {/* Submit Button */}
                      <Button
                        type="submit"
                        className="w-full h-10 bg-gradient-to-r from-primary via-primary/95 to-primary/90 hover:from-primary/90 hover:to-primary/80 text-primary-foreground rounded-xl shadow-lg shadow-primary/20 hover:shadow-xl hover:shadow-primary/30 transition-all duration-300 mt-2"
                        disabled={isLoading}
                      >
                        {isLoading ? 'Đang đăng ký...' : 'Đăng ký'}
                      </Button>
                    </form>

                    {/* Divider */}
                    <div className="relative pt-1">
                      <div className="absolute inset-0 flex items-center">
                        <div className="w-full border-t border-slate-200 dark:border-slate-700"></div>
                      </div>
                      <div className="relative flex justify-center text-xs">
                        <span className="px-3 bg-white/80 dark:bg-slate-900/80 text-muted-foreground">hoặc đăng ký với</span>
                      </div>
                    </div>

                    {/* Social Signup Icons */}
                    <div className="flex justify-center">
                      <button
                        onClick={() => { redirectToGoogle(); }}
                        type="button"
                        className="group w-full h-11 rounded-lg border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-800 hover:bg-slate-50 dark:hover:bg-slate-700 flex items-center justify-center gap-3 transition-all duration-300 hover:shadow-md text-slate-700 dark:text-black"
                      >
                        <svg className="w-5 h-5" viewBox="0 0 24 24">
                          <path fill="#EA4335" d="M5.26620003,9.76452941 C6.19878754,6.93863203 8.85444915,4.90909091 12,4.90909091 C13.6909091,4.90909091 15.2181818,5.50909091 16.4181818,6.49090909 L19.9090909,3 C17.7818182,1.14545455 15.0545455,0 12,0 C7.27006974,0 3.1977497,2.69829785 1.23999023,6.65002441 L5.26620003,9.76452941 Z" />
                          <path fill="#34A853" d="M16.0407269,18.0125889 C14.9509167,18.7163016 13.5660892,19.0909091 12,19.0909091 C8.86648613,19.0909091 6.21911939,17.076871 5.27698177,14.2678769 L1.23746264,17.3349879 C3.19279051,21.2936293 7.26500293,24 12,24 C14.9328362,24 17.7353462,22.9573905 19.834192,20.9995801 L16.0407269,18.0125889 Z" />
                          <path fill="#4A90E2" d="M19.834192,20.9995801 C22.0291676,18.9520994 23.4545455,15.903663 23.4545455,12 C23.4545455,11.2909091 23.3454545,10.5272727 23.1818182,9.81818182 L12,9.81818182 L12,14.4545455 L18.4363636,14.4545455 C18.1187732,16.013626 17.2662994,17.2212117 16.0407269,18.0125889 L19.834192,20.9995801 Z" />
                          <path fill="#FBBC05" d="M5.27698177,14.2678769 C5.03832634,13.556323 4.90909091,12.7937589 4.90909091,12 C4.90909091,11.2182781 5.03443647,10.4668121 5.26620003,9.76452941 L1.23999023,6.65002441 C0.43658717,8.26043162 0,10.0753848 0,12 C0,13.9195484 0.444780743,15.7301709 1.23746264,17.3349879 L5.27698177,14.2678769 Z" />
                        </svg>
                        <span className="text-sm font-medium text-slate-700 dark:text-slate-300">Sign up with Google</span>
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Bottom Text */}
          <div className="text-center mt-6 space-y-4">
            <p className="text-sm text-muted-foreground">
              Bằng cách đăng nhập hoặc đăng ký, bạn đồng ý với{' '}
              <a href="#" onClick={(e) => e.preventDefault()} className="text-primary hover:underline">Điều khoản sử dụng</a>
              {' '}và{' '}
              <a href="#" onClick={(e) => e.preventDefault()} className="text-primary hover:underline">Chính sách bảo mật</a>
            </p>

          </div>
        </div>
      </main>
    </div>
  );
};
