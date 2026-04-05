import React, { useState } from 'react';
import { Book, Mail, Phone, MapPin, Facebook, Twitter, Instagram, HelpCircle } from 'lucide-react';
import { Input } from '../components/ui/input';
import { Button } from '../components/ui/button';
import { toast } from 'sonner';

export const Footer: React.FC = () => {
  const [newsletterEmail, setNewsletterEmail] = useState<string>('');

  const handleNewsletterSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (newsletterEmail) {
      toast.success('Đăng ký nhận tin thành công!');
      setNewsletterEmail('');
    }
  };

  return (
    <footer className="bg-[#030213] text-white relative">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-8 mb-8">
          {/* Company Info - Column 1 */}
          <div className="space-y-4">
            <div className="flex items-center gap-2">
              <Book className="h-6 w-6 text-white" />
              <span className="text-lg">BookStore</span>
            </div>
            <p className="text-white/70 text-sm leading-relaxed">
              Của hàng sách trực tuyến với sứ mệnh mang tri thức đến mọi người. Mang đến trải nghiệm mua sắm sách tốt nhất.
            </p>
            <div className="flex gap-3">
              <a 
                href="#" 
                onClick={(e) => e.preventDefault()}
                className="w-8 h-8 bg-white/10 rounded-full flex items-center justify-center hover:bg-white/20 transition-colors"
                aria-label="Facebook"
              >
                <Facebook className="h-4 w-4" />
              </a>
              <a 
                href="#" 
                onClick={(e) => e.preventDefault()}
                className="w-8 h-8 bg-white/10 rounded-full flex items-center justify-center hover:bg-white/20 transition-colors"
                aria-label="Twitter"
              >
                <Twitter className="h-4 w-4" />
              </a>
              <a 
                href="#" 
                onClick={(e) => e.preventDefault()}
                className="w-8 h-8 bg-white/10 rounded-full flex items-center justify-center hover:bg-white/20 transition-colors"
                aria-label="Instagram"
              >
                <Instagram className="h-4 w-4" />
              </a>
            </div>
          </div>

          {/* Quick Links - Column 2 */}
          <div className="space-y-4">
            <h3 className="text-white">Liên kết nhanh</h3>
            <ul className="space-y-2.5 text-sm">
              <li>
                <a href="#" onClick={(e) => e.preventDefault()} className="text-white/70 hover:text-white transition-colors">
                  Trang chủ
                </a>
              </li>
              <li>
                <a href="#" onClick={(e) => e.preventDefault()} className="text-white/70 hover:text-white transition-colors">
                  Sản phẩm mới
                </a>
              </li>
              <li>
                <a href="#" onClick={(e) => e.preventDefault()} className="text-white/70 hover:text-white transition-colors">
                  Sách bán chạy
                </a>
              </li>
              <li>
                <a href="#" onClick={(e) => e.preventDefault()} className="text-white/70 hover:text-white transition-colors">
                  Khuyến mãi
                </a>
              </li>
              <li>
                <a href="#" onClick={(e) => e.preventDefault()} className="text-white/70 hover:text-white transition-colors">
                  Về chúng tôi
                </a>
              </li>
            </ul>
          </div>

          {/* Categories - Column 3 */}
          <div className="space-y-4">
            <h3 className="text-white">Danh mục</h3>
            <ul className="space-y-2.5 text-sm">
              <li>
                <a href="#" onClick={(e) => e.preventDefault()} className="text-white/70 hover:text-white transition-colors">
                  Văn học
                </a>
              </li>
              <li>
                <a href="#" onClick={(e) => e.preventDefault()} className="text-white/70 hover:text-white transition-colors">
                  Kỹ năng sống
                </a>
              </li>
              <li>
                <a href="#" onClick={(e) => e.preventDefault()} className="text-white/70 hover:text-white transition-colors">
                  Công nghệ
                </a>
              </li>
              <li>
                <a href="#" onClick={(e) => e.preventDefault()} className="text-white/70 hover:text-white transition-colors">
                  Trinh thám
                </a>
              </li>
              <li>
                <a href="#" onClick={(e) => e.preventDefault()} className="text-white/70 hover:text-white transition-colors">
                  Lịch sử
                </a>
              </li>
            </ul>
          </div>

          {/* Contact & Newsletter - Column 4 */}
          <div className="space-y-4">
            <h3 className="text-white">Liên hệ</h3>
            <div className="space-y-2.5 text-sm">
              <div className="flex items-start gap-2 text-white/70">
                <Mail className="h-4 w-4 flex-shrink-0 mt-0.5" />
                <span>info@bookstore.vn</span>
              </div>
              <div className="flex items-start gap-2 text-white/70">
                <Phone className="h-4 w-4 flex-shrink-0 mt-0.5" />
                <span>1900 1000</span>
              </div>
              <div className="flex items-start gap-2 text-white/70">
                <MapPin className="h-4 w-4 flex-shrink-0 mt-0.5" />
                <span>123 Đường ABC, TP.HCM</span>
              </div>
            </div>

            {/* Newsletter */}
            <div className="pt-2">
              <h4 className="text-white text-sm mb-3">Đăng ký nhận tin</h4>
              <form onSubmit={handleNewsletterSubmit} className="flex gap-2">
                <Input
                  type="email"
                  placeholder="Email của bạn"
                  value={newsletterEmail}
                  onChange={(e) => setNewsletterEmail(e.target.value)}
                  className="flex-1 h-10 bg-white/10 border-white/20 text-white placeholder:text-white/50 text-sm focus:bg-white/15 focus:border-white/30"
                  required
                />
                <Button 
                  type="submit"
                  className="h-10 bg-white text-[#030213] hover:bg-white/90 px-5 text-sm"
                >
                  Đăng ký
                </Button>
              </form>
            </div>
          </div>
        </div>

        {/* Bottom Section */}
        <div className="border-t border-white/10 pt-6 flex flex-col md:flex-row justify-between items-center gap-4">
          <p className="text-white/60 text-sm">
            © 2025 BookStore. Tất cả quyền được bảo lưu.
          </p>
          <div className="flex gap-6 text-sm">
            <a href="#" onClick={(e) => e.preventDefault()} className="text-white/60 hover:text-white transition-colors">
              Điều khoản sử dụng
            </a>
            <a href="#" onClick={(e) => e.preventDefault()} className="text-white/60 hover:text-white transition-colors">
              Chính sách bảo mật
            </a>
            <a href="#" onClick={(e) => e.preventDefault()} className="text-white/60 hover:text-white transition-colors">
              Hỗ trợ
            </a>
          </div>
        </div>
      </div>

      {/* Help Button */}
      {/* <button
        type="button"
        className="fixed bottom-6 right-6 w-12 h-12 bg-white/20 backdrop-blur-sm rounded-full flex items-center justify-center hover:bg-white/30 transition-all shadow-lg z-50"
        onClick={() => toast.info('Tính năng hỗ trợ sẽ sớm được cập nhật')}
        aria-label="Help"
      >
        <HelpCircle className="h-6 w-6 text-white" />
      </button> */}
    </footer>
  );
};
