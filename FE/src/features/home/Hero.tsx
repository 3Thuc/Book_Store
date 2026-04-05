import React from 'react';
import { Button } from '../../components/ui/button';
import { BookOpen } from 'lucide-react';
import { ImageWithFallback } from '../../components/fallbackimg/ImageWithFallback';

export const Hero: React.FC = () => {
  const scrollToBooks = () => {
    document.getElementById('books-section')?.scrollIntoView({ behavior: 'smooth' });
  };

  return (
    <section className="py-20 px-4 sm:px-6 lg:px-8 bg-gradient-to-br from-primary/5 to-secondary/10">
      <div className="max-w-7xl mx-auto">
        <div className="grid lg:grid-cols-2 gap-12 items-center">
          <div className="text-center lg:text-left">
            <h1 className="text-4xl md:text-5xl lg:text-6xl font-bold text-foreground mb-6">
              Khám phá thế giới 
              <span className="text-primary block">tri thức</span>
            </h1>
            <p className="text-lg text-muted-foreground mb-8 max-w-xl">
              Tìm kiếm và khám phá hàng nghìn cuốn sách hay từ nhiều thể loại khác nhau. 
              Đặt hàng dễ dàng và nhận sách tận nơi.
            </p>
            <div className="flex flex-col sm:flex-row gap-4 justify-center lg:justify-start">
              <Button size="lg" onClick={scrollToBooks} className="text-lg px-8">
                <BookOpen className="mr-2 h-5 w-5" />
                Khám phá ngay
              </Button>
            </div>
          </div>
          
          <div className="relative">
            <div className="aspect-square rounded-2xl overflow-hidden shadow-2xl">
              <ImageWithFallback
                src="https://images.unsplash.com/photo-1652305489491-789257d2e95c?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxib29rcyUyMGxpYnJhcnklMjByZWFkaW5nfGVufDF8fHx8MTc1OTE4Nzk3NHww&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral"
                alt="Thư viện sách"
                className="w-full h-full object-cover"
              />
            </div>
            
            <div className="absolute -bottom-4 -right-4 bg-white dark:bg-slate-900 p-4 rounded-lg shadow-lg">
              <div className="text-center">
                <p className="text-2xl font-bold text-primary">1000+</p>
                <p className="text-sm text-gray-600 dark:text-gray-300">Đầu sách</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
};
