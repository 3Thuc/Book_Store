import React, { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { toast } from 'sonner';
import { Search, ShoppingCart, Menu, X, Book, User, LogOut, ChevronDown, Shield } from 'lucide-react';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Badge } from '../components/ui/badge';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger, DropdownMenuSeparator } from '../components/ui/dropdown-menu';
import { ThemeToggle } from '../components/ThemeToggle';
import { ImageSearchPanel } from '../components/ImageSearchPanel';
import { useCart } from '../context/CartContext';
import { useAuth } from '../context/AuthContext';
import { useScrollPosition } from '../hooks/useScrollPosition';
import { PythonSearchService } from '../services';

interface HeaderProps {
  onSearch?: (query: string) => void;
  onCartClick?: () => void;
  onLogoClick?: () => void;
  onLoginClick?: () => void;
  onAccountClick?: () => void;
}

export const Header: React.FC<HeaderProps> = ({ onSearch, onCartClick, onLogoClick, onLoginClick, onAccountClick }) => {
  const navigate = useNavigate();
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [suggestions, setSuggestions] = useState<any[]>([]);
  const [showSuggestions, setShowSuggestions] = useState<boolean>(false);
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState<boolean>(false);
  const searchRef = useRef<HTMLDivElement>(null);
  const { getTotalItems } = useCart();
  const { user, isLoggedIn, logout } = useAuth();
  const { isScrolled } = useScrollPosition();

  // Autocomplete suggestions
  useEffect(() => {
    const fetchSuggestions = async () => {
      if (searchQuery.trim().length < 2) {
        setSuggestions([]);
        return;
      }

      try {
        const results = await PythonSearchService.suggest(searchQuery, 8);
        setSuggestions(results);
      } catch (error) {
        console.error('Failed to fetch suggestions:', error);
        setSuggestions([]);
      }
    };

    const debounceTimer = setTimeout(fetchSuggestions, 300);
    return () => clearTimeout(debounceTimer);
  }, [searchQuery]);

  // Close suggestions when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (searchRef.current && !searchRef.current.contains(event.target as Node)) {
        setShowSuggestions(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    setShowSuggestions(false);
    onSearch?.(searchQuery);
  };

  const handleSuggestionClick = (suggestion: any) => {
    setSearchQuery(suggestion.title);
    setShowSuggestions(false);
    onSearch?.(suggestion.title);
  };

  const handleLogout = () => {
    logout();
    setIsMobileMenuOpen(false);
    toast.success('Đăng xuất thành công!', {
      description: 'Hẹn gặp lại bạn.'
    });
  };

  const handleAdminClick = () => {
    navigate('/admin');
    setIsMobileMenuOpen(false);
  };

  const isAdmin = user?.role === 'admin' || user?.role === 'staff';

  return (
    <header className={`sticky top-0 z-50 w-full transition-all duration-300 ${
      isScrolled 
        ? 'border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60 shadow-sm' 
        : 'border-transparent bg-transparent'
    }`}>
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex h-16 items-center justify-between">
          {/* Logo */}
          <div 
            className="flex items-center space-x-2 cursor-pointer"
            onClick={onLogoClick}
          >
            <Book className="h-8 w-8 text-primary" />
            <span className="text-xl font-bold text-foreground">BookStore</span>
          </div>

          {/* Desktop Search */}
          <div className="hidden md:flex flex-1 max-w-md mx-8 gap-2" ref={searchRef}>
            <form onSubmit={handleSearch} className="w-full relative">
              <Input
                type="text"
                placeholder="Tìm kiếm sách, tác giả..."
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setShowSuggestions(true);
                }}
                onFocus={() => setShowSuggestions(true)}
                className="w-full pl-4 pr-12"
              />
              <Button type="submit" size="sm" className="absolute right-1 top-1/2 transform -translate-y-1/2">
                <Search className="h-4 w-4" />
              </Button>

              {/* Autocomplete Suggestions */}
              {showSuggestions && suggestions.length > 0 && (
                <div className="absolute top-full left-0 right-0 mt-1 bg-background border border-border rounded-md shadow-lg max-h-80 overflow-y-auto z-50">
                  {suggestions.map((suggestion, index) => (
                    <div
                      key={index}
                      className="px-4 py-2 hover:bg-accent cursor-pointer border-b border-border last:border-b-0"
                      onClick={() => handleSuggestionClick(suggestion)}
                    >
                      <div className="font-medium text-sm">{suggestion.title}</div>
                      {suggestion.author_name && (
                        <div className="text-xs text-muted-foreground">{suggestion.author_name}</div>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </form>
            {/* Image Search Panel - OUTSIDE form */}
            <ImageSearchPanel />
          </div>

          {/* Desktop Actions */}
          <div className="hidden md:flex items-center space-x-2">
            {/* Theme Toggle */}
            <ThemeToggle />
            
            {/* Cart */}
            <Button
              variant="ghost"
              size="sm"
              className="relative"
              onClick={onCartClick}
            >
              <ShoppingCart className="h-5 w-5" />
              {getTotalItems() > 0 && (
                <Badge variant="destructive" className="absolute -top-2 -right-2 h-5 w-5 flex items-center justify-center p-0 text-xs">
                  {getTotalItems()}
                </Badge>
              )}
            </Button>

            {/* User Menu */}
            {isLoggedIn ? (
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="ghost" size="sm" className="flex items-center space-x-2">
                    <User className="h-5 w-5" />
                      <span className="max-w-24 truncate">{user?.userName || user?.fullName}</span>
                    <ChevronDown className="h-4 w-4" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end" className="w-48">
                    <div className="px-2 py-1.5 text-sm font-medium">{user?.userName || user?.fullName}</div>
                  <div className="px-2 py-1.5 text-xs text-muted-foreground">{user?.email}</div>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem onClick={onAccountClick}>
                    <User className="h-4 w-4 mr-2" />
                    Thông tin tài khoản
                  </DropdownMenuItem>
                  {isAdmin && (
                    <>
                      <DropdownMenuSeparator />
                      <DropdownMenuItem onClick={handleAdminClick}>
                        <Shield className="h-4 w-4 mr-2" />
                        Quản trị hệ thống
                      </DropdownMenuItem>
                    </>
                  )}
                  <DropdownMenuSeparator />
                  <DropdownMenuItem onClick={handleLogout} className="text-destructive">
                    <LogOut className="h-4 w-4 mr-2" />
                    Đăng xuất
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            ) : (
              <Button variant="ghost" size="sm" onClick={onLoginClick}>
                <User className="h-5 w-5 mr-2" />
                Đăng nhập
              </Button>
            )}
          </div>

          {/* Mobile Actions */}
          <div className="md:hidden flex items-center space-x-2">
            {/* Theme Toggle */}
            <ThemeToggle />
            
            {/* Cart */}
            <Button
              variant="ghost"
              size="sm"
              className="relative"
              onClick={onCartClick}
            >
              <ShoppingCart className="h-5 w-5" />
              {getTotalItems() > 0 && (
                <Badge variant="destructive" className="absolute -top-2 -right-2 h-5 w-5 flex items-center justify-center p-0 text-xs">
                  {getTotalItems()}
                </Badge>
              )}
            </Button>

            {/* Mobile Menu Toggle */}
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setIsMobileMenuOpen(!isMobileMenuOpen)}
            >
              {isMobileMenuOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
            </Button>
          </div>
        </div>

        {/* Mobile Search & Menu */}
        {isMobileMenuOpen && (
          <div className={`md:hidden border-t py-4 space-y-4 transition-all duration-300 ${
            isScrolled ? 'bg-background/95 backdrop-blur' : 'bg-background/90 backdrop-blur'
          }`}>
            {/* Mobile Search */}
            <form onSubmit={handleSearch} className="px-4">
              <div className="relative">
                <Input
                  type="text"
                  placeholder="Tìm kiếm sách, tác giả..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="w-full pl-4 pr-10"
                />
                <Button type="submit" size="sm" className="absolute right-1 top-1/2 transform -translate-y-1/2">
                  <Search className="h-4 w-4" />
                </Button>
              </div>
            </form>
            
            {/* Mobile Navigation */}
            <div className="px-4 space-y-2">
              {isLoggedIn ? (
                <div className="space-y-2">
                  <div className="p-3 bg-muted rounded-lg">
                    <p className="text-sm font-medium">{user?.userName || user?.fullName}</p>
                    <p className="text-xs text-muted-foreground">{user?.email}</p>
                  </div>
                  <Button 
                    variant="ghost" 
                    className="w-full justify-start"
                    onClick={() => {
                      onAccountClick?.();
                      setIsMobileMenuOpen(false);
                    }}
                  >
                    <User className="h-5 w-5 mr-2" />
                    Thông tin tài khoản
                  </Button>
                  {isAdmin && (
                    <Button 
                      variant="ghost" 
                      className="w-full justify-start"
                      onClick={handleAdminClick}
                    >
                      <Shield className="h-5 w-5 mr-2" />
                      Quản trị hệ thống
                    </Button>
                  )}
                  <Button 
                    variant="ghost" 
                    className="w-full justify-start text-destructive hover:text-destructive"
                    onClick={handleLogout}
                  >
                    <LogOut className="h-5 w-5 mr-2" />
                    Đăng xuất
                  </Button>
                </div>
              ) : (
                <Button 
                  variant="ghost" 
                  className="w-full justify-start"
                  onClick={() => {
                    onLoginClick?.();
                    setIsMobileMenuOpen(false);
                  }}
                >
                  <User className="h-5 w-5 mr-2" />
                  Đăng nhập
                </Button>
              )}
            </div>
          </div>
        )}
      </div>
    </header>
  );
};
