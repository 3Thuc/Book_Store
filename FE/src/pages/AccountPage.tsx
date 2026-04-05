import React, { useState, useMemo, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { ArrowLeft, Package, Star, Calendar, Award, Edit3, User, Key, Eye, EyeOff, RotateCcw, CreditCard, ShoppingBag, Clock, CheckCircle2, DollarSign, MapPin, Phone, Mail, Settings, Plus, Trash2, Check, XCircle } from 'lucide-react';
import { OrderWorkflowService } from '../utils/orderWorkflowService';
import { OrderService } from '../services/orderService';
import { OrderStatus } from '../types/order';
import { Button } from '../components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Badge } from '../components/ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/tabs';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '../components/ui/dialog';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from '../components/ui/alert-dialog';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Textarea } from '../components/ui/textarea';
import { Separator } from '../components/ui/separator';
import { Avatar, AvatarFallback, AvatarImage } from '../components/ui/avatar';
import { Header } from '../layouts/Header';
import { Footer } from '../layouts/Footer';
import { BookRecommendations } from '../features/book/BookRecommendations';
import { ReviewForm } from '../features/book/ReviewForm';
import { ImageWithFallback } from '../components/fallbackimg/ImageWithFallback';
import { useAuth } from '../context/AuthContext';
import PageLoader from '../components/PageLoader';
import { useOrder } from '../context/OrderContext';
import { books } from '../data/books';
import { Book } from '../types/book';
import { OrderItem } from '../types/order';
import { toast } from 'sonner';
import { formatVND } from '../lib/formatters';
import { calculatePasswordStrength } from '../utils/passwordStrength';

const exampleImage: string | undefined = undefined;

interface AccountPageProps {
  onBack: () => void;
  onCartClick: () => void;
  onSearch: (query: string) => void;
  onLogoClick: () => void;
  onLoginClick: () => void;
  onBookClick: (book: Book) => void;
}

export const AccountPage: React.FC<AccountPageProps> = ({
  onBack,
  onCartClick,
  onSearch,
  onLogoClick,
  onLoginClick,
  onBookClick
}) => {
  const { user, addresses, orders, isLoadingOrders, changePass, updateProfile, addAddress, updateAddress, deleteAddress, setDefaultAddress, refreshOrders, initializing } = useAuth();
  const { getPurchasedBooks, reviews } = useOrder();
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState<string>('info');
  const [isChangePasswordOpen, setIsChangePasswordOpen] = useState<boolean>(false);
  const [isEditInfoOpen, setIsEditInfoOpen] = useState<boolean>(false);
  const [isAddAddressOpen, setIsAddAddressOpen] = useState<boolean>(false);
  const [editingAddress, setEditingAddress] = useState<any>(null);
  const [passwordData, setPasswordData] = useState({
    currentPassword: '',
    newPassword: '',
    confirmPassword: ''
  });
  const [showPasswords, setShowPasswords] = useState({
    current: false,
    new: false,
    confirm: false
  });
  const [editData, setEditData] = useState({
    fullName: user?.fullName || '',
    userName: user?.userName || '',
    email: user?.email || '',
    phoneNumber: user?.phoneNumber || ''
  });
  const [addressData, setAddressData] = useState({
    recipientName: '',
    recipientPhone: '',
    address: '',
    district: '',
    city: '',
    isDefault: false
  });
  const [selectedReviewItem, setSelectedReviewItem] = useState<{ item: OrderItem; orderId: string } | null>(null);
  const [editingReviewId, setEditingReviewId] = useState<string | null>(null);
  const [selectedOrder, setSelectedOrder] = useState<any>(null);
  const [isOrderDetailOpen, setIsOrderDetailOpen] = useState<boolean>(false);
  const [confirmDialog, setConfirmDialog] = useState<{
    open: boolean;
    title: string;
    description: string;
    onConfirm: () => void;
    confirmText?: string;
    isDestructive?: boolean;
  }>({ open: false, title: '', description: '', onConfirm: () => {} });
  const [returnReasonDialog, setReturnReasonDialog] = useState<{
    open: boolean;
    orderId: string;
  }>({ open: false, orderId: '' });
  const [returnReason, setReturnReason] = useState('');
  const [orderFilter, setOrderFilter] = useState<'all' | 'pending' | 'processing' | 'shipped' | 'delivered' | 'cancelled' | 'return'>('all');

  // Backend already filters orders by current user, no need to filter again
  const allOrders = orders;
  
  // Filter orders based on selected tab
  const userOrders = useMemo(() => {
    switch (orderFilter) {
      case 'pending':
        return allOrders.filter(o => o.status === 'PENDING');
      case 'processing':
        return allOrders.filter(o => ['PROCESSING', 'CONFIRMED'].includes(o.status));
      case 'shipped':
        return allOrders.filter(o => o.status === 'SHIPPED');
      case 'delivered':
        return allOrders.filter(o => o.status === 'DELIVERED');
      case 'cancelled':
        return allOrders.filter(o => ['CANCEL_REQUESTED', 'CANCELLED'].includes(o.status));
      case 'return':
        return allOrders.filter(o => ['RETURN_REQUESTED', 'RETURNED'].includes(o.status));
      default:
        return allOrders;
    }
  }, [allOrders, orderFilter]);
  const purchasedBooks = getPurchasedBooks();

  const newPasswordStrength = useMemo(
    () => calculatePasswordStrength(passwordData.newPassword),
    [passwordData.newPassword]
  );

  const stats = {
    totalOrders: allOrders.length,
    completedOrders: allOrders.filter(o => o.status === 'DELIVERED').length,
    pendingOrders: allOrders.filter(o => ['PENDING', 'PAID', 'PROCESSING', 'CONFIRMED', 'SHIPPED'].includes(o.status)).length,
    totalSpent: allOrders.reduce((sum, order) => sum + order.totalAmount, 0)
  };

    useEffect(() => {
      if (!initializing && !user) {
        navigate('/login');
      }
    }, [initializing, user, navigate]);

    useEffect(() => {
      if (user && !initializing) {
        refreshOrders();
      }
    }, [user, initializing]);
  
    if (initializing) {
      return <PageLoader />;
    }
  
    if (!user) {
      return null;
    }



  const getInitials = (name?: string): string => {
    if (!name || !name.trim()) return 'U';

    return name
      .trim()
      .split(' ')
      .map(n => n[0])
      .filter(Boolean)
      .join('')
      .toUpperCase()
      .slice(0, 2) || 'U';
  };

  const handleOpenEditInfo = () => {
    setEditData({
      fullName: user?.fullName || '',
      userName: user?.userName || '',
      email: user?.email || '',
      phoneNumber: user?.phoneNumber || ''
    });
    setIsEditInfoOpen(true);
  };

  const handleOpenAddAddress = () => {
    setAddressData({
      recipientName: user?.fullName || '',
      recipientPhone: user?.phoneNumber || '',
      address: '',
      district: '',
      city: '',
      isDefault: addresses.length === 0
    });
    setEditingAddress(null);
    setIsAddAddressOpen(true);
  };

  const handleEditAddress = (address: any) => {
    setAddressData({
      recipientName: address.recipientName,
      recipientPhone: address.recipientPhone,
      address: address.address,
      district: address.district || '',
      city: address.city || '',
      isDefault: address.isDefault
    });
    setEditingAddress(address);
    setIsAddAddressOpen(true);
  };

  // Get recommended books based on purchase history
  const getRecommendedBooks = (): Book[] => {
    if (purchasedBooks.length === 0) return books.slice(0, 10) as unknown as Book[];

    // Get categories from purchased books
    const purchasedCategories = [...new Set(
      purchasedBooks.map(book => {
        const fullBook = books.find(b => b.id === book.bookId);
        return fullBook?.category;
      }).filter(Boolean)
    )];

    // Recommend books from same categories, excluding already purchased
    const purchasedBookIds = purchasedBooks.map(book => book.bookId);
    return (books
      .filter(book =>
        purchasedCategories.includes(book.category) &&
        !purchasedBookIds.includes(book.id)
      )
      .slice(0, 10)) as unknown as Book[];
  };

  const formatPrice = (price: number) => formatVND(price);

  const formatDate = (date: Date | string | null | undefined): string => {
    if (!date) return 'N/A';
    try {
      const dateObj = typeof date === 'string' ? new Date(date) : date;
      if (isNaN(dateObj.getTime())) return 'N/A';
      return new Intl.DateTimeFormat('vi-VN', {
        year: 'numeric',
        month: 'long',
        day: 'numeric'
      }).format(dateObj);
    } catch (e) {
      return 'N/A';
    }
  };

  const getStatusColor = (status: OrderStatus): string => {
    switch (status) {
      case 'PENDING': return 'bg-yellow-100 text-yellow-800';
      case 'PAID': return 'bg-blue-100 text-blue-800';
      case 'PROCESSING': return 'bg-blue-100 text-blue-800';
      case 'CONFIRMED': return 'bg-green-100 text-green-800';
      case 'SHIPPED': return 'bg-indigo-100 text-indigo-800';
      case 'DELIVERED': return 'bg-green-200 text-green-900';
      case 'CANCEL_REQUESTED': return 'bg-red-50 text-red-700';
      case 'CANCELLED': return 'bg-red-100 text-red-800';
      case 'RETURN_REQUESTED': return 'bg-orange-100 text-orange-800';
      case 'RETURNED': return 'bg-orange-200 text-orange-900';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const getStatusText = (status: OrderStatus): string => {
    switch (status) {
      case 'PENDING': return 'Chờ xử lý';
      case 'PAID': return 'Đã thanh toán';
      case 'PROCESSING': return 'Đang xử lý';
      case 'CONFIRMED': return 'Đã xác nhận';
      case 'SHIPPED': return 'Đang giao hàng';
      case 'DELIVERED': return 'Đã giao hàng';
      case 'CANCEL_REQUESTED': return 'Chờ duyệt hủy';
      case 'CANCELLED': return 'Đã hủy';
      case 'RETURN_REQUESTED': return 'Yêu cầu trả hàng';
      case 'RETURNED': return 'Đã trả hàng';
      default: return 'Chưa xác định';
    }
  };

  const handlePasswordChange = (field: keyof typeof passwordData, value: string) => {
    setPasswordData(prev => ({
      ...prev,
      [field]: value
    }));
  };

  const togglePasswordVisibility = (field: keyof typeof showPasswords) => {
    setShowPasswords(prev => ({
      ...prev,
      [field]: !prev[field]
    }));
  };

  const handleChangePassword = async () => {
    if (!passwordData.currentPassword) {
      toast.error('Vui lòng nhập mật khẩu hiện tại');
      return;
    }
    if (!passwordData.newPassword) {
      toast.error('Vui lòng nhập mật khẩu mới');
      return;
    }
    if (passwordData.newPassword !== passwordData.confirmPassword) {
      toast.error('Mật khẩu xác nhận không khớp');
      return;
    }
    if (passwordData.currentPassword === passwordData.newPassword) {
      toast.error('Mật khẩu mới phải khác mật khẩu hiện tại');
      return;
    }
    if (newPasswordStrength.level !== 'strong') {
      toast.error('Mật khẩu mới chưa đủ mạnh. Vui lòng chọn mật khẩu mạnh (ít nhất 12 ký tự, chữ hoa, chữ thường, số và ký tự đặc biệt).');
      return;
    }
    let success = false;
    try {
      success = await changePass(passwordData.newPassword, passwordData.currentPassword);

      if (success) {
        toast.success('Đổi mật khẩu thành công');
        setIsChangePasswordOpen(false);
        setPasswordData({
          currentPassword: '',
          newPassword: '',
          confirmPassword: ''
        });
        setShowPasswords({
          current: false,
          new: false,
          confirm: false
        });
      }
    } catch (err: any) {
      toast.error(err?.response?.data?.message || 'Đổi mật khẩu thất bại');
    }
  };

  const handleEditInfoChange = (field: keyof typeof editData, value: string) => {
    setEditData(prev => ({
      ...prev,
      [field]: value
    }));
  };

  const handleSaveInfo = async () => {
    if (!editData.fullName.trim()) {
      toast.error('Vui lòng nhập họ và tên');
      return;
    }

    if (!editData.email.trim()) {
      toast.error('Vui lòng nhập email');
      return;
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(editData.email)) {
      toast.error('Email không hợp lệ');
      return;
    }

    const success = await updateProfile(editData);
    if (success) {
      toast.success('Cập nhật thông tin thành công');
      setIsEditInfoOpen(false);
    } else {
      toast.error('Cập nhật thông tin thất bại');
    }
  };

  const handleSaveAddress = async () => {
    // Kiểm tra tất cả trường không được null hoặc rỗng
    if (!addressData.recipientName.trim() ||
        !addressData.recipientPhone.trim() ||
        !addressData.address.trim() ||
        !addressData.district.trim() ||
        !addressData.city.trim()) {
      toast.error('Vui lòng nhập đầy đủ tất cả các trường');
      return;
    }

    if (editingAddress) {
      const success = await updateAddress({
        id: editingAddress.id,
        ...addressData
      });

      if (success) {
        toast.success('Cập nhật địa chỉ thành công');
        setIsAddAddressOpen(false);
        setEditingAddress(null);
      } else {
        toast.error('Cập nhật địa chỉ thất bại');
      }
    } else {
      const newAddress = await addAddress(addressData);

      if (newAddress) {
        toast.success('Thêm địa chỉ thành công');
        setIsAddAddressOpen(false);
      } else {
        toast.error('Thêm địa chỉ thất bại');
      }
    }
  };

  const handleDeleteAddress = async (addressId: string) => {
    if (addresses.length === 1) {
      toast.error('Bạn phải có ít nhất một địa chỉ');
      return;
    }

    setConfirmDialog({
      open: true,
      title: 'Xóa địa chỉ',
      description: 'Bạn có chắc muốn xóa địa chỉ này? Hành động này không thể hoàn tác.',
      confirmText: 'Xóa',
      isDestructive: true,
      onConfirm: async () => {
        const success = await deleteAddress(addressId);
        if (success) {
          toast.success('Xóa địa chỉ thành công');
        } else {
          toast.error('Xóa địa chỉ thất bại');
        }
        setConfirmDialog({ ...confirmDialog, open: false });
      }
    });
  };

  const handleSetDefaultAddress = async (addressId: string) => {
    const success = await setDefaultAddress(addressId);
    if (success) {
      toast.success('Đã đặt làm địa chỉ mặc định');
    } else {
      toast.error('Không thể đặt địa chỉ mặc định');
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-black">
      {/* Header */}
      <Header
        onSearch={onSearch}
        onCartClick={onCartClick}
        onLogoClick={onLogoClick}
        onLoginClick={onLoginClick}
        onAccountClick={() => { }}
      />

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Back Button */}


        {/* Profile Header Card */}
        <Card className="mb-8 bg-gradient-to-r from-primary via-primary/95 to-primary/90 text-white dark:text-black overflow-hidden relative border-0">
          {/* Decorative Background Elements */}
          <div className="absolute inset-0 overflow-hidden opacity-10">
            <div className="absolute top-10 right-10 w-40 h-40 border border-white/30 dark:border-black/30 rounded-full"></div>
            <div className="absolute bottom-10 left-10 w-32 h-32 border border-white/20 dark:border-black/20 rounded-full"></div>
            <div className="absolute top-1/2 left-1/3 w-24 h-24 border border-white/25 dark:border-black/25 rounded-full"></div>
          </div>

          <CardContent className="p-8 relative z-10">
            <div className="flex flex-col md:flex-row items-center md:items-start gap-6">
              {/* Avatar */}
              <Avatar className="h-24 w-24 border-4 border-white/20 dark:border-black/20 shadow-lg">
                <AvatarImage src={exampleImage} alt={user.fullName} />
                <AvatarFallback className="bg-white/20 dark:bg-black/20 text-white dark:text-black text-2xl">
                  {getInitials(user.fullName)}
                </AvatarFallback>
              </Avatar>

              {/* User Info */}
              <div className="flex-1 text-center md:text-left">
                <h1 className="text-3xl font-bold mb-2">{user.fullName}</h1>
                <div className="space-y-1 text-white/90 dark:text-black/90">
                  <div className="flex items-center justify-center md:justify-start gap-2">
                    <User className="h-4 w-4" />
                    <span className="text-sm">@{user.userName}</span>
                  </div>
                  <div className="flex items-center justify-center md:justify-start gap-2">
                    <Mail className="h-4 w-4" />
                    <span className="text-sm">{user.email}</span>
                  </div>
                  {user.phoneNumber && (
                    <div className="flex items-center justify-center md:justify-start gap-2">
                      <Phone className="h-4 w-4" />
                      <span className="text-sm">{user.phoneNumber}</span>
                    </div>
                  )}
                </div>
              </div>

              {/* Quick Actions */}
              <div className="flex flex-wrap gap-2 justify-center md:justify-end">
                <Button
                  variant="outline"
                  size="sm"
                  className="bg-white/10 dark:bg-black/10 border-white/20 dark:border-black/20 text-white dark:text-black hover:!bg-black hover:!text-white dark:hover:!bg-white dark:hover:!text-black"
                  onClick={handleOpenEditInfo}
                >
                  <Edit3 className="h-4 w-4 mr-2" />
                  Chỉnh sửa
                </Button>

                <Dialog open={isChangePasswordOpen} onOpenChange={setIsChangePasswordOpen}>
                  <DialogTrigger asChild>
                    <Button variant="outline" size="sm" className="bg-white/10 dark:bg-black/10 border-white/20 dark:border-black/20 text-white dark:text-black hover:bg-black hover:text-white dark:hover:bg-white dark:hover:text-black">
                      <Key className="h-4 w-4 mr-2" />
                      Đổi mật khẩu
                    </Button>
                  </DialogTrigger>
                  <DialogContent className="sm:max-w-[425px]">
                    <DialogHeader>
                      <DialogTitle className="flex items-center gap-2">
                        <Key className="h-5 w-5" />
                        Đổi mật khẩu
                      </DialogTitle>
                      <DialogDescription>
                        Nhập mật khẩu hiện tại và mật khẩu mới để thay đổi
                      </DialogDescription>
                    </DialogHeader>

                    <div className="space-y-4 py-4">
                      {/* Current Password */}
                      <div className="space-y-2">
                        <Label htmlFor="currentPassword">Mật khẩu hiện tại</Label>
                        <div className="relative">
                          <Input
                            id="currentPassword"
                            type={showPasswords.current ? "text" : "password"}
                            value={passwordData.currentPassword}
                            onChange={(e) => handlePasswordChange('currentPassword', e.target.value)}
                            placeholder="Nhập mật khẩu hiện tại"
                            className="pr-10"
                          />
                          <Button
                            type="button"
                            variant="ghost"
                            size="sm"
                            className="absolute right-0 top-0 h-full px-3 py-2 hover:bg-transparent"
                            onClick={() => togglePasswordVisibility('current')}
                          >
                            {showPasswords.current ? (
                              <EyeOff className="h-4 w-4" />
                            ) : (
                              <Eye className="h-4 w-4" />
                            )}
                          </Button>
                        </div>
                      </div>

                      {/* New Password */}
                      <div className="space-y-2">
                        <Label htmlFor="newPassword">Mật khẩu mới</Label>
                        <div className="relative">
                          <Input
                            id="newPassword"
                            type={showPasswords.new ? "text" : "password"}
                            value={passwordData.newPassword}
                            onChange={(e) => handlePasswordChange('newPassword', e.target.value)}
                            placeholder="Nhập mật khẩu mới (ít nhất 6 ký tự)"
                            className="pr-10"
                          />
                          <Button
                            type="button"
                            variant="ghost"
                            size="sm"
                            className="absolute right-0 top-0 h-full px-3 py-2 hover:bg-transparent"
                            onClick={() => togglePasswordVisibility('new')}
                          >
                            {showPasswords.new ? (
                              <EyeOff className="h-4 w-4" />
                            ) : (
                              <Eye className="h-4 w-4" />
                            )}
                          </Button>
                        </div>

                        {/* Password Strength Indicator */}
                        {passwordData.newPassword && (
                          <div className="mt-2">
                            <div className="flex items-center gap-2">
                              {/* Progress Bar Container */}
                              <div className="flex-1 bg-slate-200 rounded-full overflow-hidden" style={{ height: '6px' }}>
                                <div
                                  className="rounded-full transition-all duration-300 ease-out"
                                  style={{
                                    width: `${newPasswordStrength.score}%`,
                                    height: '100%',
                                    backgroundColor: newPasswordStrength.color
                                  }}
                                />
                              </div>
                              {/* Strength Text */}
                              <span
                                className="text-xs transition-colors duration-300"
                                style={{ color: newPasswordStrength.color }}
                              >
                                {newPasswordStrength.text}
                              </span>
                            </div>
                          </div>
                        )}
                      </div>

                      {/* Confirm Password */}
                      <div className="space-y-2">
                        <Label htmlFor="confirmPassword">Xác nhận mật khẩu mới</Label>
                        <div className="relative">
                          <Input
                            id="confirmPassword"
                            type={showPasswords.confirm ? "text" : "password"}
                            value={passwordData.confirmPassword}
                            onChange={(e) => handlePasswordChange('confirmPassword', e.target.value)}
                            placeholder="Nhập lại mật khẩu mới"
                            className="pr-10"
                          />
                          <Button
                            type="button"
                            variant="ghost"
                            size="sm"
                            className="absolute right-0 top-0 h-full px-3 py-2 hover:bg-transparent"
                            onClick={() => togglePasswordVisibility('confirm')}
                          >
                            {showPasswords.confirm ? (
                              <EyeOff className="h-4 w-4" />
                            ) : (
                              <Eye className="h-4 w-4" />
                            )}
                          </Button>
                        </div>
                      </div>
                    </div>

                    <DialogFooter>
                      <Button
                        variant="outline"
                        onClick={() => setIsChangePasswordOpen(false)}
                      >
                        Hủy
                      </Button>
                      <Button onClick={handleChangePassword}>
                        Đổi mật khẩu
                      </Button>
                    </DialogFooter>
                  </DialogContent>
                </Dialog>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Stats Cards */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
          <Card>
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-muted-foreground mb-1">Tổng đơn hàng</p>
                  <p className="text-3xl font-bold text-foreground">{stats.totalOrders}</p>
                </div>
                <div className="w-12 h-12 rounded-full bg-blue-100 dark:bg-blue-900/30 flex items-center justify-center">
                  <ShoppingBag className="h-6 w-6 text-blue-600 dark:text-blue-400" />
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-muted-foreground mb-1">Hoàn thành</p>
                  <p className="text-3xl font-bold text-green-600 dark:text-green-400">{stats.completedOrders}</p>
                </div>
                <div className="w-12 h-12 rounded-full bg-green-100 dark:bg-green-900/30 flex items-center justify-center">
                  <CheckCircle2 className="h-6 w-6 text-green-600 dark:text-green-400" />
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-muted-foreground mb-1">Đang xử lý</p>
                  <p className="text-3xl font-bold text-amber-600 dark:text-amber-400">{stats.pendingOrders}</p>
                </div>
                <div className="w-12 h-12 rounded-full bg-amber-100 dark:bg-amber-900/30 flex items-center justify-center">
                  <Clock className="h-6 w-6 text-amber-600 dark:text-amber-400" />
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-muted-foreground mb-1">Tổng chi tiêu</p>
                  <p className="text-2xl font-bold text-purple-600 dark:text-purple-400">{formatPrice(stats.totalSpent)}</p>
                </div>
                <div className="w-12 h-12 rounded-full bg-purple-100 dark:bg-purple-900/30 flex items-center justify-center">
                  <DollarSign className="h-6 w-6 text-purple-600 dark:text-purple-400" />
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Content Tabs */}
        <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-0">
          <TabsList className="grid w-full grid-cols-3 mb-8">
            <TabsTrigger
              value="info"
              className="flex items-center gap-2 data-[state=active]:bg-white data-[state=active]:text-black hover:bg-gray-200 dark:hover:bg-gray-700"
            >
              <User className="h-4 w-4" />
              Thông tin
            </TabsTrigger>
            <TabsTrigger
              value="addresses"
              className="flex items-center gap-2 data-[state=active]:bg-white data-[state=active]:text-black hover:bg-gray-200 dark:hover:bg-gray-700"
            >
              <MapPin className="h-4 w-4" />
              Địa chỉ
            </TabsTrigger>
            <TabsTrigger
              value="orders"
              className="flex items-center gap-2 data-[state=active]:bg-white data-[state=active]:text-black hover:bg-gray-200 dark:hover:bg-gray-700"
            >
              <Package className="h-4 w-4" />
              Đơn hàng
            </TabsTrigger>
          </TabsList>

          {/* Personal Information Tab */}
          <TabsContent value="info" className="space-y-6">
            <Card>
              <CardHeader className="border-b">
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle className="flex items-center gap-2">
                      <User className="h-5 w-5 text-primary" />
                      Thông tin cá nhân
                    </CardTitle>
                    <p className="text-sm text-muted-foreground mt-1">
                      Cập nhật thông tin để có trải nghiệm tốt hơn
                    </p>
                  </div>
                  <Button
                    variant="outline"
                    size="sm"
                    className="flex items-center gap-2 hover:bg-black hover:text-white hover:border-black dark:hover:bg-white dark:hover:text-black dark:hover:border-white transition-colors"
                    onClick={handleOpenEditInfo}
                  >
                    <Edit3 className="h-4 w-4" />
                    Chỉnh sửa
                  </Button>
                </div>
              </CardHeader>

              <CardContent className="p-8">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                  <div className="space-y-2">
                    <div className="flex items-center gap-2 text-muted-foreground mb-2">
                      <User className="h-4 w-4" />
                      <label className="text-sm font-medium">Họ và tên</label>
                    </div>
                    <p className="text-foreground font-medium text-lg">{user.fullName}</p>
                  </div>

                  <div className="space-y-2">
                    <div className="flex items-center gap-2 text-muted-foreground mb-2">
                      <User className="h-4 w-4" />
                      <label className="text-sm font-medium">Tên người dùng</label>
                    </div>
                    <p className="text-foreground font-medium text-lg">{user.userName}</p>
                  </div>

                  <div className="space-y-2">
                    <div className="flex items-center gap-2 text-muted-foreground mb-2">
                      <Mail className="h-4 w-4" />
                      <label className="text-sm font-medium">Email</label>
                    </div>
                    <p className="text-foreground font-medium text-lg">{user.email}</p>
                  </div>

                  <div className="space-y-2">
                    <div className="flex items-center gap-2 text-muted-foreground mb-2">
                      <Phone className="h-4 w-4" />
                      <label className="text-sm font-medium">Số điện thoại</label>
                    </div>
                    {user.phoneNumber ? (
                      <p className="text-foreground font-medium text-lg">{user.phoneNumber}</p>
                    ) : (
                      <>
                        <p className="text-muted-foreground italic">Chưa cập nhật</p>
                        <Button
                          variant="link"
                          size="sm"
                          className="p-0 h-auto text-primary"
                          onClick={handleOpenEditInfo}
                        >
                          Thêm số điện thoại
                        </Button>
                      </>
                    )}
                  </div>
                </div>

                <Separator className="my-8" />

                {/* Account Security */}
                <div>
                  <h3 className="font-semibold mb-4 flex items-center gap-2">
                    <Key className="h-5 w-5 text-primary" />
                    Bảo mật tài khoản
                  </h3>
                  <div className="space-y-4">
                    <div className="flex items-center justify-between p-4 border rounded-lg hover:bg-accent transition-colors">
                      <div>
                        <p className="font-medium">Mật khẩu</p>
                        <p className="text-sm text-muted-foreground">Đảm bảo mật khẩu mạnh và an toàn</p>
                      </div>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => setIsChangePasswordOpen(true)}
                      >
                        Đổi mật khẩu
                      </Button>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          {/* Addresses Tab */}
          <TabsContent value="addresses" className="space-y-6">
            <Card>
              <CardHeader className="border-b">
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle className="flex items-center gap-2">
                      <MapPin className="h-5 w-5 text-primary" />
                      Địa chỉ nhận hàng
                    </CardTitle>
                    <p className="text-sm text-muted-foreground mt-1">
                      Quản lý các địa chỉ giao hàng của bạn
                    </p>
                  </div>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handleOpenAddAddress}
                    className="flex items-center gap-2 hover:bg-black hover:text-white hover:border-black dark:hover:bg-white dark:hover:text-black dark:hover:border-white transition-colors"
                  >
                    <Plus className="h-4 w-4" />
                    Thêm địa chỉ mới
                  </Button>
                </div>
              </CardHeader>

              <CardContent className="p-6">
                {addresses.length > 0 ? (
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    {addresses.map((address) => (
                      <Card key={address.id} className="relative border-2 hover:border-primary/50 transition-colors">
                        <CardContent className="p-6">
                          {address.isDefault && (
                            <Badge className="absolute top-4 right-4 bg-primary">
                              Mặc định
                            </Badge>
                          )}

                          <div className="space-y-3 mb-4">
                            <div className="flex items-start gap-2">
                              <User className="h-4 w-4 text-muted-foreground mt-1" />
                              <div>
                                <p className="font-semibold text-foreground">{address.recipientName}</p>
                                <p className="text-sm text-muted-foreground">{address.recipientPhone}</p>
                              </div>
                            </div>

                            <div className="flex items-start gap-2">
                              <MapPin className="h-4 w-4 text-muted-foreground mt-1" />
                              <div>
                                <p className="text-sm text-foreground">{address.address}</p>
                                {(address.district || address.city) && (
                                  <p className="text-sm text-muted-foreground">
                                    {[address.district, address.city].filter(Boolean).join(', ')}
                                  </p>
                                )}
                              </div>
                            </div>
                          </div>

                          <Separator className="my-4" />

                          <div className="flex gap-2">
                            {!address.isDefault && (
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={() => handleSetDefaultAddress(address.id)}
                                className="flex-1 items-center gap-2 hover:bg-black hover:text-white hover:border-black dark:hover:bg-white dark:hover:text-black dark:hover:border-white transition-colors"
                              >
                                <Check className="h-4 w-4" />
                                Đặt mặc định
                              </Button>
                            )}
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => handleEditAddress(address)}
                              className="flex-1 items-center gap-2 hover:bg-black hover:text-white hover:border-black dark:hover:bg-white dark:hover:text-black dark:hover:border-white transition-colors"
                            >

                              <Edit3 className="h-4 w-4" />
                              Sửa
                            </Button>
                            {addresses.length > 1 && (
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={() => handleDeleteAddress(address.id)}
                                className="border-red-200 text-red-600 hover:bg-red-50 dark:border-red-900 dark:text-red-400 dark:hover:bg-red-950 flex items-center gap-2"
                              >
                                <Trash2 className="h-4 w-4" />
                              </Button>
                            )}
                          </div>
                        </CardContent>
                      </Card>
                    ))}
                  </div>
                ) : (
                  <div className="text-center py-12">
                    <div className="w-24 h-24 rounded-full bg-muted flex items-center justify-center mx-auto mb-6">
                      <MapPin className="h-12 w-12 text-primary" />
                    </div>
                    <h3 className="text-xl font-semibold mb-2">Chưa có địa chỉ nào</h3>
                    <p className="text-muted-foreground mb-6">
                      Thêm địa chỉ nhận hàng để tiện cho việc đặt hàng
                    </p>
                    <Button onClick={handleOpenAddAddress} className="gap-2">
                      <Plus className="h-5 w-5" />
                      Thêm địa chỉ đầu tiên
                    </Button>
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          {/* Orders Tab */}
          <TabsContent value="orders" className="space-y-6">
            {/* Order Filter Tabs */}
            <Card>
              <CardContent className="p-4">
                <div className="flex flex-wrap gap-2">
                  <Button
                    variant={orderFilter === 'all' ? 'default' : 'outline'}
                    size="sm"
                    onClick={() => setOrderFilter('all')}
                    className="gap-2"
                  >
                    <Package className="h-4 w-4" />
                    Tất cả ({allOrders.length})
                  </Button>
                  <Button
                    variant={orderFilter === 'pending' ? 'default' : 'outline'}
                    size="sm"
                    onClick={() => setOrderFilter('pending')}
                    className="gap-2"
                  >
                    <Clock className="h-4 w-4" />
                    Chờ xử lý ({allOrders.filter(o => o.status === 'PENDING').length})
                  </Button>
                  <Button
                    variant={orderFilter === 'processing' ? 'default' : 'outline'}
                    size="sm"
                    onClick={() => setOrderFilter('processing')}
                    className="gap-2"
                  >
                    <ShoppingBag className="h-4 w-4" />
                    Đang xử lý ({allOrders.filter(o => ['PROCESSING', 'CONFIRMED'].includes(o.status)).length})
                  </Button>
                  <Button
                    variant={orderFilter === 'shipped' ? 'default' : 'outline'}
                    size="sm"
                    onClick={() => setOrderFilter('shipped')}
                    className="gap-2"
                  >
                    <ShoppingBag className="h-4 w-4" />
                    Đang giao ({allOrders.filter(o => o.status === 'SHIPPED').length})
                  </Button>
                  <Button
                    variant={orderFilter === 'delivered' ? 'default' : 'outline'}
                    size="sm"
                    onClick={() => setOrderFilter('delivered')}
                    className="gap-2"
                  >
                    <CheckCircle2 className="h-4 w-4" />
                    Hoàn thành ({allOrders.filter(o => o.status === 'DELIVERED').length})
                  </Button>
                  <Button
                    variant={orderFilter === 'cancelled' ? 'default' : 'outline'}
                    size="sm"
                    onClick={() => setOrderFilter('cancelled')}
                    className="gap-2"
                  >
                    <XCircle className="h-4 w-4" />
                    Đã hủy ({allOrders.filter(o => ['CANCEL_REQUESTED', 'CANCELLED'].includes(o.status)).length})
                  </Button>
                  <Button
                    variant={orderFilter === 'return' ? 'default' : 'outline'}
                    size="sm"
                    onClick={() => setOrderFilter('return')}
                    className="gap-2"
                  >
                    <RotateCcw className="h-4 w-4" />
                    Trả hàng ({allOrders.filter(o => ['RETURN_REQUESTED', 'RETURNED'].includes(o.status)).length})
                  </Button>
                </div>
              </CardContent>
            </Card>

            {userOrders.length > 0 ? (
              userOrders.map((order) => (
                <Card 
                  key={order.id} 
                  className="overflow-hidden cursor-pointer hover:shadow-lg transition-shadow"
                  onClick={() => {
                    setSelectedOrder(order);
                    setIsOrderDetailOpen(true);
                  }}
                >
                  <CardHeader className="flex flex-row items-center justify-between border-b">
                    <div>
                      <CardTitle className="text-lg flex items-center gap-2">
                        <Package className="h-5 w-5 text-primary" />
                        Đơn hàng #{order.id.slice(-8)}
                      </CardTitle>
                      <p className="text-sm text-muted-foreground mt-1">
                        Đặt ngày: {formatDate(order.orderDate)}
                      </p>
                    </div>
                    <Badge className={`${getStatusColor(order.status)} px-3 py-1`}>
                      {getStatusText(order.status)}
                    </Badge>
                  </CardHeader>
                  <CardContent className="p-6">
                    <div className="space-y-4">
                        {order.items.map((item: OrderItem) => (
                        <div key={item.id} className="flex items-start space-x-4 p-4 border rounded-lg hover:bg-accent transition-colors">
                          <div className="w-20 h-28 flex-shrink-0 rounded-lg overflow-hidden shadow-md">
                          <ImageWithFallback
                            src={item.imageUrl}
                            alt={item.title}
                            className="w-full h-full object-cover"
                          />
                          </div>
                          <div className="flex-1 min-w-0">
                          <h4 className="font-semibold truncate mb-1 text-foreground">{item.title}</h4>
                          <p className="text-sm text-muted-foreground mb-1 flex items-center gap-1">
                            <User className="h-3 w-3" />
                            {item.author}
                          </p>
                          <div className="flex items-center gap-4 mt-2">
                            <p className="text-sm text-muted-foreground">
                            Số lượng: <span className="font-medium text-foreground">{item.quantity}</span>
                            </p>
                            <Separator orientation="vertical" className="h-4" />
                            <p className="text-sm font-semibold text-primary">
                            {formatPrice(item.price * item.quantity)}
                            </p>
                          </div>

                          {/* Review button - only show if order is completed */}
                          {order.status === 'DELIVERED' && (
                            <div className="mt-3">
                            {item.isReviewed ? (
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  const userReview = reviews.find(
                                    r => r.book_id === item.bookId && r.user_id === user?.id
                                  );
                                  if (userReview) {
                                    setSelectedReviewItem({ item, orderId: order.id });
                                    setEditingReviewId(userReview.rating_id);
                                  }
                                }}
                                className="bg-green-100 dark:bg-green-900 text-green-700 dark:text-green-200 border-green-300 dark:border-green-700 hover:bg-green-200 dark:hover:bg-green-800 hover:text-green-700 dark:hover:text-green-200 gap-2 transition-colors"
                              >
                                <Star className="h-4 w-4 fill-green-600 dark:fill-green-400" />
                                Đã đánh giá (sửa)
                              </Button>
                            ) : (
                              <Button
                              variant="outline"
                              size="sm"
                              onClick={(e) => {
                                e.stopPropagation();
                                setEditingReviewId(null);
                                setSelectedReviewItem({ item, orderId: order.id });
                              }}
                              className="gap-2"
                              >
                              <Star className="h-4 w-4" />
                              Viết đánh giá
                              </Button>
                            )}
                            </div>
                          )}
                          </div>
                        </div>
                        ))}


                      <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 pt-4 border-t bg-muted/50 -mx-6 -mb-6 px-6 py-4 mt-6">
                        <div className="flex items-center space-x-2 text-sm text-muted-foreground">
                          <Calendar className="h-4 w-4 text-primary" />
                          <span>Giao hàng dự kiến: <span className="font-medium text-foreground">{formatDate(order.deliveryDate)}</span></span>
                        </div>
                        <div className="text-right">
                          <p className="text-sm text-muted-foreground">Tổng cộng</p>
                          <p className="text-2xl font-bold text-primary">
                            {formatPrice(order.totalAmount)}
                          </p>
                        </div>
                      </div>


                      {/* Action Buttons */}
                      <div className="flex flex-wrap justify-end gap-2 pt-4">
                          {/* Confirm delivery and Return - for SHIPPED */}
                          {order.status === 'SHIPPED' && (
                            <>
                              <Button
                                variant="default"
                                size="sm"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setConfirmDialog({
                                    open: true,
                                    title: 'Xác nhận đã nhận hàng',
                                    description: 'Bạn xác nhận đã nhận được hàng? Đơn hàng sẽ được chuyển sang trạng thái hoàn thành.',
                                    confirmText: 'Đã nhận hàng',
                                    onConfirm: async () => {
                                      try {
                                        await OrderService.confirmDelivery(order.id);
                                        await refreshOrders();
                                        setConfirmDialog({ ...confirmDialog, open: false });
                                        toast.success('Xác nhận đã nhận hàng thành công');
                                      } catch (error: any) {
                                        toast.error(error?.response?.data?.message || 'Xác nhận thất bại');
                                        setConfirmDialog({ ...confirmDialog, open: false });
                                      }
                                    }
                                  });
                                }}
                              >
                                Đã nhận hàng
                              </Button>
                              <Button
                                variant="outline"
                                size="sm"
                                className="gap-2"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setReturnReasonDialog({ open: true, orderId: order.id });
                                  setReturnReason('');
                                }}
                              >
                                <RotateCcw className="h-4 w-4" />
                                Trả hàng
                              </Button>
                            </>
                          )}

                          {/* Request return - for DELIVERED */}
                          {order.status === 'DELIVERED' && (
                            <Button
                              variant="outline"
                              size="sm"
                              className="gap-2"
                              onClick={(e) => {
                                e.stopPropagation();
                                setReturnReasonDialog({ open: true, orderId: order.id });
                                setReturnReason('');
                              }}
                            >
                              <RotateCcw className="h-4 w-4" />
                              Trả hàng
                            </Button>
                          )}

                          {/* Return requested status */}
                          {order.status === 'RETURN_REQUESTED' && (
                            <div className="text-sm text-orange-700 dark:text-orange-400 bg-orange-50 dark:bg-orange-950 px-3 py-2 rounded border border-orange-200 dark:border-orange-800">
                              Yêu cầu trả hàng đang được xử lý
                            </div>
                          )}

                          {/* Cancel order button */}
                          {(order.status === 'PENDING' || order.status === 'PROCESSING') && (
                          <Button
                            variant="destructive"
                            size="sm"
                            onClick={(e) => {
                              e.stopPropagation();
                              setConfirmDialog({
                                open: true,
                                title: 'Hủy đơn hàng',
                                description: 'Bạn có chắc muốn hủy đơn hàng này? Hành động này không thể hoàn tác.',
                                confirmText: 'Hủy đơn hàng',
                                isDestructive: true,
                                onConfirm: async () => {
                                  try {
                                    await OrderService.cancelOrder(order.id);
                                    await refreshOrders();
                                    setConfirmDialog({ ...confirmDialog, open: false });
                                    toast.success('Đã gửi yêu cầu hủy đơn hàng');
                                  } catch (error: any) {
                                    toast.error(error?.response?.data?.message || 'Hủy đơn hàng thất bại');
                                    setConfirmDialog({ ...confirmDialog, open: false });
                                  }
                                }
                              });
                            }}
                          >
                            Hủy đơn hàng
                          </Button>
                          )}
                      </div>
                    </div>
                  </CardContent>
                </Card>
              ))
            ) : (
              <Card>
                <CardContent className="text-center py-16">
                  <div className="w-24 h-24 rounded-full bg-muted flex items-center justify-center mx-auto mb-6">
                    <Package className="h-12 w-12 text-primary" />
                  </div>
                  <h3 className="text-xl font-semibold mb-2">Chưa có đơn hàng nào</h3>
                  <p className="text-muted-foreground mb-6 max-w-md mx-auto">
                    Hãy khám phá và mua sắm những cuốn sách yêu thích của bạn để bắt đầu hành trình đọc thú vị
                  </p>
                  <Button onClick={onBack} size="lg" className="gap-2">
                    <ShoppingBag className="h-5 w-5" />
                    Khám phá sách ngay
                  </Button>
                </CardContent>
              </Card>
            )}
          </TabsContent>


        </Tabs>


      </div>

      <Footer />

      {/* Review Form Dialog */}
      {selectedReviewItem && (
        <ReviewForm
          item={selectedReviewItem.item}
          orderId={selectedReviewItem.orderId}
          isOpen={!!selectedReviewItem}
          onClose={() => {
            setSelectedReviewItem(null);
            setEditingReviewId(null);
          }}
          editingReviewId={editingReviewId || undefined}
        />
      )}

      {/* Edit Info Dialog */}
      <Dialog open={isEditInfoOpen} onOpenChange={setIsEditInfoOpen}>
        <DialogContent className="sm:max-w-[500px]">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <Edit3 className="h-5 w-5" />
              Chỉnh sửa thông tin
            </DialogTitle>
            <DialogDescription>
              Cập nhật thông tin cá nhân của bạn
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-4">
            {/* Full Name Field */}
            <div className="space-y-2">
              <Label htmlFor="editFullName" className="flex items-center gap-2">
                <User className="h-4 w-4" />
                Họ và tên
              </Label>
              <Input
                id="editFullName"
                type="text"
                value={editData.fullName}
                onChange={(e) => handleEditInfoChange('fullName', e.target.value)}
                placeholder="Nhập họ và tên"
              />
            </div>

            {/* Username Field */}
            <div className="space-y-2">
              <Label htmlFor="editUserName" className="flex items-center gap-2">
                <User className="h-4 w-4" />
                Tên người dùng
              </Label>
              <Input
                id="editUserName"
                type="text"
                value={editData.userName}
                onChange={(e) => handleEditInfoChange('userName', e.target.value)}
                placeholder="Nhập tên người dùng"
              />
            </div>

            {/* Email Field */}
            <div className="space-y-2">
              <Label htmlFor="editEmail" className="flex items-center gap-2">
                <Mail className="h-4 w-4" />
                Email
              </Label>
              <Input
                id="editEmail"
                type="email"
                value={editData.email}
                readOnly
                placeholder="Email (không thể thay đổi)"
                className="bg-muted cursor-not-allowed"
              />
            </div>

            {/* Phone Field */}
            <div className="space-y-2">
              <Label htmlFor="editPhone" className="flex items-center gap-2">
                <Phone className="h-4 w-4" />
                Số điện thoại
              </Label>
              <Input
                id="editPhone"
                type="tel"
                value={editData.phoneNumber}
                onChange={(e) => handleEditInfoChange('phoneNumber', e.target.value)}
                placeholder="Nhập số điện thoại (VD: 0912345678)"
              />
            </div>
          </div>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setIsEditInfoOpen(false)}
            >
              Hủy
            </Button>
            <Button onClick={handleSaveInfo}>
              Lưu thay đổi
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Order Detail Dialog */}
      <Dialog open={isOrderDetailOpen} onOpenChange={setIsOrderDetailOpen}>
        <DialogContent className="max-w-3xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <Package className="h-5 w-5" />
              Chi tiết đơn hàng #{selectedOrder?.id?.slice(-8)}
            </DialogTitle>
            <DialogDescription>
              Đặt ngày: {formatDate(selectedOrder?.orderDate)}
            </DialogDescription>
          </DialogHeader>

          {selectedOrder && (
            <div className="space-y-6 py-4">
              {/* Order Status */}
              <div className="flex items-center justify-between p-4 bg-muted rounded-lg">
                <span className="font-medium">Trạng thái đơn hàng:</span>
                <Badge className={`${getStatusColor(selectedOrder.status)} px-3 py-1`}>
                  {getStatusText(selectedOrder.status)}
                </Badge>
              </div>

              {/* Customer Info */}
              <div className="space-y-3">
                <h3 className="font-semibold text-lg border-b pb-2">Thông tin người nhận</h3>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <p className="text-sm text-muted-foreground">Tên người nhận</p>
                    <p className="font-medium">{selectedOrder.customerName || 'N/A'}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Số điện thoại</p>
                    <p className="font-medium">{selectedOrder.customerPhone || 'N/A'}</p>
                  </div>
                  <div className="col-span-2">
                    <p className="text-sm text-muted-foreground">Địa chỉ giao hàng</p>
                    <p className="font-medium">{selectedOrder.shippingAddress || 'N/A'}</p>
                  </div>
                </div>
              </div>

              {/* Order Items */}
              <div className="space-y-3">
                <h3 className="font-semibold text-lg border-b pb-2">Sản phẩm ({selectedOrder.items.length})</h3>
                <div className="space-y-3">
                  {selectedOrder.items.map((item: any) => (
                    <div key={item.id} className="flex items-start gap-4 p-3 border rounded-lg">
                      <div className="w-16 h-20 flex-shrink-0 rounded overflow-hidden">
                        <ImageWithFallback
                          src={item.imageUrl}
                          alt={item.title}
                          className="w-full h-full object-cover"
                        />
                      </div>
                      <div className="flex-1">
                        <h4 className="font-medium line-clamp-2">{item.title}</h4>
                        <p className="text-sm text-muted-foreground">{item.author}</p>
                        <div className="flex items-center justify-between mt-2">
                          <span className="text-sm">x{item.quantity}</span>
                          <span className="font-semibold text-primary">{formatPrice(item.price)}</span>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Payment Info */}
              <div className="space-y-3">
                <h3 className="font-semibold text-lg border-b pb-2">Thông tin thanh toán</h3>
                <div className="space-y-2">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Phương thức thanh toán:</span>
                    <span className="font-medium">
                      {selectedOrder.paymentMethod === 'COD' ? 'Thanh toán khi nhận hàng' : 'Chuyển khoản'}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Trạng thái thanh toán:</span>
                    <span className={`font-medium ${selectedOrder.isPaid ? 'text-green-600' : 'text-orange-600'}`}>
                      {selectedOrder.isPaid ? 'Đã thanh toán' : 'Chưa thanh toán'}
                    </span>
                  </div>
                  <div className="flex justify-between pt-3 border-t">
                    <span className="font-semibold text-lg">Tổng cộng:</span>
                    <span className="font-bold text-xl text-primary">{formatPrice(selectedOrder.totalAmount)}</span>
                  </div>
                </div>
              </div>

              {/* Note */}
              {selectedOrder.note && (
                <div className="space-y-2">
                  <h3 className="font-semibold text-lg border-b pb-2">Ghi chú</h3>
                  <p className="text-muted-foreground">{selectedOrder.note}</p>
                </div>
              )}
            </div>
          )}

          <DialogFooter>
            <Button variant="outline" onClick={() => setIsOrderDetailOpen(false)}>
              Đóng
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Add/Edit Address Dialog */}
      <Dialog open={isAddAddressOpen} onOpenChange={setIsAddAddressOpen}>
        <DialogContent className="sm:max-w-[500px]">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <MapPin className="h-5 w-5" />
              {editingAddress ? 'Chỉnh sửa địa chỉ' : 'Thêm địa chỉ mới'}
            </DialogTitle>
            <DialogDescription>
              {editingAddress ? 'Cập nhật thông tin địa chỉ nhận hàng' : 'Thêm địa chỉ nhận hàng mới'}
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-4">
            {/* Recipient Name */}
            <div className="space-y-2">
              <Label htmlFor="recipientName">Tên người nhận *</Label>
              <Input
                id="recipientName"
                type="text"
                value={addressData.recipientName}
                onChange={(e) => setAddressData({ ...addressData, recipientName: e.target.value })}
                placeholder="Nhập tên người nhận"
              />
            </div>

            {/* Recipient Phone */}
            <div className="space-y-2">
              <Label htmlFor="recipientPhone">Số điện thoại *</Label>
              <Input
                id="recipientPhone"
                type="tel"
                value={addressData.recipientPhone}
                onChange={(e) => setAddressData({ ...addressData, recipientPhone: e.target.value })}
                placeholder="Nhập số điện thoại (VD: 0912345678)"
              />
            </div>

            {/* Address */}
            <div className="space-y-2">
              <Label htmlFor="address">Địa chỉ *</Label>
              <Input
                id="address"
                type="text"
                value={addressData.address}
                onChange={(e) => setAddressData({ ...addressData, address: e.target.value })}
                placeholder="Số nhà, tên đường"
              />
            </div>

            {/* District */}
            <div className="space-y-2">
              <Label htmlFor="district">Quận/Huyện</Label>
              <Input
                id="district"
                type="text"
                value={addressData.district}
                onChange={(e) => setAddressData({ ...addressData, district: e.target.value })}
                placeholder="Nhập quận/huyện"
              />
            </div>

            {/* City */}
            <div className="space-y-2">
              <Label htmlFor="city">Tỉnh/Thành phố</Label>
              <Input
                id="city"
                type="text"
                value={addressData.city}
                onChange={(e) => setAddressData({ ...addressData, city: e.target.value })}
                placeholder="Nhập tỉnh/thành phố"
              />
            </div>

            {/* Set as Default */}
            <div className="flex items-center space-x-2">
              <input
                type="checkbox"
                id="isDefault"
                checked={addressData.isDefault}
                onChange={(e) => setAddressData({ ...addressData, isDefault: e.target.checked })}
                className="h-4 w-4 rounded border-gray-300"
              />
              <Label htmlFor="isDefault" className="cursor-pointer">
                Đặt làm địa chỉ mặc định
              </Label>
            </div>
          </div>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setIsAddAddressOpen(false);
                setEditingAddress(null);
              }}
            >
              Hủy
            </Button>
            <Button onClick={handleSaveAddress}>
              {editingAddress ? 'Cập nhật' : 'Thêm địa chỉ'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Confirmation Dialog */}
      <AlertDialog open={confirmDialog.open} onOpenChange={(open) => setConfirmDialog({ ...confirmDialog, open })}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{confirmDialog.title}</AlertDialogTitle>
            <AlertDialogDescription>
              {confirmDialog.description}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={() => setConfirmDialog({ ...confirmDialog, open: false })}>
              Hủy
            </AlertDialogCancel>
            <AlertDialogAction 
              onClick={confirmDialog.onConfirm}
              className={confirmDialog.isDestructive ? "bg-red-600 hover:bg-red-700" : ""}
            >
              {confirmDialog.confirmText || 'Xác nhận'}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Return Reason Dialog */}
      <Dialog open={returnReasonDialog.open} onOpenChange={(open) => setReturnReasonDialog({ ...returnReasonDialog, open })}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Yêu cầu trả hàng</DialogTitle>
            <DialogDescription>
              Vui lòng cho biết lý do bạn muốn trả hàng. Điều này giúp chúng tôi cải thiện dịch vụ.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <Label htmlFor="return-reason">Lý do trả hàng</Label>
              <Textarea
                id="return-reason"
                placeholder="Nhập lý do trả hàng..."
                value={returnReason}
                onChange={(e) => setReturnReason(e.target.value)}
                rows={4}
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => {
              setReturnReasonDialog({ open: false, orderId: '' });
              setReturnReason('');
            }}>
              Hủy
            </Button>
            <Button 
              onClick={async () => {
                if (!returnReason.trim()) {
                  toast.error('Vui lòng nhập lý do trả hàng');
                  return;
                }
                try {
                  await OrderService.returnOrder(returnReasonDialog.orderId, { reason: returnReason });
                  await refreshOrders();
                  setReturnReasonDialog({ open: false, orderId: '' });
                  setReturnReason('');
                  toast.success('Đã gửi yêu cầu trả hàng');
                } catch (error: any) {
                  toast.error(error?.response?.data?.message || 'Gửi yêu cầu thất bại');
                }
              }}
              disabled={!returnReason.trim()}
            >
              Gửi yêu cầu
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};
