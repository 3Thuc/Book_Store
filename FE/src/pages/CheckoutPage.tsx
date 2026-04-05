import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { ArrowLeft, MapPin, CreditCard, Truck, AlertCircle, Plus, Home } from 'lucide-react';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { RadioGroup, RadioGroupItem } from '../components/ui/radio-group';
import { Textarea } from '../components/ui/textarea';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Separator } from '../components/ui/separator';
import { Alert, AlertDescription } from '../components/ui/alert';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogTrigger } from '../components/ui/dialog';
import { useCart } from '../context/CartContext';
import { useAuth } from '../context/AuthContext';
import { useOrder } from '../context/OrderContext';
import adminService from '../services/adminService';
import { apiRequest } from '../lib/api-client';
import { API_ENDPOINTS } from '../lib/constants';
import { Promotion } from '../features/admin/AdminContext';
import { toast } from 'sonner';
import { formatVND } from '../lib/formatters';
import { OrderService } from '../services/orderService';

type PaymentMethod = 'COD' | 'CreditCard';

interface CheckoutFormData {
  customerName: string;
  customerPhone: string;
  shippingAddress: string;
  city: string;
  district: string;
  ward: string;
  note: string;
  paymentMethod: PaymentMethod;
}

interface NewAddressFormData {
  name: string;
  phone: string;
  address: string;
  ward: string;
  district: string;
  city: string;
  isDefault: boolean;
}

const paymentMethods = [
  {
    id: 'COD' as PaymentMethod,
    name: 'Thanh toán khi nhận hàng (COD)',
    description: 'Thanh toán bằng tiền mặt khi nhận hàng',
    icon: Truck,
  },
  {
    id: 'CreditCard' as PaymentMethod,
    name: 'Chuyển khoản ngân hàng',
    description: 'Chuyển khoản qua tài khoản ngân hàng',
    icon: CreditCard,
  },
];

const vietnamCities = [
  'Hà Nội',
  'Hồ Chí Minh',
  'Đà Nẵng',
  'Hải Phòng',
  'Cần Thơ',
  'Biên Hòa',
  'Nha Trang',
  'Huế',
  'Vũng Tàu',
  'Buôn Ma Thuột',
];

export const CheckoutPage: React.FC = () => {
  const navigate = useNavigate();
  const { items, getTotalPrice, clearCart } = useCart();
  const { user, addresses, addAddress } = useAuth();
  const { createOrder } = useOrder();
  const [isProcessing, setIsProcessing] = useState(false);
  const [isCheckingOut, setIsCheckingOut] = useState(false); // Prevent redirect during checkout
  const [selectedAddressId, setSelectedAddressId] = useState<string | null>(
    addresses?.find(a => a.isDefault)?.id || null
  );
  const [showAddressDialog, setShowAddressDialog] = useState(false);

  // Promo / voucher
  const [promoCode, setPromoCode] = useState<string>('');
  const [appliedPromo, setAppliedPromo] = useState<Promotion | null>(null);
  const [promoError, setPromoError] = useState<string>('');
  const [discountAmount, setDiscountAmount] = useState<number>(0);
  const [availablePromotions, setAvailablePromotions] = useState<any[]>([]);

  const [newAddressForm, setNewAddressForm] = useState<NewAddressFormData>({
    name: '',
    phone: '',
    address: '',
    ward: '',
    district: '',
    city: '',
    isDefault: false,
  });

  const [newAddressErrors, setNewAddressErrors] = useState<Partial<Record<keyof NewAddressFormData, string>>>({});

  const [formData, setFormData] = useState<CheckoutFormData>({
    customerName: user?.fullName || '',
    customerPhone: user?.phoneNumber || '',
    shippingAddress: '',
    city: '',
    district: '',
    ward: '',
    note: '',
    paymentMethod: 'COD',
  });

  // Redirect if cart is empty (but not during checkout process)
  React.useEffect(() => {
    if (items.length === 0 && !isCheckingOut) {
      toast.error('Giỏ hàng trống!');
      navigate('/');
    }
  }, [items.length, isCheckingOut, navigate]);

  // Redirect if not logged in
  React.useEffect(() => {
    if (!user) {
      toast.error('Vui lòng đăng nhập để tiếp tục!');
      navigate('/login');
    }
  }, [user, navigate]);

  const handleNewAddressChange = (field: keyof NewAddressFormData, value: string | boolean) => {
    setNewAddressForm(prev => ({ ...prev, [field]: value }));
    // Clear error when user starts typing
    if (newAddressErrors[field]) {
      setNewAddressErrors(prev => ({ ...prev, [field]: undefined }));
    }
  };

  const validateNewAddress = (): boolean => {
    const errors: Partial<Record<keyof NewAddressFormData, string>> = {};

    if (!newAddressForm.name.trim()) {
      errors.name = 'Vui lòng nhập họ tên';
    }

    if (!newAddressForm.phone.trim()) {
      errors.phone = 'Vui lòng nhập số điện thoại';
    } else if (!/^[0-9]{10,11}$/.test(newAddressForm.phone.replace(/\s/g, ''))) {
      errors.phone = 'Số điện thoại không hợp lệ';
    }

    if (!newAddressForm.address.trim()) {
      errors.address = 'Vui lòng nhập địa chỉ';
    }

    if (!newAddressForm.district.trim()) {
      errors.district = 'Vui lòng chọn quận/huyện';
    }

    if (!newAddressForm.city.trim()) {
      errors.city = 'Vui lòng chọn tỉnh/thành phố';
    }

    setNewAddressErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const handleAddNewAddress = async () => {
    if (!validateNewAddress()) {
      toast.error('Vui lòng điền đầy đủ thông tin!');
      return;
    }

    // Map old interface to new interface
    const added = await addAddress({
      recipientName: newAddressForm.name,
      recipientPhone: newAddressForm.phone,
      address: newAddressForm.address,
      district: newAddressForm.district,
      city: newAddressForm.city,
      isDefault: newAddressForm.isDefault,
    });

    if (added) {
      let newAddressId: string | null = null;
      if (typeof added === 'object' && added !== null && 'id' in (added as any)) {
        newAddressId = String((added as any).id);
      } else if (addresses && addresses.length > 0) {
        newAddressId = addresses.find(a => a.isDefault)?.id ?? addresses[addresses.length - 1].id ?? null;
      }

      if (newAddressId) {
        setSelectedAddressId(newAddressId);
      }

      toast.success('Đã thêm địa chỉ mới!');
      setShowAddressDialog(false);

      // Reset form
      setNewAddressForm({
        name: '',
        phone: '',
        address: '',
        ward: '',
        district: '',
        city: '',
        isDefault: false,
      });
    } else {
      toast.error('Không thể thêm địa chỉ!');
    }
  };

  const handlePlaceOrder = async () => {
    const selectedAddress = addresses.find(a => a.id === selectedAddressId);

    if (!selectedAddress) {
      toast.error('Vui lòng chọn địa chỉ giao hàng!');
      return;
    }

    setIsProcessing(true);
    setIsCheckingOut(true); 

    try {
      const orderDetails = items.map(item => ({
        bookId: Number((item.book as any).bookId),
        quantity: item.quantity,
      }));

      const paymentMethodMap: Record<PaymentMethod, 'COD'  | 'CreditCard'> = {
        'COD': 'COD',
        'CreditCard': 'CreditCard',
      };

      const requestPayload = {
        addressId: Number(selectedAddress.id),
        paymentMethod: paymentMethodMap[formData.paymentMethod],
        promoCode: appliedPromo?.code ?? (appliedPromo as any)?.promotionCode ?? undefined,
        promoId: appliedPromo ? Number((appliedPromo as any).promotionId ?? appliedPromo.id) : undefined,
        note: formData.note || undefined,
        orderDetails: orderDetails,
      };
      

      const response = await OrderService.createOrder(requestPayload);

      const order = response?.result;
      if (!order) {
        toast.error('Không thể tạo đơn hàng. Vui lòng thử lại!');
        setIsCheckingOut(false);
        return;
      }

      const orderId = String(order.id);
      toast.success('Đặt hàng thành công!');

      const needsOnlinePayment = formData.paymentMethod === 'CreditCard';

      if (needsOnlinePayment) {
        // Backend returns PayOS URL for QR code payment
        const paymentUrl = (order as any).paymentUrl;
        
        if (paymentUrl) {
          // Don't clear cart yet - wait for payment success
          // Cart will be cleared in PaymentReturnPage after successful payment
          toast.info('Đang chuyển đến trang thanh toán...');
          setTimeout(() => {
            window.location.href = paymentUrl;
          }, 500);
        } else {
          // Fallback if no paymentUrl provided
          toast.error('Không thể tạo link thanh toán. Vui lòng thử lại!');
          setIsCheckingOut(false);
        }
      } else {
        // COD - clear cart and redirect to account page
        await clearCart();
        setTimeout(() => {
          navigate('/account');
        }, 1000);
      }
    } catch (error) {
      console.error('Checkout error:', error);
      toast.error('Đã có lỗi xảy ra. Vui lòng thử lại!');
      setIsCheckingOut(false);
    } finally {
      setIsProcessing(false);
    }
  };

  // Load available promotions (public user endpoint)
  React.useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const res: any = await apiRequest.get(API_ENDPOINTS.USER.PROMOTIONS);
        const data = res?.result ?? res ?? [];
        const list = Array.isArray(data) ? data : (data.promotions ?? []);
        if (mounted) setAvailablePromotions(list);
      } catch (err) {
        // silent - promotions are optional
        console.error('Failed to load promotions', err);
      }
    })();
    return () => { mounted = false; };
  }, []);

  const applyPromo = () => {
    setPromoError('');
    const code = (promoCode || '').trim().toUpperCase();
    if (!code) {
      setPromoError('Vui lòng nhập mã giảm giá');
      return;
    }

    const findPromo = (p: any) => {
      const cands = [p.code, p.promotionCode, p.promotion_code, p.name, p.promotion_code, p.promotionCode];
      const v = cands.find(x => x !== undefined && x !== null);
      return String(v || '').toUpperCase() === code;
    };

    const promo = (availablePromotions || []).find(findPromo);
    if (!promo) {
      setPromoError('Mã không hợp lệ');
      return;
    }

    // Normalize fields
    const isActive = promo.isActive !== undefined ? Boolean(promo.isActive) : (promo.status ? String(promo.status) === 'active' : true);
    if (!isActive) {
      setPromoError('Mã này không hoạt động');
      return;
    }

    if (promo.startDate || promo.start_date) {
      const s = new Date(String(promo.startDate ?? promo.start_date));
      if (!isNaN(s.getTime()) && s.getTime() > Date.now()) {
        setPromoError('Mã chưa có hiệu lực');
        return;
      }
    }

    if (promo.endDate || promo.end_date) {
      const e = new Date(String(promo.endDate ?? promo.end_date));
      if (!isNaN(e.getTime()) && e.getTime() < Date.now()) {
        setPromoError('Mã đã hết hạn');
        return;
      }
    }

    const pct = Number(promo.discountPercent ?? promo.discount_percent ?? 0);
    if (!pct || pct <= 0) {
      setPromoError('Mã không có giá trị giảm');
      return;
    }

    const discount = Math.round(totalAmount * pct / 100);
    setDiscountAmount(discount);
    // Map to UI Promotion type if possible
    const mapped: Promotion = {
      id: String(promo.id ?? promo.promotionId ?? promo.promo_id ?? promo.code ?? Date.now()),
      code: String(promo.code ?? promo.promotionCode ?? promo.promotion_code ?? promo.name ?? ''),
      discountPercent: pct,
      startDate: promo.startDate ?? promo.start_date,
      endDate: promo.endDate ?? promo.end_date,
      isActive: isActive,
    } as Promotion;
    setAppliedPromo(mapped);
    toast.success(`Áp dụng mã ${mapped.code}: -${pct}%`);
  };

  const removePromo = () => {
    setAppliedPromo(null);
    setPromoCode('');
    setPromoError('');
    setDiscountAmount(0);
  };

  const totalAmount = getTotalPrice();
  const discountedSubtotal = Math.max(0, totalAmount - discountAmount);
  const finalTotal = discountedSubtotal ;

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-black py-8">
      <div className="container mx-auto px-4 max-w-6xl">
        {/* Header */}
        <div className="mb-6">
          <Button
            variant="ghost"
            onClick={() => navigate('/')}
            className="mb-4"
          >
            <ArrowLeft className="mr-2 h-4 w-4" />
            Quay lại giỏ hàng
          </Button>
          <h1 className="text-black dark:text-white">Thanh toán</h1>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Left Column - Forms */}
          <div className="lg:col-span-2 space-y-6">
            {/* Shipping Address */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <MapPin className="h-5 w-5" />
                  Địa chỉ giao hàng
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                {/* Address List */}
                {addresses && addresses.length > 0 ? (
                  <div className="space-y-3">
                    {addresses.map((address) => (
                      <div
                        key={address.id}
                        className={`p-4 border rounded-lg cursor-pointer transition-colors ${selectedAddressId === address.id
                            ? 'border-black dark:border-white bg-gray-50 dark:bg-white/5'
                            : 'border-gray-200 dark:border-slate-700 hover:border-gray-300 dark:hover:border-slate-600'
                          }`}
                        onClick={() => setSelectedAddressId(address.id)}
                      >
                        <div className="flex items-start gap-3">
                          <div className="pt-1">
                            <RadioGroup value={selectedAddressId || ''}>
                              <RadioGroupItem value={address.id} id={address.id} />
                            </RadioGroup>
                          </div>
                          <div className="flex-1">
                            <div className="flex items-center gap-2">
                              <Home className="h-4 w-4 text-gray-500 dark:text-gray-400" />
                              <span className="font-medium">{address.recipientName}</span>
                              {address.isDefault && (
                                <span className="text-xs bg-black dark:bg-white text-white dark:text-black px-2 py-0.5 rounded">
                                  Mặc định
                                </span>
                              )}
                            </div>
                            <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">{address.recipientPhone}</p>
                            <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                              {address.address}
                              {address.district && `, ${address.district}`}
                              {address.city && `, ${address.city}`}
                            </p>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <Alert>
                    <AlertCircle className="h-4 w-4" />
                    <AlertDescription>
                      Bạn chưa có địa chỉ nào. Vui lòng thêm địa chỉ giao hàng.
                    </AlertDescription>
                  </Alert>
                )}

                {/* Add New Address Button */}
                <Dialog open={showAddressDialog} onOpenChange={setShowAddressDialog}>
                  <DialogTrigger asChild>
                    <Button variant="outline" className="w-full">
                      <Plus className="mr-2 h-4 w-4" />
                      Thêm địa chỉ mới
                    </Button>
                  </DialogTrigger>
                  <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
                    <DialogHeader>
                      <DialogTitle>Thêm địa chỉ mới</DialogTitle>
                      <DialogDescription>
                        Nhập thông tin địa chỉ giao hàng của bạn
                      </DialogDescription>
                    </DialogHeader>
                    <div className="space-y-4 mt-4">
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div className="space-y-2">
                          <Label htmlFor="new-name">
                            Họ và tên <span className="text-red-500">*</span>
                          </Label>
                          <Input
                            id="new-name"
                            placeholder="Nguyễn Văn A"
                            value={newAddressForm.name}
                            onChange={(e) => handleNewAddressChange('name', e.target.value)}
                            className={newAddressErrors.name ? 'border-red-500' : ''}
                          />
                          {newAddressErrors.name && (
                            <p className="text-sm text-red-500">{newAddressErrors.name}</p>
                          )}
                        </div>

                        <div className="space-y-2">
                          <Label htmlFor="new-phone">
                            Số điện thoại <span className="text-red-500">*</span>
                          </Label>
                          <Input
                            id="new-phone"
                            placeholder="0123456789"
                            value={newAddressForm.phone}
                            onChange={(e) => handleNewAddressChange('phone', e.target.value)}
                            className={newAddressErrors.phone ? 'border-red-500' : ''}
                          />
                          {newAddressErrors.phone && (
                            <p className="text-sm text-red-500">{newAddressErrors.phone}</p>
                          )}
                        </div>
                      </div>

                      <div className="space-y-2">
                        <Label htmlFor="new-address">
                          Địa chỉ <span className="text-red-500">*</span>
                        </Label>
                        <Input
                          id="new-address"
                          placeholder="Số nhà, tên đường"
                          value={newAddressForm.address}
                          onChange={(e) => handleNewAddressChange('address', e.target.value)}
                          className={newAddressErrors.address ? 'border-red-500' : ''}
                        />
                        {newAddressErrors.address && (
                          <p className="text-sm text-red-500">{newAddressErrors.address}</p>
                        )}
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div className="space-y-2">
                          <Label htmlFor="new-district">
                            Quận/Huyện <span className="text-red-500">*</span>
                          </Label>
                          <Input
                            id="new-district"
                            placeholder="Quận 1"
                            value={newAddressForm.district}
                            onChange={(e) => handleNewAddressChange('district', e.target.value)}
                            className={newAddressErrors.district ? 'border-red-500' : ''}
                          />
                          {newAddressErrors.district && (
                            <p className="text-sm text-red-500">{newAddressErrors.district}</p>
                          )}
                        </div>

                        <div className="space-y-2">
                          <Label htmlFor="new-city">
                            Tỉnh/Thành phố <span className="text-red-500">*</span>
                          </Label>
                          <Input
                            id="new-city"
                            placeholder="Hà Nội"
                            value={newAddressForm.city}
                            onChange={(e) => handleNewAddressChange('city', e.target.value)}
                            className={newAddressErrors.city ? 'border-red-500' : ''}
                          />
                          {newAddressErrors.city && (
                            <p className="text-sm text-red-500">{newAddressErrors.city}</p>
                          )}
                        </div>
                      </div>

                      <div className="flex items-center gap-2">
                        <input
                          type="checkbox"
                          id="new-default"
                          checked={newAddressForm.isDefault}
                          onChange={(e) => handleNewAddressChange('isDefault', e.target.checked)}
                          className="rounded border-gray-300"
                        />
                        <Label htmlFor="new-default" className="cursor-pointer">
                          Đặt làm địa chỉ mặc định
                        </Label>
                      </div>

                      <div className="flex gap-3 pt-4">
                        <Button
                          variant="outline"
                          onClick={() => setShowAddressDialog(false)}
                          className="flex-1"
                        >
                          Hủy
                        </Button>
                        <Button
                          onClick={handleAddNewAddress}
                          className="flex-1 bg-black dark:bg-white hover:bg-gray-800 dark:hover:bg-gray-200 text-white dark:text-black"
                        >
                          Thêm địa chỉ
                        </Button>
                      </div>
                    </div>
                  </DialogContent>
                </Dialog>

                {/* Note */}
                <div className="space-y-2 pt-4">
                  <Label htmlFor="note">Ghi chú (không bắt buộc)</Label>
                  <Textarea
                    id="note"
                    placeholder="Ghi chú cho người bán..."
                    value={formData.note}
                    onChange={(e) => setFormData(prev => ({ ...prev, note: e.target.value }))}
                    rows={3}
                  />
                </div>
              </CardContent>
            </Card>

            {/* Payment Method */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <CreditCard className="h-5 w-5" />
                  Phương thức thanh toán
                </CardTitle>
              </CardHeader>
              <CardContent>
                <RadioGroup
                  value={formData.paymentMethod}
                  onValueChange={(value) => setFormData(prev => ({ ...prev, paymentMethod: value as PaymentMethod }))}
                >
                  <div className="space-y-3">
                    {paymentMethods.map((method) => {
                      const Icon = method.icon;
                      return (
                        <div
                          key={method.id}
                          className={`flex items-start space-x-3 p-4 border rounded-lg cursor-pointer transition-colors ${formData.paymentMethod === method.id
                              ? 'border-black dark:border-white bg-gray-50 dark:bg-white/5'
                              : 'border-gray-200 dark:border-slate-700 hover:border-gray-300 dark:hover:border-slate-600'
                            }`}
                          onClick={() => setFormData(prev => ({ ...prev, paymentMethod: method.id }))}
                        >
                          <RadioGroupItem value={method.id} id={method.id} />
                          <div className="flex-1">
                            <div className="flex items-center gap-2">
                              <Icon className="h-5 w-5" />
                              <Label htmlFor={method.id} className="cursor-pointer">
                                {method.name}
                              </Label>
                            </div>
                            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">{method.description}</p>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </RadioGroup>
              </CardContent>
            </Card>
          </div>

          {/* Right Column - Order Summary */}
          <div className="lg:col-span-1">
            <Card className="sticky top-4">
              <CardHeader>
                <CardTitle>Đơn hàng của bạn</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                {/* Order Items */}
                <div className="space-y-3 max-h-64 overflow-y-auto">
                  {items.map((item) => {
                    const b: any = item.book;
                    const bookId = String(b.bookId ?? b.id ?? '');
                    const title = b.title ?? '';
                    const author = b.authorName ?? b.author ?? '';
                    const imageUrl = b.imageUrl ?? b.image ?? '';
                    const lineTotal = (b.price ?? 0) * item.quantity;

                    return (
                      <div key={bookId} className="flex gap-3">
                        <img
                          src={imageUrl}
                          alt={title}
                          className="w-16 h-20 object-cover rounded"
                        />
                        <div className="flex-1 min-w-0">
                          <p className="text-sm truncate">{title}</p>
                          <p className="text-sm text-gray-500 dark:text-gray-400">{author}</p>
                          <div className="flex items-center justify-between mt-1">
                            <span className="text-sm text-gray-500 dark:text-gray-400">x{item.quantity}</span>
                            <span className="text-sm">{formatVND(lineTotal)}</span>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>

                <Separator />

                {/* Voucher / Promo */}
                <div className="space-y-2">
                  <Label htmlFor="promo">Mã giảm giá</Label>
                  <div className="flex gap-2">
                    <Input
                      id="promo"
                      placeholder="Nhập mã khuyến mãi"
                      value={promoCode}
                      onChange={(e) => { setPromoCode(e.target.value); setPromoError(''); }}
                    />
                    {!appliedPromo ? (
                      <Button onClick={applyPromo} className="whitespace-nowrap">Áp dụng</Button>
                    ) : (
                      <Button variant="outline" onClick={removePromo} className="whitespace-nowrap">Xóa mã</Button>
                    )}
                  </div>
                  {promoError && <div className="text-sm text-red-600">{promoError}</div>}
                </div>
                  {/* Price Summary */}
                <div className="space-y-2">
                  <div className="flex justify-between text-sm">
                    <span className="text-gray-600 dark:text-gray-400">Tạm tính:</span>
                    <span>{formatVND(totalAmount)}</span>
                  </div>
                  {appliedPromo && (
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-600 dark:text-gray-400">Giảm ({appliedPromo.code}):</span>
                      <span>-{formatVND(discountAmount)}</span>
                    </div>
                  )}
                  <Separator />
                  <div className="flex justify-between">
                    <span>Tổng cộng:</span>
                    <span className="text-black dark:text-white">{formatVND(finalTotal)}</span>
                  </div>
                </div>

                <Button
                  onClick={handlePlaceOrder}
                  disabled={isProcessing || !selectedAddressId}
                  className="w-full bg-black dark:bg-white hover:bg-gray-800 dark:hover:bg-gray-200 text-white dark:text-black"
                >
                  {isProcessing ? 'Đang xử lý...' : 'Đặt hàng'}
                </Button>

                <p className="text-xs text-gray-500 dark:text-gray-400 text-center">
                  Bằng cách đặt hàng, bạn đồng ý với Điều khoản sử dụng của chúng tôi
                </p>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </div>
  );
};
