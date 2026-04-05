import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { ArrowLeft, CheckCircle, XCircle, Copy, Clock, QrCode } from 'lucide-react';
import { Button } from '../components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Alert, AlertDescription } from '../components/ui/alert';
import { Progress } from '../components/ui/progress';
import { useOrder } from '../context/OrderContext';
import { useAuth } from '../context/AuthContext';
import { toast } from 'sonner';

interface PaymentPageProps {
  orderId: string;
}

export const PaymentPage: React.FC<PaymentPageProps> = ({ orderId }) => {
  const navigate = useNavigate();
  const { orders, updateOrderPaymentStatus } = useOrder();
  const { user } = useAuth();
  const [timeLeft, setTimeLeft] = useState(600); // 10 minutes in seconds
  const [paymentStatus, setPaymentStatus] = useState<'pending' | 'success' | 'failed'>('pending');
  const [isProcessing, setIsProcessing] = useState(false);

  const order = orders.find(o => o.id === orderId);

  useEffect(() => {
    if (!order || !user) {
      navigate('/account');
      return;
    }

    // Check if already paid
    if (order.isPaid) {
      setPaymentStatus('success');
      return;
    }

    // Countdown timer
    const timer = setInterval(() => {
      setTimeLeft(prev => {
        if (prev <= 1) {
          clearInterval(timer);
          if (paymentStatus === 'pending') {
            setPaymentStatus('failed');
            toast.error('Hết thời gian thanh toán!');
          }
          return 0;
        }
        return prev - 1;
      });
    }, 1000);

    return () => clearInterval(timer);
  }, [order, user, paymentStatus]);

  // Auto redirect to home page after successful payment
  useEffect(() => {
    if (paymentStatus === 'success') {
      const redirectTimer = setTimeout(() => {
        toast.success('Cảm ơn bạn đã mua hàng!');
        navigate('/');
      }, 3000);

      return () => clearTimeout(redirectTimer);
    }
  }, [paymentStatus, navigate]);

  if (!order || !user) {
    return null;
  }

  const formatTime = (seconds: number) => {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
  };

  const handleSimulatePayment = async () => {
    setIsProcessing(true);
    
    // Simulate payment processing
    setTimeout(() => {
      const success = Math.random() > 0.2; // 80% success rate for demo
      
      if (success) {
        updateOrderPaymentStatus(orderId, true);
        setPaymentStatus('success');
        toast.success('Thanh toán thành công!');
      } else {
        setPaymentStatus('failed');
        toast.error('Thanh toán thất bại! Vui lòng thử lại.');
      }
      
      setIsProcessing(false);
    }, 2000);
  };

  const copyToClipboard = (text: string, label: string) => {
    navigator.clipboard.writeText(text);
    toast.success(`Đã sao chép ${label}!`);
  };

  const paymentMethodNames = {
    'BANKING': 'Chuyển khoản ngân hàng',
    'MOMO': 'Ví MoMo',
    'VNPAY': 'VNPAY',
    'COD': 'Thanh toán khi nhận hàng'
  };

  const progress = ((600 - timeLeft) / 600) * 100;

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-black py-8">
      <div className="container mx-auto px-4 max-w-4xl">
        {/* Header */}
        <div className="mb-6">
          <Button
            variant="ghost"
            onClick={() => navigate('/account')}
            className="mb-4"
          >
            <ArrowLeft className="mr-2 h-4 w-4" />
            Quay lại đơn hàng
          </Button>
          <h1 className="text-black dark:text-white">Thanh toán đơn hàng</h1>
        </div>

        {paymentStatus === 'success' ? (
          // Success State
          <Card className="border-green-200 dark:border-green-800 bg-green-50 dark:bg-green-950/30">
            <CardContent className="pt-6">
              <div className="text-center space-y-4">
                <div className="flex justify-center">
                  <CheckCircle className="h-20 w-20 text-green-600 dark:text-green-400" />
                </div>
                <div>
                  <h2 className="text-green-900 dark:text-green-300 mb-2">Thanh toán thành công!</h2>
                  <p className="text-green-700 dark:text-green-400">
                    Đơn hàng <span className="font-medium">{order.id}</span> đã được thanh toán thành công.
                  </p>
                  <p className="text-green-700 dark:text-green-400 mt-2">
                    Chúng tôi đang xử lý đơn hàng của bạn.
                  </p>
                  <p className="text-green-600 dark:text-green-500 mt-4 text-sm">
                    Đang chuyển về trang chủ trong 3 giây...
                  </p>
                </div>
                <div className="flex gap-3 justify-center pt-4">
                  <Button onClick={() => navigate('/')}>
                    Về trang chủ ngay
                  </Button>
                  <Button variant="outline" onClick={() => navigate('/account')}>
                    Xem đơn hàng
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        ) : paymentStatus === 'failed' ? (
          // Failed State
          <Card className="border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-950/30">
            <CardContent className="pt-6">
              <div className="text-center space-y-4">
                <div className="flex justify-center">
                  <XCircle className="h-20 w-20 text-red-600 dark:text-red-400" />
                </div>
                <div>
                  <h2 className="text-red-900 dark:text-red-300 mb-2">Thanh toán không thành công</h2>
                  <p className="text-red-700 dark:text-red-400">
                    {timeLeft === 0 
                      ? 'Đã hết thời gian thanh toán. Vui lòng thử lại.'
                      : 'Giao dịch không thành công. Vui lòng thử lại hoặc chọn phương thức thanh toán khác.'
                    }
                  </p>
                </div>
                <div className="flex gap-3 justify-center pt-4">
                  <Button 
                    onClick={() => {
                      setPaymentStatus('pending');
                      setTimeLeft(600);
                    }}
                    className="bg-red-600 hover:bg-red-700"
                  >
                    Thử lại
                  </Button>
                  <Button variant="outline" onClick={() => navigate('/')}>
                    Về trang chủ
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        ) : (
          // Pending State - Show QR Code
          <div className="space-y-6">
            {/* Timer Alert */}
            <Alert>
              <Clock className="h-4 w-4" />
              <AlertDescription>
                <div className="flex items-center justify-between">
                  <span>Thời gian còn lại để thanh toán:</span>
                  <span className="font-medium text-lg">{formatTime(timeLeft)}</span>
                </div>
                <Progress value={progress} className="mt-2" />
              </AlertDescription>
            </Alert>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* QR Code Section */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <QrCode className="h-5 w-5" />
                    Quét mã QR để thanh toán
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  {/* Mock QR Code */}
                  <div className="bg-white dark:bg-black p-6 rounded-lg border-2 border-gray-200 dark:border-slate-700 flex items-center justify-center">
                    <div className="text-center space-y-4">
                      {/* Simple QR code placeholder */}
                      <div className="bg-black dark:bg-white p-4 inline-block">
                        <div className="grid grid-cols-8 gap-1">
                          {Array.from({ length: 64 }).map((_, i) => (
                            <div
                              key={i}
                              className={`w-3 h-3 ${
                                Math.random() > 0.5 ? 'bg-white dark:bg-black' : 'bg-black dark:bg-white'
                              }`}
                            />
                          ))}
                        </div>
                      </div>
                      <p className="text-sm text-gray-600 dark:text-gray-400">
                        Sử dụng app {paymentMethodNames[order.paymentMethod || 'BANKING']}
                        <br />
                        để quét mã QR
                      </p>
                    </div>
                  </div>

                  {/* Demo Button - In production this would be automatic */}
                  <Alert className="bg-blue-50 dark:bg-blue-950/30 border-blue-200 dark:border-blue-800">
                    <AlertDescription className="text-sm text-blue-800 dark:text-blue-300">
                      <p className="font-medium mb-2">Chế độ Demo:</p>
                      <p>Nhấn nút bên dưới để mô phỏng thanh toán qua QR code</p>
                    </AlertDescription>
                  </Alert>

                  <Button
                    onClick={handleSimulatePayment}
                    disabled={isProcessing}
                    className="w-full bg-black dark:bg-white hover:bg-gray-800 dark:hover:bg-gray-200 text-white dark:text-black"
                  >
                    {isProcessing ? 'Đang xử lý...' : 'Mô phỏng thanh toán (Demo)'}
                  </Button>
                </CardContent>
              </Card>

              {/* Payment Details */}
              <div className="space-y-6">
                {/* Order Info */}
                <Card>
                  <CardHeader>
                    <CardTitle>Thông tin đơn hàng</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-600 dark:text-gray-400">Mã đơn hàng:</span>
                      <div className="flex items-center gap-2">
                        <span className="font-medium">{order.id}</span>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-6 w-6 p-0"
                          onClick={() => copyToClipboard(order.id, 'mã đơn hàng')}
                        >
                          <Copy className="h-3 w-3" />
                        </Button>
                      </div>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-600 dark:text-gray-400">Phương thức:</span>
                      <span className="font-medium">
                        {paymentMethodNames[order.paymentMethod || 'BANKING']}
                      </span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-600 dark:text-gray-400">Số tiền:</span>
                      <span className="font-medium text-lg">
                        {order.totalAmount.toLocaleString('vi-VN')}đ
                      </span>
                    </div>
                  </CardContent>
                </Card>

                {/* Bank Transfer Info */}
                {order.paymentMethod === 'BANKING' && (
                  <Card>
                    <CardHeader>
                      <CardTitle>Thông tin chuyển khoản</CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-3">
                      <div className="space-y-2">
                        <div className="flex justify-between items-center">
                          <span className="text-sm text-gray-600 dark:text-gray-400">Ngân hàng:</span>
                          <div className="flex items-center gap-2">
                            <span className="font-medium">Vietcombank</span>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-6 w-6 p-0"
                              onClick={() => copyToClipboard('Vietcombank', 'tên ngân hàng')}
                            >
                              <Copy className="h-3 w-3" />
                            </Button>
                          </div>
                        </div>
                        <div className="flex justify-between items-center">
                          <span className="text-sm text-gray-600 dark:text-gray-400">Số tài khoản:</span>
                          <div className="flex items-center gap-2">
                            <span className="font-medium">1234567890</span>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-6 w-6 p-0"
                              onClick={() => copyToClipboard('1234567890', 'số tài khoản')}
                            >
                              <Copy className="h-3 w-3" />
                            </Button>
                          </div>
                        </div>
                        <div className="flex justify-between items-center">
                          <span className="text-sm text-gray-600 dark:text-gray-400">Chủ tài khoản:</span>
                          <div className="flex items-center gap-2">
                            <span className="font-medium">BOOKSTORE JSC</span>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-6 w-6 p-0"
                              onClick={() => copyToClipboard('BOOKSTORE JSC', 'tên chủ tài khoản')}
                            >
                              <Copy className="h-3 w-3" />
                            </Button>
                          </div>
                        </div>
                        <div className="flex justify-between items-center">
                          <span className="text-sm text-gray-600 dark:text-gray-400">Nội dung:</span>
                          <div className="flex items-center gap-2">
                            <span className="font-medium">{order.id}</span>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-6 w-6 p-0"
                              onClick={() => copyToClipboard(order.id, 'nội dung chuyển khoản')}
                            >
                              <Copy className="h-3 w-3" />
                            </Button>
                          </div>
                        </div>
                      </div>
                      <Alert className="mt-4">
                        <AlertDescription className="text-sm">
                          Vui lòng nhập đúng nội dung chuyển khoản để đơn hàng được xử lý nhanh chóng.
                        </AlertDescription>
                      </Alert>
                    </CardContent>
                  </Card>
                )}

                {/* Instructions */}
                <Card>
                  <CardHeader>
                    <CardTitle>Hướng dẫn thanh toán</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <ol className="list-decimal list-inside space-y-2 text-sm text-gray-700 dark:text-gray-300">
                      <li>Mở ứng dụng {paymentMethodNames[order.paymentMethod || 'BANKING']} trên điện thoại</li>
                      <li>Chọn chức năng quét mã QR</li>
                      <li>Quét mã QR bên trái màn hình</li>
                      <li>Xác nhận thông tin và hoàn tất thanh toán</li>
                      <li>Hệ thống sẽ tự động cập nhật trạng thái đơn hàng</li>
                    </ol>
                  </CardContent>
                </Card>
              </div>
            </div>

            {/* Cancel Option */}
            <div className="text-center">
              <Button
                variant="outline"
                onClick={() => navigate('/account')}
              >
                Tôi sẽ thanh toán sau
              </Button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};
