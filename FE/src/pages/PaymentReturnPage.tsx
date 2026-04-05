import { useEffect, useState, useRef } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { CheckCircle2, XCircle, Loader2 } from 'lucide-react';
import { Button } from '../components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card';
import { useCart } from '../context/CartContext';

/**
 * Payment Return Page
 * Handles successful payment callback from PayOS
 */
export const PaymentReturnPage = () => {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { clearCart } = useCart();
  const [status, setStatus] = useState<'loading' | 'success' | 'failed'>('loading');
  const hasCheckedRef = useRef(false);

  // Get params from URL
  // Support both PayOS format (orderCode) and backend format (orderId)
  const orderId = searchParams.get('orderId') || searchParams.get('orderCode');
  const paymentStatus = searchParams.get('status');
  const code = searchParams.get('code');

  useEffect(() => {
    // Prevent multiple executions (React strict mode runs useEffect twice)
    if (hasCheckedRef.current) return;
    hasCheckedRef.current = true;

    // Check payment status and clear cart if successful
    const checkPayment = async () => {
      console.log('Payment params:', { code, paymentStatus, orderId });
      
      // Check if payment is successful
      // - code === '00' means success from PayOS
      // - paymentStatus === 'PAID' means paid status from PayOS
      // - If orderId exists but no code/status, assume backend direct success redirect
      if (code === '00' || paymentStatus === 'paid' || (orderId && !code && !paymentStatus)) {
        // Clear cart after successful payment
        try {
          await clearCart();
        } catch (error) {
          console.error('Failed to clear cart:', error);
          // Still show success even if clear fails (cart might already be empty)
        }
        setStatus('success');
      } else if (code || paymentStatus) {
        // Has payment params but not successful
        setStatus('failed');
      } else {
        // No params at all - treat as failed
        setStatus('failed');
      }
    };
    
    setTimeout(() => {
      checkPayment();
    }, 1500);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  if (status === 'loading') {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <Card className="w-full max-w-md mx-4">
          <CardContent className="pt-6 text-center">
            <Loader2 className="w-16 h-16 mx-auto mb-4 animate-spin text-primary" />
            <h2 className="text-xl font-semibold mb-2">Đang xác nhận thanh toán...</h2>
            <p className="text-gray-600">Vui lòng đợi trong giây lát</p>
          </CardContent>
        </Card>
      </div>
    );
  }

  if (status === 'success') {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <Card className="w-full max-w-md mx-4">
          <CardHeader className="text-center">
            <CheckCircle2 className="w-16 h-16 mx-auto mb-4 text-green-500" />
            <CardTitle className="text-2xl">Thanh toán thành công!</CardTitle>
            <CardDescription>
              Mã đơn hàng: <span className="font-semibold">#{orderId}</span>
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="text-center text-gray-600">
              Đơn hàng của bạn đã được thanh toán thành công. Chúng tôi sẽ xử lý và giao hàng sớm nhất có thể.
            </p>
            <div className="flex gap-3">
              <Button
                variant="outline"
                className="flex-1"
                onClick={() => navigate('/')}
              >
                Về trang chủ
              </Button>
              <Button
                className="flex-1"
                onClick={() => navigate('/account')}
              >
                Xem đơn hàng
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50">
      <Card className="w-full max-w-md mx-4">
        <CardHeader className="text-center">
          <XCircle className="w-16 h-16 mx-auto mb-4 text-red-500" />
          <CardTitle className="text-2xl">Thanh toán thất bại</CardTitle>
          <CardDescription>
            Mã đơn hàng: <span className="font-semibold">#{orderId}</span>
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-center text-gray-600">
            Rất tiếc, thanh toán của bạn không thành công. Vui lòng thử lại hoặc chọn phương thức thanh toán khác.
          </p>
          <div className="flex gap-3">
            <Button
              variant="outline"
              className="flex-1"
              onClick={() => navigate('/')}
            >
              Về trang chủ
            </Button>
            <Button
              className="flex-1"
              onClick={() => navigate('/account')}
            >
              Xem đơn hàng
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};
