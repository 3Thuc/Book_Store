import React from 'react';
import { useNavigate } from 'react-router-dom';
import { X, Plus, Minus, ShoppingBag, Check } from 'lucide-react';
import { Button } from '../../components/ui/button';
import { Separator } from '../../components/ui/separator';
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetDescription } from '../../components/ui/sheet';
import { ImageWithFallback } from '../../components/fallbackimg/ImageWithFallback';
import { useCart } from '../../context/CartContext';
import { useAuth } from '../../context/AuthContext';
import { toast } from 'sonner';
import { formatVND } from '../../lib/formatters';

interface CartProps {
  isOpen: boolean;
  onClose: () => void;
}

export const Cart: React.FC<CartProps> = ({ isOpen, onClose }) => {
  const navigate = useNavigate();
  const { items, updateQuantity, removeFromCart, clearCart, toggleSelectItem, selectAllItems, deselectAllItems, getSelectedTotalPrice, getSelectedTotalItems, getTotalItems } = useCart();
  const { isLoggedIn } = useAuth();

  const isAllSelected = items.length > 0 && items.every(item => item.selected);

  const handleToggleSelectAll = () => {
    if (isAllSelected) {
      deselectAllItems();
    } else {
      selectAllItems();
    }
  };

  React.useEffect(() => {
    if (isOpen) {
      window.dispatchEvent(new CustomEvent('cart-open'));
    }
  }, [isOpen]);

  const handleQuantityChange = async (bookId: string, newQuantity: number) => {
    if (newQuantity < 1) {
      await removeFromCart(bookId);
      return;
    }
    await updateQuantity(bookId, newQuantity);
  };

  const handleRemoveItem = async (bookId: string) => {
    await removeFromCart(bookId);
  };

  const handleClearCart = async () => {
    await clearCart();
  };

  const handleDeselectAll = () => {
    deselectAllItems();
  };

  const handleContinueShopping = () => {
    onClose();
  };

  const handleCheckout = () => {
    if (!isLoggedIn) {
      toast.error('Bạn cần đăng nhập để đặt hàng');
      onClose();
      navigate('/login');
      return;
    }

    const selectedCount = items.filter(item => item.selected).length;
    if (selectedCount === 0) {
      toast.error('Vui lòng chọn ít nhất một sản phẩm để đặt hàng');
      return;
    }

    // Navigate to checkout page
    onClose();
    navigate('/checkout');
  };

  const formatPrice = (price: number) => formatVND(price);

  const totalPrice = getSelectedTotalPrice();
  const totalItems = getTotalItems();
  const selectedCount = items.filter(item => item.selected).length;

  return (
    <Sheet open={isOpen} onOpenChange={onClose}>
      <SheetContent className="w-full sm:max-w-md flex flex-col p-0">
        {/* Header */}
        <SheetHeader className="px-6 py-4 border-b">
          <SheetTitle>
            Giỏ hàng {items.length > 0 && `(${totalItems} sản phẩm)`}
          </SheetTitle>
          <SheetDescription>
            {items.length > 0
              ? 'Xem lại và chỉnh sửa các sản phẩm trong giỏ hàng của bạn'
              : 'Giỏ hàng hiện tại của bạn đang trống'
            }
          </SheetDescription>
        </SheetHeader>

        {/* Content */}
        {items.length === 0 ? (
          // Empty Cart
          <div className="flex-1 flex flex-col items-center justify-center text-center px-6 pb-6">
            <div className="w-24 h-24 rounded-full bg-muted flex items-center justify-center mb-6">
              <ShoppingBag className="h-12 w-12 text-muted-foreground" strokeWidth={1.5} />
            </div>
            <h3 className="mb-2">Giỏ hàng trống</h3>
            <p className="text-muted-foreground mb-6 max-w-xs">
              Hãy thêm một vài cuốn sách yêu thích vào giỏ hàng nhé!
            </p>
            <Button
              onClick={handleContinueShopping}
              className="bg-black hover:bg-black/90 text-white px-8"
            >
              Tiếp tục mua sắm
            </Button>
          </div>
        ) : (
          // Cart with items
          <>
            {/* Cart Items */}
            <div className="flex-1 overflow-y-auto px-6 py-4">
              {/* Chọn tất cả */}
              <div className="flex items-center gap-2 mb-4 pb-2 border-b select-none">
                <button
                  type="button"
                  onClick={handleToggleSelectAll}
                  className="flex-shrink-0 focus:outline-none"
                >
                  {isAllSelected ? (
                    <div className="w-5 h-5 rounded-full bg-black text-white dark:bg-white dark:text-black flex items-center justify-center border border-black dark:border-white">
                      <Check className="h-3.5 w-3.5" strokeWidth={3} />
                    </div>
                  ) : (
                    <div className="w-5 h-5 rounded-full border border-gray-300 hover:border-gray-400 dark:border-gray-600 dark:hover:border-gray-500" />
                  )}
                </button>
                <span
                  className="text-sm font-medium text-foreground cursor-pointer"
                  onClick={handleToggleSelectAll}
                >
                  Chọn tất cả ({items.length} sản phẩm)
                </span>
              </div>

              <div className="space-y-4">
                {items.map((item) => {
                  const b: any = item.book;
                  const bookId = String(b.bookId ?? b.id ?? '');
                  const title = b.title ?? '';
                  const author = b.author ?? b.authorName ?? '';
                  const price = b.price ?? 0;
                  const imageUrl = b.imageUrl ?? b.image ?? '';

                  return (
                    <div key={bookId} className="flex items-center gap-4">
                      {/* Selection Checkbox */}
                      <button
                        type="button"
                        onClick={() => toggleSelectItem(bookId)}
                        className="flex-shrink-0 focus:outline-none"
                      >
                        {item.selected ? (
                          <div className="w-5 h-5 rounded-full bg-black text-white dark:bg-white dark:text-black flex items-center justify-center border border-black dark:border-white">
                            <Check className="h-3.5 w-3.5" strokeWidth={3} />
                          </div>
                        ) : (
                          <div className="w-5 h-5 rounded-full border border-gray-300 hover:border-gray-400 dark:border-gray-600 dark:hover:border-gray-500" />
                        )}
                      </button>

                      {/* Book Image */}
                      <div className="flex-shrink-0">
                        <ImageWithFallback
                          src={imageUrl}
                          alt={title}
                          className="w-16 h-20 object-cover rounded border"
                        />
                      </div>

                      {/* Book Info */}
                      <div className="flex-1 min-w-0">
                        <h4 className="line-clamp-1 mb-0.5">{title}</h4>
                        <p className="text-muted-foreground mb-2">{author}</p>
                        <p className="mb-3">{formatPrice(price)}</p>

                        {/* Quantity Controls */}
                        <div className="flex items-center gap-3">
                          <div className="flex items-center border rounded">
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-8 w-8 p-0 rounded-none hover:bg-muted"
                              onClick={() => handleQuantityChange(bookId, item.quantity - 1)}
                            >
                              <Minus className="h-4 w-4" />
                            </Button>

                            <input
                              type="number"
                              min="1"
                              value={item.quantity}
                              onChange={(e) => {
                                const val = parseInt(e.target.value);
                                if (!isNaN(val) && val > 0) {
                                  handleQuantityChange(bookId, val);
                                }
                              }}
                              className="w-12 h-8 text-center border-0 focus:outline-none focus:ring-0 [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none"
                            />

                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-8 w-8 p-0 rounded-none hover:bg-muted"
                              onClick={() => handleQuantityChange(bookId, item.quantity + 1)}
                            >
                              <Plus className="h-4 w-4" />
                            </Button>
                          </div>

                          {/* Remove Button */}
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-8 w-8 p-0 text-destructive hover:text-destructive hover:bg-destructive/10"
                            onClick={() => handleRemoveItem(bookId)}
                          >
                            <X className="h-4 w-4" />
                          </Button>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Footer */}
            <div className="border-t px-6 py-4 space-y-4">
              {/* Clear All */}
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-4">
                  <button
                    type="button"
                    onClick={handleClearCart}
                    className="text-muted-foreground hover:text-foreground transition-colors text-sm"
                  >
                    Xóa tất cả
                  </button>
                  {selectedCount > 0 && (
                    <button
                      type="button"
                      onClick={handleDeselectAll}
                      className="text-muted-foreground hover:text-foreground transition-colors text-sm font-medium"
                    >
                      Bỏ chọn tất cả
                    </button>
                  )}
                </div>
                <div className="text-right">
                  <p className="text-muted-foreground mb-1 text-sm">Tổng cộng</p>
                  <p className="text-2xl font-semibold">{formatPrice(totalPrice)}</p>
                </div>
              </div>

              {/* Checkout Button */}
              <Button
                onClick={handleCheckout}
                disabled={!isLoggedIn || selectedCount === 0}
                className="w-full h-12 bg-black hover:bg-black/90 text-white"
              >
                Thanh toán {selectedCount > 0 && `(${selectedCount})`}
              </Button>

              {/* Continue Shopping Link */}
              <button
                type="button"
                onClick={handleContinueShopping}
                className="w-full text-center text-muted-foreground hover:text-foreground transition-colors"
              >
                Tiếp tục mua sắm
              </button>

              {!isLoggedIn && (
                <p className="text-sm text-muted-foreground text-center">
                  Bạn cần đăng nhập để đặt hàng
                </p>
              )}
            </div>
          </>
        )}
      </SheetContent>
    </Sheet>
  );
};
