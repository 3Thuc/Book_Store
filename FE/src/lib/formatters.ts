export function formatVND(value: number | string | undefined | null): string {
  const num = Number(value ?? 0) || 0;
  // Round to nearest integer VND (DB stores amounts as whole VND, e.g., 1999000)
  const rounded = Math.round(num);
  return new Intl.NumberFormat('vi-VN', {
    style: 'currency',
    currency: 'VND',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(rounded);
}

export default formatVND;
