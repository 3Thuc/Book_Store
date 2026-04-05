import type { ReactNode } from 'react';

export interface PasswordStrength {
  label: ReactNode;
  score: number;
  level: 'weak' | 'fair' | 'good' | 'strong';
  text: string;
  color: string;
  bgColor: string;
}

export const calculatePasswordStrength = (password: string): PasswordStrength => {
  if (!password) {
    return {
      label: '',
      score: 0,
      level: 'weak',
      text: '',
      color: '#94A3B8',
      bgColor: '#F1F5F9'
    };
  }

  const length = password.length;
  const hasLower = /[a-z]/.test(password);
  const hasUpper = /[A-Z]/.test(password);
  const hasNumber = /[0-9]/.test(password);
  const hasSymbol = /[^A-Za-z0-9]/.test(password);

  let score = 0;

  if (length >= 8) score += 25;
  if (hasLower) score += 15;
  if (hasUpper) score += 15;
  if (hasNumber) score += 20;
  if (hasSymbol) score += 25;

  const types = [hasLower, hasUpper, hasNumber, hasSymbol].filter(Boolean).length;
  if (types === 4 && length >= 12) score += 10;

  if (score > 100) score = 100;

  if (score < 40) {
    return {
      label: '',
      score,
      level: 'weak',
      text: 'Yếu',
      color: '#EF4444',
      bgColor: '#FEE2E2'
    };
  } else if (score < 60) {
    return {
      label: '',
      score,
      level: 'fair',
      text: 'Trung bình',
      color: '#F97316',
      bgColor: '#FFEDD5'
    };
  } else if (score <= 90) {
    return {
      label: '',
      score,
      level: 'good',
      text: 'Tốt',
      color: '#3B82F6',
      bgColor: '#DBEAFE'
    };
  } else {
    return {
      label: '',
      score,
      level: 'strong',
      text: 'Mạnh',
      color: '#10B981',
      bgColor: '#D1FAE5'
    };
  }
};
