import React from 'react';
import { Moon, Sun } from 'lucide-react';
import { useTheme } from '../context/ThemeContext';

export const ThemeToggle: React.FC = () => {
  const { theme, toggleTheme } = useTheme();
  const isDark = theme === 'dark';

  return (
    <button
      type="button"
      onClick={toggleTheme}
      className="group relative inline-flex items-center gap-2 rounded-full bg-slate-200 dark:bg-slate-700 p-1 transition-colors duration-300 hover:bg-slate-300 dark:hover:bg-slate-600"
      aria-label={isDark ? 'Chuyển sang chế độ sáng' : 'Chuyển sang chế độ tối'}
    >
      {/* Track */}
      <div className="relative flex items-center w-14 h-7 rounded-full">
        {/* Icons Background */}
        <div className="absolute inset-0 flex items-center justify-between px-2">
          {/* Sun Icon - Left */}
          <Sun 
            className={`h-4 w-4 transition-all duration-300 ${
              !isDark 
                ? 'text-amber-500 scale-100 opacity-100' 
                : 'text-slate-400 scale-75 opacity-50'
            }`}
          />
          
          {/* Moon Icon - Right */}
          <Moon 
            className={`h-4 w-4 transition-all duration-300 ${
              isDark 
                ? 'text-blue-400 scale-100 opacity-100' 
                : 'text-slate-400 scale-75 opacity-50'
            }`}
          />
        </div>

        {/* Sliding Circle */}
        <div
          className={`absolute top-0.5 left-0.5 h-6 w-6 rounded-full bg-white dark:bg-slate-900 shadow-lg transition-transform duration-300 ease-out flex items-center justify-center ${
            isDark ? 'translate-x-7' : 'translate-x-0'
          }`}
        >
          {/* Active Icon in Circle */}
          {isDark ? (
            <Moon className="h-3.5 w-3.5 text-blue-500" />
          ) : (
            <Sun className="h-3.5 w-3.5 text-amber-600" />
          )}
        </div>
      </div>

      <span className="sr-only">
        {isDark ? 'Chuyển sang chế độ sáng' : 'Chuyển sang chế độ tối'}
      </span>
    </button>
  );
};
