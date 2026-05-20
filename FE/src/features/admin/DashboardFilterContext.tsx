import React, { createContext, useContext, useState, useCallback, ReactNode } from 'react';
import { CalendarDays, X } from 'lucide-react';

// ─── Types ────────────────────────────────────────────────────────────────────
export interface DashboardFilter {
  year: number;   // 0 = tất cả năm
  month: number;  // 0 = tất cả tháng
  day: number;    // 0 = tất cả ngày
}

interface DashboardFilterCtx {
  filter: DashboardFilter;
  setYear:  (y: number) => void;
  setMonth: (m: number) => void;
  setDay:   (d: number) => void;
  reset:    () => void;
  /** Trả về true nếu orderDate nằm trong khoảng filter */
  inPeriod: (orderDate: string) => boolean;
  /** Label ngắn mô tả filter đang active */
  filterLabel: string;
  /** True nếu đang áp dụng bất kỳ filter nào */
  isFiltered: boolean;
}

// ─── Default ──────────────────────────────────────────────────────────────────
const DEFAULT_FILTER: DashboardFilter = {
  year:  0,  // 0 = tất cả năm (mặc định)
  month: 0,
  day:   0,
};

const DashboardFilterContext = createContext<DashboardFilterCtx | null>(null);

// ─── Provider ─────────────────────────────────────────────────────────────────
export const DashboardFilterProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const [filter, setFilter] = useState<DashboardFilter>(DEFAULT_FILTER);

  const setYear  = useCallback((y: number) => setFilter({ year: y, month: 0, day: 0 }), []);
  const setMonth = useCallback((m: number) => setFilter(f => ({ ...f, month: m, day: 0 })), []);
  const setDay   = useCallback((d: number) => setFilter(f => ({ ...f, day: d })), []);
  const reset    = useCallback(() => setFilter(DEFAULT_FILTER), []);

  const inPeriod = useCallback((orderDate: string): boolean => {
    const d = new Date(orderDate);
    if (filter.year  !== 0 && d.getFullYear()   !== filter.year)           return false;
    if (filter.month !== 0 && (d.getMonth() + 1) !== filter.month)         return false;
    if (filter.day   !== 0 && d.getDate()        !== filter.day)           return false;
    return true;
  }, [filter]);

  const filterLabel = (() => {
    if (filter.year  === 0) return 'Tất cả năm';
    if (filter.month === 0) return `Năm ${filter.year}`;
    if (filter.day   === 0) return `Tháng ${filter.month}/${filter.year}`;
    return `Ngày ${filter.day}/${filter.month}/${filter.year}`;
  })();

  const isFiltered = filter.year !== 0 || filter.month !== 0 || filter.day !== 0;

  return (
    <DashboardFilterContext.Provider
      value={{ filter, setYear, setMonth, setDay, reset, inPeriod, filterLabel, isFiltered }}
    >
      {children}
    </DashboardFilterContext.Provider>
  );
};

// ─── Hook ─────────────────────────────────────────────────────────────────────
export const useDashboardFilter = (): DashboardFilterCtx => {
  const ctx = useContext(DashboardFilterContext);
  if (!ctx) throw new Error('useDashboardFilter must be used inside DashboardFilterProvider');
  return ctx;
};

// ─── Filter Bar Component (dùng chung cho mọi trang) ─────────────────────────
export const DashboardFilterBar: React.FC = () => {
  const { filter, setYear, setMonth, setDay, reset, filterLabel, isFiltered } = useDashboardFilter();

  const currentYear = new Date().getFullYear();
  const years = Array.from({ length: 6 }, (_, i) => currentYear - i);

  // Số ngày trong tháng đang chọn
  const daysInMonth =
    filter.year > 0 && filter.month > 0
      ? new Date(filter.year, filter.month, 0).getDate()
      : 31;

  return (
    <div className="flex flex-wrap items-center gap-2 px-3 py-2.5 rounded-xl border border-border/70 bg-muted/40 shadow-sm">
      {/* Icon + Label */}
      <div className="flex items-center gap-1.5 text-xs font-semibold text-muted-foreground uppercase tracking-wide">
        <CalendarDays className="h-3.5 w-3.5" />
        <span>Bộ lọc</span>
      </div>

      <div className="w-px h-4 bg-border" />

      {/* ── Năm ── */}
      <select
        value={filter.year}
        onChange={e => setYear(Number(e.target.value))}
        className="px-2.5 py-1 text-sm border rounded-lg bg-background text-foreground cursor-pointer hover:border-primary focus:border-primary focus:outline-none transition-colors"
      >
        <option value={0}>Tất cả năm</option>
        {years.map(y => (
          <option key={y} value={y}>Năm {y}</option>
        ))}
      </select>

      {/* ── Tháng — chỉ hiện khi có năm cụ thể ── */}
      {filter.year > 0 && (
        <select
          value={filter.month}
          onChange={e => setMonth(Number(e.target.value))}
          className="px-2.5 py-1 text-sm border rounded-lg bg-background text-foreground cursor-pointer hover:border-primary focus:border-primary focus:outline-none transition-colors"
        >
          <option value={0}>Tất cả tháng</option>
          {['Tháng 1','Tháng 2','Tháng 3','Tháng 4','Tháng 5','Tháng 6',
            'Tháng 7','Tháng 8','Tháng 9','Tháng 10','Tháng 11','Tháng 12'].map((m, i) => (
            <option key={i + 1} value={i + 1}>{m}</option>
          ))}
        </select>
      )}

      {/* ── Ngày — chỉ hiện khi có tháng cụ thể ── */}
      {filter.year > 0 && filter.month > 0 && (
        <select
          value={filter.day}
          onChange={e => setDay(Number(e.target.value))}
          className="px-2.5 py-1 text-sm border rounded-lg bg-background text-foreground cursor-pointer hover:border-primary focus:border-primary focus:outline-none transition-colors"
        >
          <option value={0}>Tất cả ngày</option>
          {Array.from({ length: daysInMonth }, (_, i) => i + 1).map(d => (
            <option key={d} value={d}>Ngày {d}</option>
          ))}
        </select>
      )}

      {/* ── Badge + Reset ── */}
      <div className="flex items-center gap-2 ml-auto">
        <span className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium border transition-colors ${
          isFiltered
            ? 'bg-primary/10 text-primary border-primary/30'
            : 'bg-background text-muted-foreground border-border'
        }`}>
          <CalendarDays className="h-3 w-3" />
          {filterLabel}
        </span>

        {isFiltered && (
          <button
            onClick={reset}
            className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium
                       bg-muted hover:bg-destructive/10 hover:text-destructive
                       text-muted-foreground border border-border transition-colors"
            title="Đặt lại về Tất cả năm"
          >
            <X className="h-3 w-3" />
            Đặt lại
          </button>
        )}
      </div>
    </div>
  );
};
