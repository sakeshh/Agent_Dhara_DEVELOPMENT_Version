'use client';

import { useState } from 'react';
import { motion } from 'framer-motion';
import { FaDatabase, FaChevronDown, FaChevronUp, FaExclamationTriangle, FaCheckCircle } from 'react-icons/fa';

export interface DuckDbDiff {
  skipped?: boolean;
  reason?: string;
  before_dataset?: string;
  table_aliases?: Record<string, string>;
  preview_sql_used?: string;
  diff?: {
    before_rows: number;
    after_rows: number;
    before_cols: string[];
    after_cols: string[];
    row_delta: number;
    added_columns: string[];
    removed_columns: string[];
    dtype_changes: { column: string; before: string; after: string }[];
    key_overlap?: { keys_lost: number; keys_new: number; error?: string };
  };
}

interface DuckDbDiffPanelProps {
  diff: DuckDbDiff | null;
  darkMode?: boolean;
}

export function DuckDbDiffPanel({ diff, darkMode = false }: DuckDbDiffPanelProps) {
  const [showSql, setShowSql] = useState(false);

  if (!diff) {
    return null;
  }

  // Render a friendly status box when skipped rather than hiding completely
  if (diff.skipped) {
    // Keep list of icons we imported: FaInfoCircle is not in there, let's import it or use FaExclamationTriangle
    return (
      <div className={`mt-4 rounded-xl border p-4 text-xs flex items-start gap-2.5 ${
        darkMode
          ? "border-amber-500/25 bg-amber-500/5 text-amber-200/70"
          : "border-amber-500/30 bg-amber-50/50 text-amber-800"
      }`}>
        <FaExclamationTriangle className="mt-0.5 shrink-0 text-amber-500" />
        <div>
          <span className="font-bold">DuckDB Preview:</span> {diff.reason === 'no_preview_datasets' ? 'No preview datasets loaded in session' : diff.reason === 'duckdb_diff_not_enabled' ? 'DuckDB diff not enabled in configuration' : diff.reason || 'No preview available'}
        </div>
      </div>
    );
  }

  // Safe fallback to read either nested diff or flat structure
  const actualDiff = diff.diff ? diff.diff : (diff as any);
  
  // Make sure we have actual rows data before rendering
  if (typeof actualDiff.before_rows !== 'number' && typeof actualDiff.after_rows !== 'number') {
    return null;
  }

  const delta = actualDiff.row_delta ?? 0;
  const deltaColor = delta < 0
    ? "text-rose-500 font-bold"
    : delta > 0
    ? "text-emerald-500 font-bold"
    : "text-zinc-500";

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      className={`mt-4 rounded-2xl border p-5 shadow-sm transition-all ${
        darkMode
          ? "border-blue-500/30 bg-blue-950/20 text-zinc-100"
          : "border-[#0070AD]/30 bg-blue-50/30 text-zinc-900"
      }`}
    >
      {/* Header */}
      <div className="flex items-center justify-between pb-3 border-b border-black/5 dark:border-white/5 mb-4">
        <div className="flex items-center gap-2">
          <div className="p-2 rounded-lg bg-[#0070AD]/10 text-[#0070AD]">
            <FaDatabase className="text-base animate-pulse" />
          </div>
          <div>
            <h4 className="text-sm font-black tracking-tight uppercase">DuckDB Impact Preview & Diff</h4>
            <p className="text-[11px] opacity-60">
              Comparing source dataset <span className="font-semibold">{diff.before_dataset || 'input'}</span> with preview results
            </p>
          </div>
        </div>
      </div>

      {/* Row Metrics Grid */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-4">
        <div className={`p-3 rounded-xl border ${darkMode ? "bg-black/20 border-white/5" : "bg-white/80 border-black/5"}`}>
          <div className="text-[10px] font-black uppercase tracking-wider opacity-50">Before Rows</div>
          <div className="text-lg font-black mt-1">
            {typeof actualDiff.before_rows === 'number' ? actualDiff.before_rows.toLocaleString() : '—'}
          </div>
        </div>
        <div className={`p-3 rounded-xl border ${darkMode ? "bg-black/20 border-white/5" : "bg-white/80 border-black/5"}`}>
          <div className="text-[10px] font-black uppercase tracking-wider opacity-50">After Rows</div>
          <div className="text-lg font-black mt-1">
            {typeof actualDiff.after_rows === 'number' ? actualDiff.after_rows.toLocaleString() : '—'}
          </div>
        </div>
        <div className={`p-3 rounded-xl border ${darkMode ? "bg-black/20 border-white/5" : "bg-white/80 border-black/5"}`}>
          <div className="text-[10px] font-black uppercase tracking-wider opacity-50">Row Delta</div>
          <div className="flex items-center gap-1.5 mt-1">
            <span className={`text-lg font-black ${deltaColor}`}>
              {delta > 0 ? `+${delta.toLocaleString()}` : delta.toLocaleString()}
            </span>
            {delta < 0 && (
              <span className="text-[10px] bg-rose-500/10 text-rose-500 px-1.5 py-0.5 rounded-full font-bold">
                Rows Dropped
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Schema / Columns Diff */}
      <div className="space-y-3">
        {/* Added Columns */}
        {actualDiff.added_columns && actualDiff.added_columns.length > 0 && (
          <div className="flex flex-col sm:flex-row sm:items-center gap-2">
            <span className="text-[10px] font-black uppercase tracking-wider opacity-60 w-32 shrink-0">Added Columns:</span>
            <div className="flex flex-wrap gap-1.5">
              {actualDiff.added_columns.map((col: string) => (
                <span key={col} className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-600 border border-emerald-500/20">
                  + {col}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* Removed Columns */}
        {actualDiff.removed_columns && actualDiff.removed_columns.length > 0 && (
          <div className="flex flex-col sm:flex-row sm:items-center gap-2">
            <span className="text-[10px] font-black uppercase tracking-wider opacity-60 w-32 shrink-0">Removed Columns:</span>
            <div className="flex flex-wrap gap-1.5">
              {actualDiff.removed_columns.map((col: string) => (
                <span key={col} className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-rose-500/10 text-rose-600 border border-rose-500/20">
                  - {col}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* Dtype Changes */}
        {actualDiff.dtype_changes && actualDiff.dtype_changes.length > 0 && (
          <div className="space-y-1.5">
            <div className="text-[10px] font-black uppercase tracking-wider opacity-60">Data Type Changes:</div>
            <div className={`overflow-hidden rounded-xl border ${darkMode ? "border-white/5 bg-black/10" : "border-black/5 bg-white/40"}`}>
              <table className="w-full text-left text-[11px]">
                <thead className={darkMode ? "bg-white/5" : "bg-black/[0.02]"}>
                  <tr>
                    <th className="p-2 font-bold">Column</th>
                    <th className="p-2 font-bold">Before</th>
                    <th className="p-2 font-bold">After</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-black/5 dark:divide-white/5">
                  {actualDiff.dtype_changes.map((ch: any) => (
                    <tr key={ch.column}>
                      <td className="p-2 font-mono font-semibold">{ch.column}</td>
                      <td className="p-2 text-rose-500 font-mono">{ch.before}</td>
                      <td className="p-2 text-emerald-500 font-mono">{ch.after}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Key Overlap / Reconciliation issues */}
        {actualDiff.key_overlap && (
          <div className={`p-3 rounded-xl border flex items-center gap-3 text-xs ${
            actualDiff.key_overlap.keys_lost > 0
              ? darkMode ? "bg-rose-500/10 border-rose-500/20 text-rose-200" : "bg-rose-50 border-rose-100 text-rose-950"
              : darkMode ? "bg-emerald-500/10 border-emerald-500/20 text-emerald-200" : "bg-emerald-50 border-emerald-100 text-emerald-950"
          }`}>
            {actualDiff.key_overlap.keys_lost > 0 ? (
              <>
                <FaExclamationTriangle className="shrink-0 text-rose-500 animate-pulse text-base" />
                <div>
                  <span className="font-bold">Keys Reconciliation Issue: </span>
                  Lost <span className="font-black">{actualDiff.key_overlap.keys_lost}</span> keys.
                  {actualDiff.key_overlap.keys_new > 0 && ` Found ${actualDiff.key_overlap.keys_new} new keys.`}
                </div>
              </>
            ) : (
              <>
                <FaCheckCircle className="shrink-0 text-emerald-500 text-base" />
                <div>
                  <span className="font-bold">Keys Reconciled: </span>
                  All primary/business keys preserved perfectly.
                  {actualDiff.key_overlap.keys_new > 0 && ` Found ${actualDiff.key_overlap.keys_new} new keys.`}
                </div>
              </>
            )}
          </div>
        )}
      </div>

      {/* SQL Collapsible */}
      {diff.preview_sql_used && (
        <div className="mt-4 pt-3 border-t border-black/5 dark:border-white/5">
          <button
            type="button"
            onClick={() => setShowSql(!showSql)}
            className="flex items-center gap-1 text-[11px] font-bold opacity-70 hover:opacity-100 transition-opacity"
          >
            {showSql ? <FaChevronUp /> : <FaChevronDown />}
            <span>{showSql ? "Hide" : "Show"} Preview SQL Query</span>
          </button>
          {showSql && (
            <motion.pre
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: "auto", opacity: 1 }}
              className="mt-2 p-3 rounded-lg bg-zinc-950 font-mono text-[10px] text-emerald-100 overflow-x-auto whitespace-pre-wrap leading-relaxed border border-white/5"
            >
              {diff.preview_sql_used}
            </motion.pre>
          )}
        </div>
      )}
    </motion.div>
  );
}
