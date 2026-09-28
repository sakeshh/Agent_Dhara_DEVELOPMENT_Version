"""
Deploy PlanRecoveryWorkspace component and update ManualReviewPanel in Agent-Dhara---Frontend-App.
"""
import os

FRONTEND_ROOT = r"C:\Users\ssakesh\OneDrive - Capgemini\Documents\Agent-Dhara---Frontend-App"

WORKSPACE_COMPONENT = """'use client';

import { useState } from 'react';
import { motion } from 'framer-motion';
import { FaExclamationTriangle, FaCheckCircle, FaTools, FaUndo, FaRobot, FaBan } from 'react-icons/fa';
import { PlanRecoveryContract, RecoveryAdvisoryIssue } from '../types/pipeline';

export interface PlanRecoveryWorkspaceProps {
  recovery: PlanRecoveryContract;
  darkMode?: boolean;
  busy?: boolean;
  onApplyCorrections: (resolutions: Array<{ item_id: string; resolution_id: string }>) => Promise<void>;
  onAutoFixAll?: () => Promise<void>;
  onEditAll?: () => void;
  onResetDefaults?: () => Promise<void>;
}

export default function PlanRecoveryWorkspace({
  recovery,
  darkMode = false,
  busy = false,
  onApplyCorrections,
  onAutoFixAll,
  onEditAll,
  onResetDefaults,
}: PlanRecoveryWorkspaceProps) {
  // Pre-seed user picks with recommended alternatives
  const [picks, setPicks] = useState<Record<string, string>>(() => {
    const initial: Record<string, string> = {};
    (recovery.issues || []).forEach(issue => {
      const rec = issue.recommended_action || issue.allowed_actions.find(a => a.recommended)?.action || issue.allowed_actions[0]?.action;
      if (issue.item_id && rec) {
        initial[issue.item_id] = rec;
      }
    });
    return initial;
  });

  const [emergencyConfirm, setEmergencyConfirm] = useState(false);

  const handleFixSubmit = async () => {
    const payload = Object.entries(picks).map(([itemId, actionId]) => ({
      item_id: itemId,
      resolution_id: actionId,
    }));
    await onApplyCorrections(payload);
  };

  const handleAutoApply = async () => {
    if (onAutoFixAll) {
      await onAutoFixAll();
    } else {
      const payload = (recovery.issues || [])
        .filter(i => !!i.item_id && !!i.recommended_action)
        .map(i => ({
          item_id: i.item_id as string,
          resolution_id: i.recommended_action as string,
        }));
      await onApplyCorrections(payload);
    }
  };

  const border = darkMode ? 'border-amber-400/40 bg-amber-500/10' : 'border-amber-300 bg-amber-50/80';
  const cardBg = darkMode ? 'border-white/10 bg-black/40' : 'border-slate-200 bg-white shadow-sm';
  const text = darkMode ? 'text-zinc-100' : 'text-zinc-900';
  const sub = darkMode ? 'text-zinc-300' : 'text-zinc-600';

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      className={`rounded-2xl border p-5 ${border} mb-6`}
    >
      {/* Recovery Workspace Header */}
      <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
        <div className="flex items-center gap-2.5">
          <div className="rounded-lg bg-amber-500/20 p-2 text-amber-500 border border-amber-500/30">
            <FaTools className="text-base" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h2 className={`text-sm font-black uppercase tracking-wider ${text}`}>
                Plan Recovery Workspace
              </h2>
              <span className="rounded-full bg-rose-500/20 text-rose-600 dark:text-rose-300 border border-rose-500/30 px-2 py-0.5 text-[10px] font-bold">
                {recovery.unresolved_issues_count} Issue{recovery.unresolved_issues_count === 1 ? '' : 's'} Blocked
              </span>
            </div>
            <p className={`text-xs mt-0.5 ${sub}`}>
              Agent Dhara preserved <strong className="text-emerald-600 dark:text-emerald-400">{recovery.accepted_decisions_count} valid decisions</strong>. 
              Only the conflicting decisions below require your adjustment.
            </p>
          </div>
        </div>

        {/* Global Quick Action */}
        <div className="flex items-center gap-2">
          <button
            type="button"
            disabled={busy}
            onClick={() => void handleAutoApply()}
            className="inline-flex items-center gap-1.5 rounded-lg bg-emerald-600 px-3 py-1.5 text-xs font-bold text-white shadow-sm hover:bg-emerald-700 disabled:opacity-50 transition"
          >
            <FaCheckCircle className="text-[11px]" /> Auto-Apply Recommended Fixes
          </button>
        </div>
      </div>

      {/* Issues List */}
      <div className="space-y-3.5 mb-5">
        {(recovery.issues || []).map((issue: RecoveryAdvisoryIssue) => {
          const currentPick = (issue.item_id && picks[issue.item_id]) || issue.recommended_action;

          return (
            <div key={issue.issue_id} className={`rounded-xl border p-4 ${cardBg}`}>
              {/* Issue Metadata */}
              <div className="flex flex-wrap items-start justify-between gap-2 mb-2">
                <div>
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="rounded-md bg-slate-500/15 border border-slate-500/20 px-2 py-0.5 text-[11px] font-mono font-bold">
                      {issue.dataset} &bull; {issue.column || '*'}
                    </span>
                    <span className="rounded bg-rose-500/15 text-rose-600 dark:text-rose-400 text-[10px] font-bold uppercase tracking-wider px-1.5 py-0.5">
                      {issue.code}
                    </span>
                  </div>
                  <h3 className={`text-sm font-bold mt-1.5 ${text}`}>
                    {issue.title}
                  </h3>
                </div>
              </div>

              <p className={`text-xs leading-relaxed mb-3 ${sub}`}>
                {issue.explanation}
              </p>

              {/* Allowed Alternatives Picker */}
              <div className="space-y-2 mt-3">
                <div className={`text-[10px] font-bold uppercase tracking-wider ${sub}`}>
                  Select a legal resolution:
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                  {issue.allowed_actions.map(opt => {
                    const isChecked = currentPick === opt.action;

                    return (
                      <label
                        key={opt.action}
                        className={`flex cursor-pointer items-start gap-2.5 rounded-lg border p-2.5 text-xs transition ${
                          isChecked
                            ? darkMode
                              ? 'border-emerald-400/60 bg-emerald-500/15 text-emerald-100'
                              : 'border-emerald-500 bg-emerald-50/80 text-emerald-950 shadow-sm'
                            : darkMode
                              ? 'border-white/5 bg-white/5 hover:border-white/20'
                              : 'border-black/5 bg-black/[0.02] hover:border-black/20'
                        }`}
                      >
                        <input
                          type="radio"
                          name={`rec-issue-${issue.issue_id}`}
                          checked={isChecked}
                          onChange={() => {
                            if (issue.item_id) {
                              setPicks(prev => ({ ...prev, [issue.item_id as string]: opt.action }));
                            }
                          }}
                          className="mt-0.5 text-emerald-600 focus:ring-emerald-500"
                        />
                        <div className="flex-1">
                          <div className="flex flex-wrap items-center gap-1.5 font-bold">
                            <span>{opt.label}</span>
                            {opt.recommended && (
                              <span className="rounded bg-emerald-500/20 text-emerald-600 dark:text-emerald-300 text-[9px] px-1.5 py-0.2">
                                Recommended
                              </span>
                            )}
                            <span className={`text-[9px] font-mono px-1 py-0.2 rounded ${
                              opt.risk === 'LOW' ? 'bg-blue-500/15 text-blue-500' : 'bg-amber-500/15 text-amber-500'
                            }`}>
                              Risk: {opt.risk || 'LOW'}
                            </span>
                          </div>
                          {opt.description && (
                            <p className={`mt-0.5 text-[11px] leading-snug ${sub}`}>
                              {opt.description}
                            </p>
                          )}
                        </div>
                      </label>
                    );
                  })}
                </div>
              </div>

              {/* Prohibited Notices if any */}
              {issue.prohibited_actions && issue.prohibited_actions.length > 0 && (
                <div className="mt-3 pt-2.5 border-t border-black/5 dark:border-white/5 flex flex-wrap gap-2 text-[11px]">
                  {issue.prohibited_actions.map(p => (
                    <span key={p.action} className="inline-flex items-center gap-1 text-rose-500/90 dark:text-rose-400">
                      <FaBan className="text-[9px]" /> <strong>{p.action}</strong>: {p.reason}
                    </span>
                  ))}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Recovery Footer Actions */}
      <div className="flex flex-wrap items-center justify-between gap-3 pt-3 border-t border-black/10 dark:border-white/10">
        <div className="flex items-center gap-2">
          <button
            type="button"
            disabled={busy}
            onClick={() => void handleFixSubmit()}
            className="inline-flex items-center gap-2 rounded-xl bg-amber-600 px-4 py-2 text-sm font-bold text-white shadow hover:bg-amber-700 disabled:opacity-50 transition"
          >
            <FaTools className="text-xs" /> Apply Corrections &amp; Revalidate Plan
          </button>

          {onEditAll && (
            <button
              type="button"
              disabled={busy}
              onClick={onEditAll}
              className={`rounded-xl px-3 py-2 text-xs font-bold transition hover:bg-black/5 dark:hover:bg-white/5 ${sub}`}
            >
              Edit All Decisions
            </button>
          )}
        </div>

        {/* Emergency Reset */}
        {onResetDefaults && (
          <div>
            {!emergencyConfirm ? (
              <button
                type="button"
                disabled={busy}
                onClick={() => setEmergencyConfirm(true)}
                className="text-[11px] font-semibold text-rose-500/80 hover:text-rose-600 transition underline"
              >
                Emergency: Reset All to Defaults
              </button>
            ) : (
              <div className="flex items-center gap-1.5">
                <span className="text-[11px] text-rose-600 font-bold">Confirm reset?</span>
                <button
                  type="button"
                  disabled={busy}
                  onClick={() => {
                    setEmergencyConfirm(false);
                    void onResetDefaults();
                  }}
                  className="rounded px-2 py-0.5 bg-rose-600 text-white text-[10px] font-bold shadow hover:bg-rose-700"
                >
                  Yes, Reset
                </button>
                <button
                  type="button"
                  onClick={() => setEmergencyConfirm(false)}
                  className="text-[10px] text-slate-500 hover:text-slate-700"
                >
                  Cancel
                </button>
              </div>
            )}
          </div>
        )}
      </div>
    </motion.div>
  );
}
"""

def main():
    target_comp = os.path.join(FRONTEND_ROOT, "components", "PlanRecoveryWorkspace.tsx")
    with open(target_comp, "w", encoding="utf-8") as f:
        f.write(WORKSPACE_COMPONENT.strip() + "\n")
    print(f"Written: {target_comp}")

if __name__ == "__main__":
    main()
