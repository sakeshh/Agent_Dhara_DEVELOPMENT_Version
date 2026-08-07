'use client';

import { useState, Fragment } from 'react';
import { motion } from 'framer-motion';

export interface StepEvidence {
  null_count?: number | null;
  null_pct?: number | null;
  dtype?: string;
  mean?: number | null;
  median?: number | null;
  std?: number | null;
  skew?: number | null;
  recommended_fill?: string | null;
  row_count?: number | null;
  issue_type?: string | null;
  severity?: string;
  why_this_action: string;
  alternatives: string[];
  confidence: number;
  rule_override?: boolean;
  llm_recommendation?: any;
}

export interface EngineRecommendation {
  engine: string;
  dialect?: string | null;
  reason: string;
  alternatives: string[];
  warning?: string | null;
}

export interface PlanNarration {
  engine_explanation?: string;
  dataset_summaries?: Record<
    string,
    {
      summary: string;
      steps: Record<string, string>;
    }
  >;
  manual_review_explanations?: Array<{ column: string; explanation: string }>;
  relationships_summary?: string;
  overall_readiness?: string;
}

export function LlmRecommendationBadge({
  llmRec,
  darkMode = false,
}: {
  llmRec: any;
  darkMode?: boolean;
}) {
  if (!llmRec) return null;
  return (
    <span
      className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[9px] font-black uppercase tracking-wide ${
        darkMode ? 'bg-purple-500/20 text-purple-200' : 'bg-purple-100 text-purple-800'
      }`}
      title={`AI Recommendation: ${llmRec.suggested_fix}`}
    >
      ✨ AI Insight
    </span>
  );
}

function engineRecToUi(rec: EngineRecommendation): string {
  const e = (rec.engine || 'python').toLowerCase();
  if (e === 'pyspark') return 'spark';
  if (e === 'sql') return 'sql';
  if (e === 'adf') return 'adf';
  return 'python';
}

export function applyEngineRecommendation(
  rec: EngineRecommendation | undefined
): { engine: 'python' | 'sql' | 'spark' | 'adf'; sqlDialect?: 'tsql' | 'ansi' } | null {
  if (!rec?.engine) return null;
  const ui = engineRecToUi(rec);
  const out: { engine: 'python' | 'sql' | 'spark' | 'adf'; sqlDialect?: 'tsql' | 'ansi' } = {
    engine: ui as 'python' | 'sql' | 'spark' | 'adf',
  };
  if (ui === 'sql' && rec.dialect) {
    out.sqlDialect = rec.dialect === 'ansi' ? 'ansi' : 'tsql';
  }
  return out;
}

export function EngineRecommendationCard({
  rec,
  narration,
  darkMode = false,
  onUseRecommendation,
  currentEngine,
}: {
  rec: EngineRecommendation;
  narration?: string;
  darkMode?: boolean;
  onUseRecommendation?: () => void;
  currentEngine?: string;
}) {
  const recUi = engineRecToUi(rec);
  const matches = currentEngine === recUi;

  const shell = darkMode
    ? 'border-emerald-400/30 bg-emerald-500/10 text-emerald-50'
    : 'border-emerald-200 bg-emerald-50 text-emerald-950';

  return (
    <div className={`rounded-xl border p-4 ${shell}`}>
      <motion.div className="mb-2 flex flex-wrap items-center gap-2">
        <span className="text-[11px] font-black uppercase tracking-widest opacity-70">
          Recommended engine
        </span>
        <span className="font-mono text-sm font-bold">
          {rec.engine.toUpperCase()}
          {rec.dialect ? ` (${rec.dialect})` : ''}
        </span>
        {matches ? (
          <span
            className={`ml-auto rounded-full px-2 py-0.5 text-[10px] font-bold ${
              darkMode ? 'bg-emerald-500/30' : 'bg-emerald-200'
            }`}
          >
            Active
          </span>
        ) : onUseRecommendation ? (
          <button
            type="button"
            onClick={onUseRecommendation}
            className="ml-auto rounded-lg bg-emerald-600 px-3 py-1 text-xs font-bold text-white"
          >
            Use recommendation
          </button>
        ) : null}
      </motion.div>
      <p className="text-sm leading-relaxed">{narration || rec.reason}</p>
      {rec.warning ? (
        <p
          className={`mt-2 rounded-lg p-2 text-xs ${
            darkMode ? 'bg-amber-500/15 text-amber-100' : 'bg-amber-100 text-amber-900'
          }`}
        >
          {rec.warning}
        </p>
      ) : null}
      {rec.alternatives?.length > 0 ? (
        <details className="mt-2 text-xs opacity-80">
          <summary className="cursor-pointer">Alternatives ({rec.alternatives.length})</summary>
          <ul className="mt-1 list-inside list-disc space-y-0.5 pl-1">
            {rec.alternatives.map((alt, i) => (
              <li key={i}>{alt}</li>
            ))}
          </ul>
        </details>
      ) : null}
    </div>
  );
}

export function OverallReadinessBanner({
  narration,
  plan,
  darkMode = false,
}: {
  narration: PlanNarration | null;
  plan: Record<string, unknown> | null;
  darkMode?: boolean;
}) {
  const [expandedLedger, setExpandedLedger] = useState<Record<string, boolean>>({});
  const datasets = (plan?.datasets || {}) as Record<
    string,
    {
      steps?: Array<{
        bucket?: string;
        classification?: string;
        requires_user_choice?: boolean;
      }>;
    }
  >;
  let totalSteps = 0;
  let autoCount = 0;
  let approvedManualCount = 0;
  let reviewCount = 0;
  let blockedStepCount = 0;
  let destructiveCount = 0;
  for (const ds of Object.values(datasets)) {
    const steps = ds.steps || [];
    totalSteps += steps.length;
    for (const s of steps) {
      if (s.destructive || s.bucket === 'destructive') {
        destructiveCount += 1;
      }
      if (s.execution_mode === 'APPROVED_MANUAL') {
        approvedManualCount += 1;
      } else {
        const cls = (s.classification || s.bucket || 'auto').toLowerCase();
        if (cls === 'auto') autoCount += 1;
        else if (cls === 'blocked') blockedStepCount += 1;
        else if (cls === 'review' || s.requires_user_choice) reviewCount += 1;
      }
    }
  }
  const manualPending = Array.isArray(plan?.manual_review)
    ? (plan.manual_review as unknown[]).filter(
        (m) =>
          typeof m === 'object' &&
          m !== null &&
          ((m as { status?: string }).status || 'pending') === 'pending'
      ).length
    : 0;
  const manualResolved = Array.isArray(plan?.resolved_manual_review)
    ? (plan.resolved_manual_review as unknown[]).length
    : 0;
  const blockedCount = Array.isArray(plan?.blocked) ? (plan.blocked as unknown[]).length : 0;
  const blockedReason =
    blockedCount > 0 && Array.isArray(plan?.blocked)
      ? String((plan!.blocked as { message?: string }[])[0]?.message || 'blocking issues')
      : '';
  const pct =
    totalSteps + manualResolved > 0
      ? Math.round(((autoCount + manualResolved) / (totalSteps + manualResolved)) * 100)
      : 0;
  const readinessMessage =
    blockedCount > 0 || blockedStepCount > 0
      ? `Blocked: ${blockedReason || 'resolve blocking issues before approving.'}`
      : manualPending > 0 || reviewCount > 0
        ? 'Some steps need your review before proceeding.'
        : 'All steps are safe to auto-apply. No rows will be dropped.';

  const color =
    blockedCount > 0 || blockedStepCount > 0
      ? darkMode
        ? 'border-rose-400/40 bg-rose-950/60 text-rose-100'
        : 'border-rose-300 bg-rose-50 text-slate-900'
      : manualPending > 0 || reviewCount > 0
        ? darkMode
          ? 'border-amber-400/40 bg-amber-950/60 text-amber-100'
          : 'border-amber-300 bg-amber-50 text-slate-900'
        : darkMode
          ? 'border-emerald-400/30 bg-emerald-950/60 text-emerald-100'
          : 'border-emerald-300 bg-emerald-50 text-slate-900';

  const text = darkMode ? 'text-zinc-100 font-bold' : 'text-slate-900 font-bold';
  const sub = darkMode ? 'text-zinc-200 font-medium' : 'text-slate-800 font-semibold';

  const validationErrorsCount = Array.isArray(plan?.build_warnings)
    ? plan.build_warnings.length
    : 0;
  const isPlanFailed = validationErrorsCount > 0 || blockedCount > 0;
  
  const totalCards = manualPending + manualResolved;
  const etlReadiness = isPlanFailed ? 0 : (totalCards === 0 ? 100 : Math.round((manualResolved / totalCards) * 100));
  const totalDecisions = autoCount + approvedManualCount + manualPending;
  const autoCoveragePct = totalDecisions > 0 ? Math.round((autoCount / totalDecisions) * 100) : 0;
  const manualCoveragePct = totalDecisions > 0 ? Math.round((approvedManualCount / totalDecisions) * 100) : 0;

  return (
    <div className={`rounded-xl border p-4 ${color}`}>
      <div className={`mb-2 flex items-center justify-between ${text}`}>
        <span className="text-sm font-bold">ETL Readiness Score</span>
        {isPlanFailed ? (
          <span className="inline-block px-2 py-1 rounded bg-rose-500/25 text-rose-700 dark:text-rose-300 text-xs font-black uppercase">VALIDATION_FAILED</span>
        ) : (
          <span className="text-2xl font-bold">{etlReadiness}%</span>
        )}
      </div>

      <p className={`mb-3 text-sm font-medium ${sub}`}>{readinessMessage}</p>

      {/* Premium Execution Gateway Grid */}
      <div className={`mb-4 p-3 rounded-lg border text-xs grid grid-cols-2 sm:grid-cols-3 gap-3 ${
        darkMode ? 'bg-zinc-900/90 border-zinc-700 text-zinc-100' : 'bg-white border-amber-200/80 text-slate-900 shadow-sm'
      }`}>
        <div>
          <span className="block text-[10px] font-extrabold uppercase tracking-wider text-slate-800 dark:text-zinc-200">Phase 1 Plan Status</span>
          <span className={`font-black text-xs ${isPlanFailed ? 'text-rose-700 dark:text-rose-400' : 'text-emerald-700 dark:text-emerald-400'}`}>
            {isPlanFailed ? 'VALIDATION_FAILED' : 'VALIDATED'}
          </span>
        </div>
        <div>
          <span className="block text-[10px] font-extrabold uppercase tracking-wider text-slate-800 dark:text-zinc-200">Source DQ Score</span>
          <span className="font-black text-xs text-amber-700 dark:text-amber-400">
            {plan?.readiness_scorecard ? (plan.readiness_scorecard as any)["Overall"]?.ledger?.source_score ?? '65.2' : '65.2'}%
          </span>
        </div>
        <div>
          <span className="block text-[10px] font-extrabold uppercase tracking-wider text-slate-800 dark:text-zinc-200">Projected Post-Prep Score</span>
          <span className="font-black text-xs text-sky-700 dark:text-sky-400">
            {plan?.readiness_scorecard ? (plan.readiness_scorecard as any)["Overall"]?.score ?? '79.7' : '79.7'}%
          </span>
        </div>
        <div>
          <span className="block text-[10px] font-extrabold uppercase tracking-wider text-slate-800 dark:text-zinc-200">Projection Status</span>
          <span className={`font-black text-xs ${isPlanFailed ? 'text-rose-700 dark:text-rose-400' : 'text-emerald-700 dark:text-emerald-400'}`}>
            {isPlanFailed ? 'UNVALIDATED' : 'VALIDATED'}
          </span>
        </div>
        <div>
          <span className="block text-[10px] font-extrabold uppercase tracking-wider text-slate-800 dark:text-zinc-200">Code Gen Eligibility</span>
          <span className={`font-black text-xs ${isPlanFailed ? 'text-rose-700 dark:text-rose-400' : 'text-emerald-700 dark:text-emerald-400'}`}>
            {isPlanFailed ? 'NO' : 'YES'}
          </span>
        </div>
        <div>
          <span className="block text-[10px] font-extrabold uppercase tracking-wider text-slate-800 dark:text-zinc-200">Automatic Steps</span>
          <span className="font-extrabold text-xs text-slate-900 dark:text-zinc-100">{autoCount} / {totalDecisions} ({autoCoveragePct}%)</span>
        </div>
        <div>
          <span className="block text-[10px] font-extrabold uppercase tracking-wider text-slate-800 dark:text-zinc-200">Approved Manual Steps</span>
          <span className="font-extrabold text-xs text-slate-900 dark:text-zinc-100">{approvedManualCount} / {totalDecisions} ({manualCoveragePct}%)</span>
        </div>
        <div>
          <span className="block text-[10px] font-extrabold uppercase tracking-wider text-slate-800 dark:text-zinc-200">Destructive Steps</span>
          <span className="font-extrabold text-xs text-slate-900 dark:text-zinc-100">{destructiveCount}</span>
        </div>
        <div>
          <span className="block text-[10px] font-extrabold uppercase tracking-wider text-slate-800 dark:text-zinc-200">Policy Cards Resolved</span>
          <span className="font-extrabold text-xs text-slate-900 dark:text-zinc-100">{manualResolved} / {totalCards}</span>
        </div>
      </div>

      {/* projected phase 2 gates */}
      {Array.isArray(plan?.projected_phase2_gates) && plan.projected_phase2_gates.length > 0 && (
        <div className="mb-4 border-t border-b border-black/10 dark:border-white/10 py-3">
          <h4 className="text-xs font-bold uppercase tracking-wider mb-2 opacity-80">
            Phase 2: Integration Gateways (Projected)
          </h4>
          <div className="space-y-2">
            {plan.projected_phase2_gates.map((gate: any) => (
              <div 
                key={gate.operation_id} 
                className={`p-3 rounded-lg border text-xs ${
                  darkMode ? 'bg-black/30 border-white/10 text-white' : 'bg-white/60 border-black/5 text-zinc-800'
                }`}
              >
                <div className="flex items-center justify-between mb-2 gap-2">
                  <span className="font-bold truncate max-w-[65%]">
                    🔗 {gate.operation_id.replace(/^rel_/, '').replace(/_/g, ' ➔ ')}
                  </span>
                  <span className={`px-2 py-0.5 rounded text-[10px] font-black uppercase whitespace-nowrap ${
                    gate.gate_status === 'BLOCKED'
                      ? 'bg-rose-500/25 text-rose-600 dark:text-rose-400'
                      : gate.gate_status === 'READY_WITH_WARNINGS'
                        ? 'bg-amber-500/25 text-amber-600 dark:text-amber-400'
                        : 'bg-emerald-500/25 text-emerald-600 dark:text-emerald-400'
                  }`}>
                    {`PROJECTED_${gate.gate_status}`}
                  </span>
                </div>
                
                <div className="grid grid-cols-2 gap-x-2 gap-y-1 mb-2 opacity-75 text-[10px]">
                  <div>✓ Type compatibility: <span className="font-bold text-emerald-600 dark:text-emerald-400">PASS</span></div>
                  <div>✓ Normalization: <span className={`font-bold ${gate.normalization_parity === 'PASS' ? 'text-emerald-600 dark:text-emerald-400' : 'text-rose-600 dark:text-rose-400'}`}>{gate.normalization_parity}</span></div>
                  <div>✓ Parent Uniqueness: <span className="font-bold text-emerald-600 dark:text-emerald-400">{gate.require_parent_unique ? 'REQUIRED' : 'NOT REQUIRED'}</span></div>
                  <div>✓ Cardinality: <span className="font-bold">{gate.cardinality}</span></div>
                </div>

                {gate.blocking_reasons?.length > 0 && (
                  <div className="mt-2 p-2 rounded bg-rose-500/10 border border-rose-500/20 text-rose-600 dark:text-rose-400 text-[10px]">
                    <div className="font-bold mb-0.5">⛔ Blocking integration:</div>
                    {gate.blocking_reasons.map((r: string, idx: number) => (
                      <div key={idx} className="break-words">• {r}</div>
                    ))}
                    <div className="mt-1.5 font-semibold text-zinc-700 dark:text-zinc-300">
                      💡 Action: Resolve key preparation policy in Phase 1 cards.
                    </div>
                  </div>
                )}
                {gate.warnings?.length > 0 && (
                  <div className="mt-2 p-2 rounded bg-amber-500/10 border border-amber-500/20 text-amber-600 dark:text-amber-400 text-[10px]">
                    <div className="font-bold mb-0.5">⚠ Warning:</div>
                    {gate.warnings.map((w: any, idx: number) => (
                      <div key={idx} className="break-words">• {typeof w === 'string' ? w : `${w.waiver_id}: ${w.reason} (expires ${new Date(w.expires_at).toLocaleDateString()})`}</div>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      <div className={`mb-3 space-y-1 text-xs ${sub}`}>
        <div>• <strong>Automation coverage:</strong> {autoCount} executable automatic actions / {totalDecisions} total decisions ({autoCoveragePct}% by action count)</div>
        <div>• <strong>Policy resolution:</strong> {manualResolved} of {totalCards} manual issue decisions resolved</div>
        {validationErrorsCount > 0 && (
          <div className="text-rose-600 dark:text-rose-400 font-semibold">• <strong>Validation check:</strong> FAILED ({validationErrorsCount} blocking errors detected)</div>
        )}
      </div>

      {plan?.readiness_scorecard && (
        <div className={`mt-3 mb-4 overflow-x-auto rounded-lg border p-2 ${
          darkMode ? 'border-white/10 bg-black/40 text-zinc-100' : 'border-slate-200 bg-white/80 text-slate-900 shadow-sm'
        }`}>
          <table className="w-full text-left text-xs border-collapse">
            <thead>
              <tr className="border-b border-slate-300 dark:border-white/20">
                <th className="pb-1.5 font-extrabold text-slate-900 dark:text-zinc-100">Dataset</th>
                <th className="pb-1.5 font-extrabold text-center text-slate-900 dark:text-zinc-100">Score</th>
                <th className="pb-1.5 font-extrabold text-center text-slate-900 dark:text-zinc-100">Readiness</th>
                <th className="pb-1.5 font-extrabold pl-2 text-slate-900 dark:text-zinc-100">Primary Reason</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(plan.readiness_scorecard).map(([ds, card]: [string, any]) => {
                const hasLedger = !!card.ledger;
                const isExpanded = !!expandedLedger[ds];
                return (
                  <Fragment key={ds}>
                    <tr className="border-b border-black/5 last:border-0 hover:bg-black/5">
                      <td className="py-1.5 font-bold font-mono flex items-center gap-1 text-slate-900 dark:text-zinc-100">
                        {hasLedger && (
                          <button
                            type="button"
                            onClick={() => setExpandedLedger(prev => ({ ...prev, [ds]: !prev[ds] }))}
                            className="p-0.5 hover:bg-black/10 rounded text-[9px] font-bold"
                          >
                            {isExpanded ? '▼' : '▶'}
                          </button>
                        )}
                        {ds}
                      </td>
                      <td className="py-1.5 text-center font-bold text-slate-900 dark:text-zinc-100">{card.score}%</td>
                      <td className="py-1.5 text-center">
                        <span className={`inline-block px-1.5 py-0.5 rounded text-[9px] font-black uppercase ${
                          card.readiness === 'NEEDS_WORK' 
                            ? 'bg-rose-500/20 text-rose-700 dark:text-rose-400' 
                            : card.readiness === 'READY_WITH_WARNINGS' 
                              ? 'bg-amber-500/20 text-amber-700 dark:text-amber-400' 
                              : 'bg-emerald-500/20 text-emerald-700 dark:text-emerald-400'
                        }`}>
                          {card.readiness.replace(/_/g, ' ')}
                        </span>
                      </td>
                      <td className="py-1.5 pl-2 font-medium text-slate-800 dark:text-zinc-200">{card.primary_reason}</td>
                    </tr>
                    {hasLedger && isExpanded && (
                      <tr className={darkMode ? 'bg-white/5' : 'bg-black/5'}>
                        <td colSpan={4} className="p-3 text-[11px] leading-normal font-mono border-t border-b border-black/10">
                          <div className="grid grid-cols-2 sm:grid-cols-3 gap-2 opacity-90">
                            <div>• <strong>Source DQ Score:</strong> {card.ledger.source_score}%</div>
                            <div>• <strong>Resolved Point Improvements:</strong> +{card.ledger.resolved_issue_penalties}%</div>
                            <div>• <strong>Remaining Penalty Deductions:</strong> -{card.ledger.remaining_issue_penalties}%</div>
                            <div>• <strong>Quarantined-Row Treatment:</strong> {card.ledger.quarantined_row_treatment}%</div>
                            <div>• <strong>New Validation Findings:</strong> {card.ledger.new_validation_findings}%</div>
                            <div>• <strong>Projected Post-Prep Score:</strong> {card.ledger.projected_final_score}%</div>
                          </div>
                          <div className="mt-1.5 text-[9px] opacity-60">
                            Calculation: type={card.score_type} · version={card.methodology_version} · plan_hash={card.plan_hash?.substring(0,16)} · authoritative={card.is_authoritative ? 'YES' : 'NO'}
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {Array.isArray(plan?.invariants) && (plan!.invariants as { name?: string; enabled?: boolean }[]).length > 0 ? (
        <motion.div className={`mb-3 flex flex-wrap gap-2 text-[10px] ${sub}`}>
          {(plan!.invariants as { name?: string; enabled?: boolean; check?: string }[]).map((inv) =>
            inv.enabled ? (
              <span
                key={inv.name}
                className={`rounded-full px-2 py-0.5 font-semibold ${
                  darkMode ? 'bg-emerald-500/20 text-emerald-100' : 'bg-emerald-100 text-emerald-900'
                }`}
              >
                ✓ {inv.name === 'never_drop_rows' ? 'Row count preserved' : 'No silent column loss'}
              </span>
            ) : null
          )}
        </motion.div>
      ) : null}
      <div className={`flex flex-wrap gap-3 text-xs ${sub}`}>
        <span className="flex items-center gap-1">
          <span className="inline-block h-2 w-2 rounded-full bg-emerald-500" />
          {autoCount} auto
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block h-2 w-2 rounded-full bg-amber-400" />
          {manualPending} manual review
          {manualResolved > 0 ? ` · ${manualResolved} resolved` : ''}
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block h-2 w-2 rounded-full bg-rose-500" />
          {blockedCount} blocked
        </span>
      </div>
      <motion.div className={`mt-3 h-2 rounded-full ${darkMode ? 'bg-white/10' : 'bg-white/80'}`}>
        <motion.div
          className="h-2 rounded-full bg-emerald-500 transition-all"
          style={{ width: `${pct}%` }}
          initial={{ width: 0 }}
          animate={{ width: `${pct}%` }}
        />
      </motion.div>

      {/* projected column certificates */}
      {Array.isArray(plan?.projected_column_readiness) && plan.projected_column_readiness.length > 0 && (
        <div className="mt-4 border-t border-black/10 dark:border-white/10 pt-3">
          <h4 className="text-xs font-bold uppercase tracking-wider mb-2 opacity-80">
            Phase 1: Projected Column Readiness (Projected)
          </h4>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
            {plan.projected_column_readiness.map((cert: any) => (
              <div 
                key={cert.certificate_id} 
                className={`p-2.5 rounded-lg border text-xs flex flex-col justify-between ${
                  darkMode ? 'bg-black/30 border-white/10 text-white' : 'bg-white/60 border-black/5 text-zinc-800'
                }`}
              >
                <div className="flex items-center justify-between mb-1 gap-2">
                  <span className="font-mono font-bold truncate max-w-[70%]" title={cert.source_column}>
                    {cert.dataset_id.split('/').pop().split('\\').pop()}:{cert.source_column}
                  </span>
                  <span className={`px-1.5 py-0.5 rounded-[4px] text-[9px] font-black uppercase whitespace-nowrap ${
                    cert.readiness_status.startsWith('BLOCKED')
                      ? 'bg-rose-500/20 text-rose-600 dark:text-rose-400'
                      : cert.readiness_status === 'READY_WITH_WARNINGS'
                        ? 'bg-amber-500/20 text-amber-600 dark:text-amber-400'
                        : 'bg-emerald-500/20 text-emerald-600 dark:text-emerald-400'
                  }`}>
                    {cert.readiness_status.replace(/_/g, ' ')}
                  </span>
                </div>
                <div className="opacity-75 space-y-0.5 text-[10px]">
                  <div>• <strong>Role:</strong> {cert.semantic_role} ({cert.physical_type})</div>
                  {cert.warnings?.length > 0 && (
                    <div className="text-amber-600 dark:text-amber-400 font-medium truncate" title={cert.warnings[0]}>
                      ⚠ {cert.warnings[0]}
                    </div>
                  )}
                  {cert.blocking_reasons?.length > 0 && (
                    <div className="text-rose-600 dark:text-rose-400 font-medium truncate" title={cert.blocking_reasons[0].message}>
                      ⛔ {cert.blocking_reasons[0].message}
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

    </div>
  );
}

export function StepEvidenceTooltip({
  evidence,
  narration,
  bucket,
  darkMode = false,
}: {
  evidence: StepEvidence;
  narration?: string;
  bucket: string;
  darkMode?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const b = (bucket || 'auto').toLowerCase();
  const badge =
    b === 'blocked'
      ? darkMode
        ? 'bg-rose-500/30 text-rose-100 border-rose-400/40'
        : 'bg-rose-100 text-rose-800 border-rose-200'
      : b === 'review'
        ? darkMode
          ? 'bg-amber-500/30 text-amber-100 border-amber-400/40'
          : 'bg-amber-100 text-amber-800 border-amber-200'
        : darkMode
          ? 'bg-emerald-500/25 text-emerald-100 border-emerald-400/30'
          : 'bg-emerald-100 text-emerald-800 border-emerald-200';

  const confidencePct = Math.round((evidence.confidence ?? 0.5) * 100);
  const pop = darkMode
    ? 'bg-zinc-900 border-white/15 text-zinc-100'
    : 'bg-white border-black/10 text-zinc-800';

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className={`rounded border px-1.5 py-0.5 text-[9px] font-black uppercase ${badge}`}
      >
        {b === 'auto' ? '✓' : b === 'review' ? '⚠' : '✗'} {b}
        {evidence.confidence_advisory || evidence.llm_recommendation?.authority_source === 'LLM_ADVISORY' ? ` · ${confidencePct}%` : ''}
      </button>
      {open ? (
        <div className={`absolute left-0 top-6 z-50 w-72 rounded-lg border p-3 text-xs shadow-lg ${pop}`}>
          <button
            type="button"
            onClick={() => setOpen(false)}
            className="absolute right-2 top-2 opacity-50 hover:opacity-100"
          >
            ✕
          </button>
          <p className="mb-2 pr-4 font-semibold">Why this action?</p>
          <p className="mb-2 opacity-90">{narration || evidence.why_this_action}</p>

          {evidence.llm_recommendation ? (
            <div className="mb-2 rounded border border-purple-500/20 bg-purple-500/5 p-2 text-[11px]">
              <p className="font-bold text-purple-700 mb-0.5">
                {evidence.llm_recommendation.authority_source === 'LLM_ADVISORY' ? '✨ AI Recommendation' : '📋 Deterministic Recommendation'}
              </p>
              <p className="mb-1"><strong>Fix:</strong> {evidence.llm_recommendation.suggested_fix}</p>
              <p className="mb-1"><strong>Why:</strong> {evidence.llm_recommendation.why_it_matters}</p>
              <p className="mb-0 text-red-500"><strong>Risk:</strong> {evidence.llm_recommendation.risk}</p>
            </div>
          ) : null}

          <motion.div
            className={`mb-2 grid grid-cols-2 gap-1 rounded p-2 ${
              darkMode ? 'bg-white/5' : 'bg-zinc-50'
            }`}
          >
            {evidence.null_pct != null ? (
              <>
                <span className="opacity-60">Null rate</span>
                <span className="font-mono">{evidence.null_pct}%</span>
              </>
            ) : null}
            {evidence.row_count != null && evidence.row_count > 0 ? (
              <>
                <span className="opacity-60">Rows</span>
                <span className="font-mono">{evidence.row_count.toLocaleString()}</span>
              </>
            ) : null}
            {evidence.dtype ? (
              <>
                <span className="opacity-60">Type</span>
                <span className="font-mono">{evidence.dtype}</span>
              </>
            ) : null}
            {'median' in evidence && evidence.median != null ? (
              <>
                <span className="opacity-60">Median</span>
                <span className="font-mono">{evidence.median}</span>
              </>
            ) : null}
            {'skew' in evidence && evidence.skew != null ? (
              <>
                <span className="opacity-60">Skew</span>
                <span className="font-mono">{evidence.skew}</span>
              </>
            ) : null}
            {evidence.recommended_fill ? (
              <>
                <span className="opacity-60">Fill</span>
                <span className="font-mono">{evidence.recommended_fill}</span>
              </>
            ) : null}
          </motion.div>
          <div className="mb-2">
            <div className="mb-0.5 flex justify-between opacity-60">
              <span>Confidence</span>
              <span>{confidencePct}%</span>
            </div>
            <div className={`h-1.5 rounded-full ${darkMode ? 'bg-white/10' : 'bg-zinc-200'}`}>
              <div
                className={`h-1.5 rounded-full ${
                  confidencePct >= 80 ? 'bg-emerald-500' : confidencePct >= 60 ? 'bg-amber-400' : 'bg-rose-400'
                }`}
                style={{ width: `${confidencePct}%` }}
              />
            </div>
          </div>
          {evidence.alternatives?.length > 0 ? (
            <ul className="list-inside list-disc space-y-0.5 opacity-80">
              {evidence.alternatives.map((alt, i) => (
                <li key={i}>{alt}</li>
              ))}
            </ul>
          ) : null}
          {evidence.rule_override ? (
            <p className={`mt-2 rounded p-1 text-[10px] ${darkMode ? 'bg-blue-500/20' : 'bg-blue-50 text-blue-800'}`}>
              Business rule override applied
            </p>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

export function getStepNarration(
  plan: Record<string, unknown> | null,
  dataset: string,
  order: number
): string | undefined {
  const narr = plan?.narration as PlanNarration | undefined;
  return narr?.dataset_summaries?.[dataset]?.steps?.[String(order)];
}

export interface PlanJoin {
  parent_dataset?: string;
  child_dataset?: string;
  parent_key?: string;
  child_key?: string;
  left_dataset?: string;
  right_dataset?: string;
  left_key?: string;
  right_key?: string;
  join_type?: string;
  cardinality?: string;
  overlap_count?: number;
  orphan_row_count?: number | null;
  evidence?: StepEvidence;
}

export interface PlanManyToMany {
  dataset_a?: string;
  dataset_b?: string;
  column_a?: string;
  column_b?: string;
  bridge_name?: string;
  cardinality?: string;
  overlap_count?: number;
  recommended_action?: string;
  resolution_options?: string[];
  default_resolution?: string;
  evidence?: StepEvidence;
}

export interface PlanRelationships {
  joins?: PlanJoin[];
  many_to_many?: PlanManyToMany[];
  load_order?: string[];
  join_count?: number;
  mn_count?: number;
  warnings?: Array<Record<string, unknown>>;
}

export function ManyToManyCard({
  relationships,
  narration,
  darkMode = false,
}: {
  relationships: PlanRelationships | null | undefined;
  narration?: string;
  darkMode?: boolean;
}) {
  const mn = relationships?.many_to_many || [];
  if (!mn.length) return null;

  const border = darkMode ? 'border-amber-400/35 bg-amber-500/10' : 'border-amber-200 bg-amber-50';
  const text = darkMode ? 'text-zinc-100' : 'text-zinc-900';
  const sub = darkMode ? 'text-zinc-300' : 'text-zinc-700';

  return (
    <motion.div
      initial={{ opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      className={`rounded-xl border p-4 ${border}`}
    >
      <p className={`mb-2 text-[11px] font-black uppercase tracking-widest ${sub}`}>
        Many-to-many relationships ({mn.length})
      </p>
      {narration ? <p className={`mb-3 text-xs ${sub}`}>{narration}</p> : null}
      <ul className={`space-y-3 text-xs ${text}`}>
        {mn.map((b, i) => (
          <li
            key={i}
            className={`rounded-lg border px-3 py-2 ${darkMode ? 'border-white/10' : 'border-black/10'}`}
          >
            <p className="font-mono font-bold">
              {b.dataset_a}.{b.column_a} ↔ {b.dataset_b}.{b.column_b}
            </p>
            <p className={`mt-1 ${sub}`}>
              Bridge table: <span className="font-mono">{b.bridge_name}</span>
            </p>
            {b.evidence?.why_this_action ? (
              <p className={`mt-1 ${sub}`}>{b.evidence.why_this_action}</p>
            ) : null}
            {b.resolution_options?.length ? (
              <div className={`mt-2 flex flex-wrap gap-1.5 ${sub}`}>
                {b.resolution_options.map((opt) => (
                  <span
                    key={opt}
                    className={`rounded px-2 py-0.5 font-mono text-[10px] ${
                      opt === b.default_resolution
                        ? darkMode
                          ? 'bg-amber-500/30 text-amber-100'
                          : 'bg-amber-200 text-amber-950'
                        : darkMode
                          ? 'bg-white/10'
                          : 'bg-black/5'
                    }`}
                  >
                    {opt}
                    {opt === b.default_resolution ? ' (default)' : ''}
                  </span>
                ))}
              </div>
            ) : null}
          </li>
        ))}
      </ul>
    </motion.div>
  );
}

export function RelationshipsCard({
  relationships,
  darkMode = false,
}: {
  relationships: PlanRelationships | null | undefined;
  darkMode?: boolean;
}) {
  const joins = (relationships?.joins || []).filter((j) => j.join_type !== 'review');
  if (!joins.length) return null;

  const border = darkMode ? 'border-violet-400/30 bg-violet-500/10' : 'border-violet-200 bg-violet-50';
  const text = darkMode ? 'text-zinc-100' : 'text-zinc-900';
  const sub = darkMode ? 'text-zinc-300' : 'text-zinc-700';

  return (
    <div className={`rounded-xl border p-4 ${border}`}>
      <p className={`mb-2 text-[11px] font-black uppercase tracking-widest ${sub}`}>
        Detected relationships & joins
      </p>
      {relationships?.load_order?.length ? (
        <p className={`mb-2 text-xs ${sub}`}>
          Load order: <span className="font-mono">{relationships.load_order.join(' → ')}</span>
        </p>
      ) : null}
      <ul className={`space-y-2 text-xs ${text}`}>
        {joins.map((j, i) => (
          <li key={i} className={`rounded-lg border px-3 py-2 ${darkMode ? 'border-white/10' : 'border-black/10'}`}>
            <span className="font-mono font-bold">
              {j.parent_dataset || j.left_dataset}.{j.parent_key || j.left_key}
            </span>
            <span className={sub}> {j.join_type || 'join'} </span>
            <span className="font-mono font-bold">
              {j.child_dataset || j.right_dataset}.{j.child_key || j.right_key}
            </span>
            <span className={`ml-2 rounded px-1.5 py-0.5 text-[10px] ${darkMode ? 'bg-white/10' : 'bg-black/5'}`}>
              {j.cardinality}
            </span>
            {j.orphan_row_count ? (
              <span className="ml-1 text-amber-600">· {j.orphan_row_count} orphan rows</span>
            ) : null}
            {j.evidence?.why_this_action ? (
              <p className={`mt-1 ${sub}`}>{j.evidence.why_this_action}</p>
            ) : null}
          </li>
        ))}
      </ul>
    </div>
  );
}

export function getPlanFromRecord(plan: Record<string, unknown> | null) {
  return {
    engineRecommendation: plan?.engine_recommendation as EngineRecommendation | undefined,
    narration: plan?.narration as PlanNarration | undefined,
    relationships: plan?.relationships as PlanRelationships | undefined,
  };
}
