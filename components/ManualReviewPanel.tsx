'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { FaChevronDown, FaChevronUp, FaSlidersH, FaShieldAlt } from 'react-icons/fa';
import RecommendationComparisonPanel from './RecommendationComparisonPanel';

export interface ResolutionOption {
  id: string;
  label: string;
  action: string;
  recommended?: boolean;
  description?: string;
  disabled?: boolean;
  disabled_reason?: string;
  authority_source?: string;
  llm_metadata?: any;
}

export interface ManualReviewItem {
  id: string;
  dataset?: string | null;
  column?: string | null;
  issue_type?: string;
  severity?: string;
  message?: string;
  guidance?: string;
  status?: string;
  default_resolution?: string;
  resolution_options?: ResolutionOption[];
  llm_recommendation?: any;
  risk_tier?: 'standard' | 'complex' | 'non_fixable';
}

export interface ManualReviewPanelProps {
  items: ManualReviewItem[];
  darkMode?: boolean;
  busy?: boolean;
  onApply: (resolutions: Array<{ item_id: string; resolution_id: string }>) => Promise<void>;
}

const POLICY_GROUPS = {
  POLICY_PLACEHOLDER: {
    label: "Normalize placeholder values to null",
    description: "Standardize non-semantic/empty values (e.g. '?', 'N/A', 'UNKNOWN') as database NULLs.",
    issue_types: ["placeholder_detected", "punctuation_only_value", "sentinel_numeric_value"],
  },
  POLICY_DECLARED_KEY_NULL: {
    label: "Disposition of nulls in mandatory business keys",
    description: "Specify handling for rows where a designated business key (e.g. campaign_id) is NULL.",
    issue_types: ["declared_key_null_violation", "declared_key_high_null_ratio"],
  },
  POLICY_SUSPECTED_KEY_NULL: {
    label: "Disposition of nulls in suspected identifiers",
    description: "Null percentage on key/identifier columns exceeds threshold. Imputation is forbidden.",
    issue_types: ["high_null_percentage", "suspected_key_high_null_ratio"],
  },
  POLICY_CAMPAIGN_RI: {
    label: "Referential integrity and orphan partitioning",
    description: "Resolve child foreign keys not present in parent reference dataset.",
    issue_types: ["orphan_foreign_keys", "orphan_foreign_key_rows", "referential_integrity_violation"],
  },
  POLICY_NUMERIC_PARSE: {
    label: "Numeric parsing and coercion",
    description: "Coerce text-encoded numeric representations (e.g. budget, amount) into canonical numeric fields.",
    issue_types: ["invalid_numeric_values", "invalid_numeric", "numeric_value_serialized_as_string", "mixed_scalar_types", "suspicious_zero"],
  },
  POLICY_DATE_PARSE: {
    label: "Date standardisation and parsing",
    description: "Parse date strings in various formats (e.g. start_date, sale_date) into ISO-compliant dates.",
    issue_types: ["invalid_date_format", "mixed_date_formats", "date_format_inconsistency", "date_range_violation", "future_dates", "weekend_date_anomaly", "date_clumping_month_end"],
  },
  POLICY_CATEGORICAL_MAP: {
    label: "Categorical domain value mapping",
    description: "Resolve categorical invalid categories or enum violations against static domain schemas.",
    issue_types: ["invalid_category", "enum_violation", "string_with_only_digits_in_text_column"],
  },
  POLICY_EXACT_DEDUP: {
    label: "Exact duplicates disposition",
    description: "Identify and flag/deduplicate rows that are byte-for-byte exact matches.",
    issue_types: ["exact_duplicate_rows", "duplicate_primary_key", "business_key_duplicate"],
  },
  POLICY_NEAR_DEDUP: {
    label: "Near-duplicates cluster resolution",
    description: "Review rows sharing high textual/numeric similarity and choose a grouping policy.",
    issue_types: ["near_duplicate_rows"],
  },
  POLICY_NEGATIVE_AMOUNT: {
    label: "Negative financial amount flags",
    description: "Financial values fall below zero. Flag for policy compliance review.",
    issue_types: ["negative_values", "cross_field_non_negative", "negative_values_in_non_negative_measure", "negative_values_review_required", "declared_non_negative_rule_violation"],
  },
  POLICY_GENERIC_NULL: {
    label: "Generic optional-field null policy",
    description: "Specify default treatment for null fields in optional, non-key attributes.",
    issue_types: ["nulls", "null_values"],
  },
  POLICY_WHITESPACE_NORMALIZATION: {
    label: "Whitespace and naming normalization",
    description: "Identify leading/trailing whitespaces or column name formatting anomalies.",
    issue_types: ["whitespace", "internal_whitespace", "column_name_whitespace"],
  },
  POLICY_STRING_LENGTH: {
    label: "String-length and formats anomaly review",
    description: "Identify email, phone, PAN, GSTIN, Aadhaar, CIN or string length violations.",
    issue_types: ["string_length_anomaly", "invalid_phone", "invalid_pan", "invalid_gstin", "invalid_aadhaar", "invalid_cin", "invalid_email", "invalid_url", "invalid_uuid"],
  },
  POLICY_CUSTOM_BUSINESS: {
    label: "Custom business-rule flags",
    description: "Custom rules, formulas, and user-defined pattern validation warnings.",
    issue_types: ["custom_rule_violation", "custom_regex", "formula_rule_violation"],
  },
};

export default function ManualReviewPanel({
  items,
  darkMode = false,
  busy = false,
  onApply,
}: ManualReviewPanelProps) {
  const pending = useMemo(
    () => items.filter((m) => (m.status || 'pending') === 'pending'),
    [items]
  );

  const [userPicks, setUserPicks] = useState<Record<string, string>>({});
  const [expandedCards, setExpandedCards] = useState<Record<string, boolean>>({});

  // Group pending items into policy cards
  const policyCards = useMemo(() => {
    const cards: Array<{
      id: string;
      label: string;
      description: string;
      items: ManualReviewItem[];
      options: ResolutionOption[];
    }> = [];

    const categorized = new Set<string>();

    Object.entries(POLICY_GROUPS).forEach(([groupId, defn]) => {
      const groupItems = pending.filter(item => {
        const it = item.issue_type || '';
        const col = (item.column || '').toLowerCase();

        // Special case for key nulls vs normal high null pct
        if (it === 'high_null_percentage' && groupId === 'POLICY_SUSPECTED_KEY_NULL') {
          return col.includes('id') || col.includes('key') || col.includes('code') || col.includes('ref') || col.includes('num');
        }
        if (it === 'high_null_percentage' && groupId !== 'POLICY_SUSPECTED_KEY_NULL') {
          return false;
        }

        return defn.issue_types.includes(it);
      });

      if (groupItems.length > 0) {
        // Collect all unique options
        const optionsMap = new Map<string, ResolutionOption>();
        groupItems.forEach(item => {
          (item.resolution_options || []).forEach(opt => {
            optionsMap.set(opt.id, opt);
          });
        });
        const options = Array.from(optionsMap.values());

        cards.push({
          id: groupId,
          label: defn.label,
          description: defn.description,
          items: groupItems,
          options,
        });
        groupItems.forEach(item => categorized.add(item.id));
      }
    });

    // Uncategorized items get individual cards
    pending.forEach(item => {
      if (!categorized.has(item.id)) {
        cards.push({
          id: item.id,
          label: `Individual Item: ${(item.issue_type || 'General').replace(/_/g, ' ')}`,
          description: item.message || 'Manual review required.',
          items: [item],
          options: item.resolution_options || [],
        });
      }
    });

    return cards;
  }, [pending]);

  // Set initial picks from default resolutions
  useEffect(() => {
    const initialPicks: Record<string, string> = {};
    pending.forEach(m => {
      const enabledOpts = (m.resolution_options || []).filter(o => !o.disabled);
      const defaultId =
        m.default_resolution ||
        enabledOpts.find(o => o.recommended)?.id ||
        enabledOpts[0]?.id ||
        m.resolution_options?.[0]?.id ||
        'keep_as_is';
      initialPicks[m.id] = defaultId;
    });
    setUserPicks(prev => ({ ...initialPicks, ...prev }));
  }, [pending]);

  const getPickForCard = useCallback(
    (cardId: string, options: ResolutionOption[]) => {
      // Find the card in policyCards to inspect its items
      const card = policyCards.find(c => c.id === cardId);
      if (!card) return 'keep_as_is';
      
      // If all items in this card share the same resolution, return it
      const firstItemId = card.items[0]?.id;
      const firstPick = userPicks[firstItemId];
      
      const allSame = card.items.every(item => userPicks[item.id] === firstPick);
      if (allSame && firstPick) {
        return firstPick;
      }
      
      // Otherwise, return first recommended or first option
      return options.find(o => o.recommended)?.id || options[0]?.id || 'keep_as_is';
    },
    [policyCards, userPicks]
  );

  const setPickForCard = useCallback(
    (cardId: string, resolutionId: string) => {
      const card = policyCards.find(c => c.id === cardId);
      if (!card) return;
      
      const updates: Record<string, string> = {};
      card.items.forEach(item => {
        updates[item.id] = resolutionId;
      });
      setUserPicks(prev => ({ ...prev, ...updates }));
    },
    [policyCards]
  );

  const toggleExpand = useCallback((cardId: string) => {
    setExpandedCards(prev => ({ ...prev, [cardId]: !prev[cardId] }));
  }, []);

  const applyAll = useCallback(async () => {
    const resolutions = pending.map(m => ({
      item_id: m.id,
      resolution_id: userPicks[m.id] || m.default_resolution || 'keep_as_is',
    }));
    await onApply(resolutions);
  }, [pending, userPicks, onApply]);

  if (!pending.length) return null;

  const border = darkMode ? 'border-amber-400/35 bg-amber-500/10' : 'border-amber-300 bg-amber-50';
  const text = darkMode ? 'text-zinc-100' : 'text-zinc-900';
  const sub = darkMode ? 'text-zinc-300' : 'text-zinc-600';
  const cardBg = darkMode ? 'border-white/10 bg-black/20' : 'border-black/10 bg-white';

  return (
    <motion.div
      initial={{ opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      className={`rounded-xl border p-4 ${border}`}
    >
      <motion.div className="mb-3 flex flex-wrap items-center gap-2">
        <span className={`text-[11px] font-black uppercase tracking-widest flex items-center gap-1.5 ${sub}`}>
          <FaShieldAlt className="text-amber-500" /> Manual review — choose policy resolutions
        </span>
        <span
          className={`rounded-full px-2 py-0.5 text-[10px] font-bold ${
            darkMode ? 'bg-amber-500/30 text-amber-100' : 'bg-amber-200 text-amber-950'
          }`}
        >
          {policyCards.length} Policy Card{policyCards.length === 1 ? '' : 's'} ({pending.length} issue{pending.length === 1 ? '' : 's'}) pending
        </span>
      </motion.div>
      <p className={`mb-4 text-xs leading-relaxed ${sub}`}>
        Dhara grouped {pending.length} issues into policy decisions. Choose a default action below for each group, or expand a group to customize resolutions per column.
      </p>

      <ul className="space-y-4">
        {policyCards.map(card => {
          const isExpanded = !!expandedCards[card.id];
          const isPolicyGroup = card.items.length > 1;
          const currentPick = getPickForCard(card.id, card.options);

          return (
            <li key={card.id} className={`rounded-xl border p-4 ${cardBg}`}>
              <div className="flex justify-between items-start mb-2">
                <div>
                  <h3 className={`text-sm font-bold ${text}`}>{card.label}</h3>
                  <p className={`mt-0.5 text-xs ${sub}`}>{card.description}</p>
                </div>
                {isPolicyGroup && (
                  <button
                    type="button"
                    onClick={() => toggleExpand(card.id)}
                    className={`flex items-center gap-1 text-[11px] font-bold uppercase tracking-wider px-2 py-1 rounded transition hover:bg-black/5 ${sub}`}
                  >
                    <FaSlidersH /> {isExpanded ? 'Collapse' : 'Customize'}
                  </button>
                )}
              </div>

              {!isExpanded ? (
                // Group-level resolution picker
                <div className="mt-3 space-y-2">
                  {card.options.map(opt => {
                    const checked = currentPick === opt.id;
                    return (
                      <label
                        key={opt.id}
                        className={`flex cursor-pointer items-start gap-2 rounded-lg border px-3 py-2 text-xs transition ${
                          checked
                            ? darkMode
                              ? 'border-emerald-400/50 bg-emerald-500/15'
                              : 'border-emerald-400 bg-emerald-50'
                            : darkMode
                              ? 'border-white/10 hover:bg-white/5'
                              : 'border-black/5 hover:bg-black/[0.02]'
                        }`}
                      >
                        <input
                          type="radio"
                          name={`policy-${card.id}`}
                          checked={checked}
                          onChange={() => setPickForCard(card.id, opt.id)}
                          className="mt-0.5"
                        />
                        <span className={text}>
                          <span className="font-semibold">{opt.label}</span>
                          {opt.recommended && (
                            <span className="ml-1 text-[10px] font-bold text-emerald-600">recommended</span>
                          )}
                          {opt.description && (
                            <span className={`mt-0.5 block font-normal ${sub}`}>{opt.description}</span>
                          )}
                        </span>
                      </label>
                    );
                  })}
                </div>
              ) : (
                // Expanded per-item customizer
                <motion.div
                  initial={{ height: 0, opacity: 0 }}
                  animate={{ height: 'auto', opacity: 1 }}
                  className="mt-3 border-t pt-3 space-y-4"
                >
                  <span className={`text-[10px] font-bold uppercase tracking-widest ${sub}`}>
                    Customizing resolutions for {card.items.length} columns:
                  </span>
                  <ul className="space-y-3 pl-2">
                    {card.items.map(m => (
                      <li key={m.id} className="border-l-2 pl-3 py-1 space-y-2">
                        <div className={`text-xs font-semibold ${text}`}>
                          <span className="font-mono">{m.dataset || '—'}</span>
                          {m.column && (
                            <>
                              <span className="mx-1 opacity-40">·</span>
                              <span className="font-mono text-amber-600">{m.column}</span>
                            </>
                          )}
                          <span className={`ml-2 rounded px-1 py-0.5 text-[9px] uppercase ${darkMode ? 'bg-white/10' : 'bg-black/5'}`}>
                            {(m.issue_type || '').replace(/_/g, ' ')}
                          </span>
                        </div>
                        {m.message && <p className={`text-[11px] ${sub}`}>{m.message}</p>}
                        
                        <div className="space-y-1.5 mt-2">
                          {(m.resolution_options || []).map(opt => {
                            const checked = userPicks[m.id] === opt.id;
                            const isDisabled = opt.disabled === true;
                            return (
                              <label
                                key={opt.id}
                                title={isDisabled ? opt.disabled_reason : undefined}
                                className={`flex items-start gap-2 rounded-md border px-2.5 py-1.5 text-xs transition ${
                                  isDisabled
                                    ? darkMode
                                      ? 'cursor-not-allowed border-white/5 opacity-40'
                                      : 'cursor-not-allowed border-black/5 opacity-40'
                                    : checked
                                      ? darkMode
                                        ? 'cursor-pointer border-emerald-400/50 bg-emerald-500/15'
                                        : 'cursor-pointer border-emerald-400 bg-emerald-50'
                                      : darkMode
                                        ? 'cursor-pointer border-white/10 hover:bg-white/5'
                                        : 'cursor-pointer border-black/5 hover:bg-black/[0.02]'
                                }`}
                              >
                                <input
                                  type="radio"
                                  name={`item-${m.id}`}
                                  checked={checked}
                                  disabled={isDisabled}
                                  onChange={() => !isDisabled && setUserPicks(prev => ({ ...prev, [m.id]: opt.id }))}
                                  className="mt-0.5"
                                />
                                <span className={text}>
                                  <span className="font-medium">{opt.label}</span>
                                  {opt.recommended && !isDisabled && (
                                    <span className="ml-1 text-[9px] font-bold text-emerald-600">recommended</span>
                                  )}
                                  {isDisabled && (
                                    <span className="ml-1 text-[9px] font-semibold text-amber-500/80">Phase 2 only</span>
                                  )}
                                </span>
                              </label>
                            );
                          })}
                        </div>
                      </li>
                    ))}
                  </ul>
                </motion.div>
              )}
            </li>
          );
        })}
      </ul>

      <button
        type="button"
        disabled={busy || pending.length === 0}
        onClick={() => void applyAll()}
        className="mt-4 inline-flex items-center gap-2 rounded-xl bg-amber-600 px-4 py-2.5 text-sm font-bold text-white shadow hover:bg-amber-700 disabled:opacity-50"
      >
        Apply resolutions to {pending.length} issue{pending.length === 1 ? '' : 's'}
      </button>
    </motion.div>
  );
}
