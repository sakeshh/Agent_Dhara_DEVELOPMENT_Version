"""
Patch ManualReviewPanel.tsx to:
1. Prevent assigning disabled options to protected columns in setPickForCard.
2. Render a persistent summary bar when all items are resolved rather than returning null.
"""
import os

PANEL_PATH = r"C:\Users\ssakesh\OneDrive - Capgemini\Documents\Agent-Dhara---Frontend-App\components\ManualReviewPanel.tsx"

with open(PANEL_PATH, "r", encoding="utf-8") as f:
    code = f.read()

# 1. Update setPickForCard
old_pick = """  const setPickForCard = useCallback(
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
  );"""

new_pick = """  const setPickForCard = useCallback(
    (cardId: string, resolutionId: string) => {
      const card = policyCards.find(c => c.id === cardId);
      if (!card) return;
      
      const updates: Record<string, string> = {};
      card.items.forEach(item => {
        const itemOpt = (item.resolution_options || []).find(o => o.id === resolutionId);
        // Pre-flight capability filter: do not force disabled options onto protected columns
        if (!itemOpt || !itemOpt.disabled) {
          updates[item.id] = resolutionId;
        }
      });
      setUserPicks(prev => ({ ...prev, ...updates }));
    },
    [policyCards]
  );"""

if old_pick in code:
    code = code.replace(old_pick, new_pick)
    print("Patched setPickForCard")
else:
    print("setPickForCard target not found or already patched")

# 2. Update persistent summary when pending is empty
old_empty = """  if (!pending.length) return null;"""

new_empty = """  const [isEditingResolved, setIsEditingResolved] = useState(false);

  if (!pending.length && !isEditingResolved) {
    return (
      <div className={`rounded-xl border p-3.5 mb-4 flex flex-wrap items-center justify-between gap-3 ${
        darkMode ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-200' : 'border-emerald-300 bg-emerald-50 text-emerald-900'
      }`}>
        <div className="flex items-center gap-2">
          <FaCheckCircle className="text-emerald-500 text-sm" />
          <span className="text-xs font-bold">
            All {items.length} manual review item(s) selected
          </span>
        </div>
        <button
          type="button"
          onClick={() => setIsEditingResolved(true)}
          className="text-xs font-bold underline hover:opacity-80 transition"
        >
          Review / Edit Choices
        </button>
      </div>
    );
  }"""

if old_empty in code:
    code = code.replace(old_empty, new_empty)
    print("Patched persistent review summary")
else:
    print("empty check target not found or already patched")

with open(PANEL_PATH, "w", encoding="utf-8") as f:
    f.write(code)
print("Saved ManualReviewPanel.tsx")
