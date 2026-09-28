import os

PANEL_PATH = r"C:\Users\ssakesh\OneDrive - Capgemini\Documents\Agent-Dhara---Frontend-App\components\EtlGenerationPanel.tsx"

with open(PANEL_PATH, "r", encoding="utf-8") as f:
    code = f.read()

target = "            {planValidationErrors.length > 0 ? ("
replacement = """            {recoveryContract && (
              <PlanRecoveryWorkspace
                recovery={recoveryContract}
                darkMode={dm}
                busy={busy}
                onApplyCorrections={applyManualResolutions}
                onAutoFixAll={async () => {
                  const fixes = (recoveryContract.issues || [])
                    .filter(i => !!i.item_id && !!i.recommended_action)
                    .map(i => ({ item_id: i.item_id as string, resolution_id: i.recommended_action as string }));
                  await applyManualResolutions(fixes);
                }}
                onEditAll={() => setRecoveryContract(null)}
              />
            )}
            {planValidationErrors.length > 0 && !recoveryContract ? ("""

if target in code and "recoveryContract &&" not in code:
    code = code.replace(target, replacement, 1)
    with open(PANEL_PATH, "w", encoding="utf-8") as f:
        f.write(code)
    print("PlanRecoveryWorkspace rendered successfully in EtlGenerationPanel.tsx")
else:
    print("Target already updated or not found")
