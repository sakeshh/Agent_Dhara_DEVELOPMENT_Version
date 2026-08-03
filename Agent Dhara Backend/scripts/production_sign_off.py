#!/usr/bin/env python3
"""
Agent Dhara Production Sign-off Gate (RC-5)
===========================================
Programmatic certification utility that validates the system's correctness,
output artifact completeness, schema integrity, and automated test outcomes.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

# Path Configuration
BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))
REPORTS_DIR = BASE_DIR / "output" / "reports_test"
REPORT_JSON = REPORTS_DIR / "report.json"
MANIFEST_JSON = REPORTS_DIR / "cleaning_manifest.json"
REPORT_HTML = REPORTS_DIR / "report.html"
REPORT_MD = REPORTS_DIR / "report.md"


# List of all test files to run for absolute verification
TEST_FILES = [
    "tests/test_r1_production_readiness.py",
    "tests/test_report_generation_v2.py",
    "tests/test_release3_observability_governance.py",
    "tests/test_harden_fabric_and_score.py",
    "tests/test_rc3_expanded_boundaries.py",
    "tests/test_remediation_and_metrics_fixes.py",
    "tests/verifier/independent_verifier.py"
]


def log_step(msg: str):
    print(f"\n[\033[1;34mINFO\033[0m] {msg}")

def log_success(msg: str):
    print(f"[\033[1;32mPASS\033[0m] {msg}")

def log_failure(msg: str):
    print(f"[\033[1;31mFAIL\033[0m] {msg}", file=sys.stderr)

def check_file_exists(p: Path) -> bool:
    if not p.exists():
        log_failure(f"Required artifact does not exist: {p.name} at {p}")
        return False
    if p.stat().st_size == 0:
        log_failure(f"Required artifact is empty: {p.name} at {p}")
        return False
    log_success(f"Artifact exists and is non-empty: {p.name} ({p.stat().st_size} bytes)")
    return True

def main() -> int:
    print("=" * 70)
    print("      AGENT DHARA - PRODUCTION SIGN-OFF GATING SYSTEM      ")
    print("=" * 70)

    # ----------------------------------------------------
    # Step 1: Automated Test Suite Gating
    # ----------------------------------------------------
    log_step("Executing automated test suite and independent verifiers...")
    cmd = [sys.executable, "-m", "pytest"] + TEST_FILES + ["-v", "--tb=short"]
    try:
        res = subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True)
        if res.returncode == 0:
            log_success("All automated unit/integration tests and verifiers passed!")
        else:
            log_failure("Automated test suite execution encountered failures:")
            print(res.stdout)
            print(res.stderr, file=sys.stderr)
            return 1
    except Exception as e:
        log_failure(f"Failed to execute test suite process: {e}")
        return 1

    # ----------------------------------------------------
    # Step 2: Artifact Existence Validation
    # ----------------------------------------------------
    log_step("Verifying presence and completeness of generated report artifacts...")
    artifacts = [REPORT_JSON, MANIFEST_JSON, REPORT_HTML, REPORT_MD]
    for art in artifacts:
        if not check_file_exists(art):
            return 1

    # ----------------------------------------------------
    # Step 3: Schema & Structural Verification
    # ----------------------------------------------------
    log_step("Validating JSON schemas and governance fields...")
    
    # 1. Parse report.json
    try:
        with open(REPORT_JSON, encoding="utf-8") as f:
            rep_data = json.load(f)
        log_success("report.json is a syntactically valid JSON document.")
    except Exception as e:
        log_failure(f"Failed to parse report.json: {e}")
        return 1

    # 2. Check metadata structures
    req_top_keys = ["datasets", "relationships", "data_quality_issues", "governance"]
    missing_keys = [k for k in req_top_keys if k not in rep_data]
    if missing_keys:
        log_failure(f"report.json is missing mandatory top-level sections: {missing_keys}")
        return 1
    log_success("report.json contains all required schema sections.")

    # 3. Check readiness scorecard status
    # Read the summary or build the scorecard directly
    try:
        from agent.service_layer import build_dq_scorecard
        scorecard = build_dq_scorecard(rep_data)
        verdict = scorecard.get("verdict")
        score = scorecard.get("overall_dq_score")
        log_success(f"Aggregate production readiness verdict calculated: {verdict} (Score: {score})")
        
        # If the verdict is blocked due to low scores, we fail production release
        if verdict == "BLOCKED":
            log_failure(f"Production gate blocked! Quality verdict is BLOCKED. Readiness Score: {score}")
            return 1
        elif verdict == "UNKNOWN":
            log_failure("Production gate blocked! Score / readiness evaluation status is UNKNOWN.")
            return 1
        else:
            log_success(f"Production readiness gate is satisfied: {verdict}")
    except Exception as e:
        log_failure(f"Readiness check failed: {e}")
        return 1

    # 4. Parse cleaning_manifest.json
    try:
        with open(MANIFEST_JSON, encoding="utf-8") as f:
            man_data = json.load(f)
        log_success("cleaning_manifest.json is a syntactically valid JSON document.")
    except Exception as e:
        log_failure(f"Failed to parse cleaning_manifest.json: {e}")
        return 1

    req_manifest_keys = ["manifest_version", "generated_at", "verdict", "overall_dq_score", "datasets"]
    missing_man_keys = [k for k in req_manifest_keys if k not in man_data]
    if missing_man_keys:
        log_failure(f"cleaning_manifest.json is missing required keys: {missing_man_keys}")
        return 1
    log_success("cleaning_manifest.json schema matches the remediation plan contract.")

    # ----------------------------------------------------
    # Step 4: Cross-Artifact Metrics Verification
    # ----------------------------------------------------
    if not validate_cross_artifact_metrics():
        log_failure("Cross-artifact metrics verification failed! Artifact metrics are inconsistent.")
        return 1

    # ----------------------------------------------------
    # Certification Verdict
    # ----------------------------------------------------
    print("\n" + "=" * 70)
    print("            AGENT DHARA - CERTIFICATE OF COMPLIANCE            ")
    print("=" * 70)
    print(" This is to certify that Agent Dhara's data quality gating and ")
    print(" reporting layer has successfully passed all production-grade ")
    print(" regression checks, semantic validations, and security limits. ")
    print("\n Release Certification: PASSED")
    print(f" Quality Readiness Verdict: {verdict}")
    print(f" Authority Level: PRODUCTION READY")
    print("=" * 70 + "\n")
    return 0


def validate_cross_artifact_metrics() -> bool:
    log_step("Executing cross-artifact metrics validation (asserting consistency)...")
    import re
    
    # 1. Read JSON
    try:
        with open(REPORT_JSON, encoding="utf-8") as f:
            rep_data = json.load(f)
        from agent.service_layer import build_dq_scorecard
        scorecard = build_dq_scorecard(rep_data)
        json_score = float(scorecard.get("overall_dq_score", 0.0))
        json_verdict = str(scorecard.get("verdict", "")).strip()
        
        json_ds_scores = {}
        for ds, block in rep_data.get("data_quality_issues", {}).get("datasets", {}).items():
            s_val = block.get("summary", {}).get("dq_score")
            if s_val is not None:
                json_ds_scores[ds] = float(s_val)
    except Exception as e:
        log_failure(f"Failed parsing report.json for cross-validation: {e}")
        return False

    # 2. Read Manifest
    try:
        with open(MANIFEST_JSON, encoding="utf-8") as f:
            man_data = json.load(f)
        man_score = float(man_data.get("overall_dq_score", 0.0))
        man_verdict = str(man_data.get("verdict", "")).strip()
        man_ds_scores = {ds: float(info.get("dq_score", 0.0)) for ds, info in man_data.get("datasets", {}).items()}
    except Exception as e:
        log_failure(f"Failed parsing cleaning_manifest.json for cross-validation: {e}")
        return False

    # Compare JSON and Manifest directly
    if json_score != man_score:
        log_failure(f"Score mismatch: report.json ({json_score}) != cleaning_manifest.json ({man_score})")
        return False
    if json_verdict != man_verdict:
        log_failure(f"Verdict mismatch: report.json ({json_verdict}) != cleaning_manifest.json ({man_verdict})")
        return False
    for ds, val in json_ds_scores.items():
        if ds in man_ds_scores and man_ds_scores[ds] != val:
            log_failure(f"Dataset '{ds}' score mismatch: report.json ({val}) != cleaning_manifest.json ({man_ds_scores[ds]})")
            return False
    log_success("report.json metrics == cleaning_manifest.json metrics")

    # 3. Read Markdown
    try:
        def norm_v(v: str) -> str:
            return re.sub(r'[^A-Z_]', '', v.upper().replace(' ', '_').replace('-', '_'))

        md_text = open(REPORT_MD, encoding="utf-8").read()
        score_match = re.search(r"Overall DQ Score\s*\|\s*([\d\.]+)\s*/\s*100", md_text)
        verdict_match = re.search(r"Data Quality Verdict:\s*([A-Z_ \-]+)", md_text)
        if not score_match:
            log_failure("Could not parse overall score from report.md")
            return False
        if not verdict_match:
            log_failure("Could not parse verdict from report.md")
            return False
        md_score = float(score_match.group(1))
        md_verdict = norm_v(verdict_match.group(1))
        norm_json_verdict = norm_v(json_verdict)
        
        if md_score != json_score:
            log_failure(f"Score mismatch: report.json ({json_score}) != report.md ({md_score})")
            return False
        if md_verdict != norm_json_verdict:
            log_failure(f"Verdict mismatch: report.json ({norm_json_verdict}) != report.md ({md_verdict})")
            return False
            
        # Parse dataset scores from markdown
        for ds, val in json_ds_scores.items():
            pattern = rf"\|\s*`?{re.escape(ds)}`?\s*\|\s*([\d\.]+)\s*\|"
            ds_match = re.search(pattern, md_text)
            if ds_match:
                ds_score = float(ds_match.group(1))
                if ds_score != val:
                    log_failure(f"Dataset '{ds}' score mismatch: report.json ({val}) != report.md ({ds_score})")
                    return False
        log_success("report.md metrics == report.json metrics")
    except Exception as e:
        log_failure(f"Failed validating report.md metrics: {e}")
        return False

    # 4. Read HTML
    try:
        html_text = open(REPORT_HTML, encoding="utf-8").read()
        score_match = re.search(r"Overall DQ score:\s*<strong[^>]*>\s*([\d\.]+)\s*</strong>", html_text, re.IGNORECASE)
        verdict_match = re.search(r"Data quality verdict:\s*<strong[^>]*>\s*([A-Z_ \-]+)\s*</strong>", html_text, re.IGNORECASE)

        if not score_match:
            log_failure("Could not parse overall score from report.html")
            return False
        if not verdict_match:
            log_failure("Could not parse verdict from report.html")
            return False

        html_score = float(score_match.group(1))
        html_verdict = norm_v(verdict_match.group(1))
        norm_json_verdict = norm_v(json_verdict)

        if html_score != json_score:
            log_failure(f"Score mismatch: report.json ({json_score}) != report.html ({html_score})")
            return False
        if html_verdict != norm_json_verdict:
            log_failure(f"Verdict mismatch: report.json ({norm_json_verdict}) != report.html ({html_verdict})")
            return False
        log_success("report.html metrics == report.json metrics")
    except Exception as e:
        log_failure(f"Failed validating report.html metrics: {e}")
        return False
        
    return True


if __name__ == "__main__":
    sys.exit(main())
