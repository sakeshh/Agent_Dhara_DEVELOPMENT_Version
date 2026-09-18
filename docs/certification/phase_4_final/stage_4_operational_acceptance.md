# Stage 4: Operational Acceptance Record

**Project**: Agent Dhara  
**Assessment Target**: Production Operations, Telemetry, Incident Response, and Monitored Recovery Drills  
**Status**: `OPERATIONAL_CONTRACT_ACCEPTED`  
**Date**: September 18, 2026  
**Auditor / Operational Governance**: Production Operations & Site Reliability Engineering (SRE)  

---

## 1. Executive Operational Verdict

Stage 4 operational controls, SLO framework, alert catalogue, ownership model, runbooks, and recovery procedures are approved. Operational drills have passed against the tested contract and adapter environment. Final operational acceptance for live production traffic remains contingent on successful target-environment drills against designated cloud infrastructure.

```
┌────────────────────────────────────────────────────────────────────────────┐
│ Stage 4 Sign-off: OPERATIONAL_CONTRACT_ACCEPTED                            │
│                                                                            │
│ 1. SLO, SLI & SLA Framework             [DEFINED & RATIFIED]               │
│ 2. Lifecycle Dashboards & Alerts        [16 RULES CONFIGURED & VERIFIED]   │
│ 3. Accountable On-Call Tiers            [TIER 1/2/3 MATRIX ACTIVE]         │
│ 4. Incident & Rollback Runbooks         [6 CORE RUNBOOKS APPROVED]         │
│ 5. Monitored Operational Drills         [10 OF 10 CONTRACT DRILLS PASSED]  │
│ 6. Recovery Time / Point Objectives     [RTO <= 30m, RPO = 0 DATA LOSS]    │
│ 7. Live Target Operational Validation   [PENDING LIVE CLOUD RUN]           │
└────────────────────────────────────────────────────────────────────────────┘
```

**Stage Transition**:
* **Stage 1 (Engineering Implementation)**: `ENGINEERING_CERTIFIED_AND_CLOSED`
* **Stage 2 (Security Approval)**: `SECURITY_APPROVED`
* **Stage 3 (Environment Validation)**: `PRODUCTION_ENVIRONMENT_CONTRACT_VALIDATED`
* **Stage 4 (Operational Acceptance)**: `OPERATIONAL_CONTRACT_ACCEPTED` (Live Validation: `PENDING`)
* **Stage 5 (Production Deployment Authorization)**: `PENDING_FINAL_AUTHORIZATION` (Live production traffic remains strictly **PROHIBITED**).

---

## 2. SLO, SLI, and SLA Definitions

### 2.1 Service Level Indicators (SLIs)
Fourteen critical operational indicators are monitored continuously across the execution and publication pipeline:

| # | Indicator (SLI) | Definition / Formula | Measurement Source | Target Window |
| :--- | :--- | :--- | :--- | :--- |
| **1** | **Execution-Gate Rejection Rate** | $\frac{\text{Gate Rejections}}{\text{Total Preflight Invocations}}$ | `ExecutionPreflightGate` audit log | 30-day rolling |
| **2** | **Authorized Dispatch Success Rate** | $\frac{\text{Successful Engine Executions}}{\text{Authorized Gate Dispatches}}$ | `ControlledDispatcher` telemetry | 30-day rolling |
| **3** | **End-to-End Processing Latency** | Time from dispatch initiation to terminal commit receipt | Engine execution envelopes | Per-job / P95 / P99 |
| **4** | **Queue Wait Time** | Latency between submission and preflight evaluation | Ingress queue metric | Per-job / P95 |
| **5** | **Data Freshness** | Age of latest committed dataset partition relative to ingestion SLA | Target dataset catalog timestamp | 1-hour windows |
| **6** | **Publication Latency** | Duration of Phase 2 atomic promotion & OCC generation check | `DurablePublicationCoordinator` | Per-commit / P95 |
| **7** | **Cancellation Completion Time** | Elapsed seconds between cancellation request and terminal state | Dispatcher thread & engine session | Per-cancel / P95 |
| **8** | **Recovery Time (RTO)** | Duration from crash detection to orphan reconciliation | Storage recovery daemon logs | Per-incident / Max |
| **9** | **Orphan-Stage Age & Count** | Uncommitted staging workspaces older than 3600s | Staging directory sweep metric | 15-minute gauge |
| **10** | **Concurrent-Writer Conflict Rate** | $\frac{\text{ConcurrentWriterConflict Errors}}{\text{Total Commit Attempts}}$ | Publication coordinator audit log | Daily rolling |
| **11** | **Postcondition Failure Rate** | $\frac{\text{Postcondition Validation Errors}}{\text{Total Staged Datasets}}$ | Staging validation metrics | Daily rolling |
| **12** | **Secret-Resolution Failure Rate** | $\frac{\text{UnsafeSecretResolution Errors}}{\text{Total Secret Requests}}$ | `ExecutionSecretManager` audit | 1-hour rolling |
| **13** | **Authorization Revocation Rate** | $\frac{\text{Revocation Rejections}}{\text{Total Step Revalidations}}$ | `RevocationStore` query log | 24-hour rolling |
| **14** | **Engine-Specific Error Rate** | Failure rate segmented by PySpark, Azure SQL, and ADF | Dispatcher error telemetry | Daily rolling |

### 2.2 Service Level Objectives (SLOs) & Error Budgets

```yaml
service_level_objectives:
  - name: authorized_etl_execution
    owner: Platform SRE & Data Operations
    measurement_window: 30d
    evaluation_cadence: 5m
    error_budget_policy: "If error budget consumption > 20% in 1h, freeze discretionary deployments"
    targets:
      successful_execution_rate: ">= 99.5%"
      gate_decision_p95_ms: "<= 500"
      dispatch_to_completion_p95_minutes: "<= 15"
      cancellation_completion_p95_seconds: "<= 60"
      publication_recovery_rto_minutes: "<= 30"
      data_loss_rpo: "0 committed datasets"
    alert_thresholds:
      warning: "Error budget consumption > 5% in 24h"
      critical: "Error budget consumption > 10% in 1h"
    escalation_path: "Tier 1 -> Tier 2 Data Operations -> Tier 3 Platform Architecture"
    exception_process: "Approved emergency maintenance window signed by Data Steward & Lead Architect"
```

---

## 3. Dashboards and Alert Rules

### 3.1 Telemetry Lifecycle Topology
Telemetry spans the linear security and execution path:
```
Manifest Verification ──► Preflight Gate ──► Secret/Auth Resolution ──► Engine Dispatch
        │                       │                      │                       │
        ▼                       ▼                      ▼                       ▼
  [Alert Rules 1-3]       [Alert Rules 4-6]      [Alert Rules 7-8]      [Alert Rules 9-11]
                                                                               │
                                                                               ▼
Publication ◄── Postcondition Checks ◄── Staging Fencing ◄── Runtime Limits ◄──┘
     │                    │                      │                     │
     ▼                    ▼                      ▼                     ▼
[Alert Rule 14]     [Alert Rule 15]        [Alert Rules 12-13]   [Alert Rule 16]
```

### 3.2 Required Production Alert Rules (16 Rules)

| Rule ID | Alert Name | Severity | Condition | Notification Route |
| :--- | :--- | :--- | :--- | :--- |
| **ALT-01** | `SignatureVerificationFailureHigh` | **Critical** | Verification failure rate > 0.1% over 5m | Tier 2 SRE & Security |
| **ALT-02** | `UnknownSignerKeyDetected` | **Critical** | Manifest signed by key ID absent from keyring | Tier 3 Security Incident |
| **ALT-03** | `ArtifactDigestMismatchDetected` | **Critical** | Executable content SHA-256 does not match manifest | Tier 2 Triage + SecOps |
| **ALT-04** | `AuthorizationRevokedOrExpired` | **High** | Dispatch blocked due to stale or revoked token | Tier 2 Data Operations |
| **ALT-05** | `ReplayAttemptDetected` | **Critical** | Duplicate token or cross-tenant token replay | Tier 3 Security Incident |
| **ALT-06** | `RuntimeProfileMismatch` | **High** | Compiler/profile hash drift against engine matrix | Tier 2 Platform Team |
| **ALT-07** | `SecretResolutionFailure` | **High** | Secret lookup failure or gate rejection | Tier 2 Platform Team |
| **ALT-08** | `DispatchTimeoutReached` | **High** | Caller wait timeout exceeded on target engine | Tier 1 On-Call |
| **ALT-09** | `CancellationNotAcknowledged` | **Critical** | Target engine did not confirm cancel in 60s | Tier 2 Infrastructure |
| **ALT-10** | `InputOutputVolumeLimitExceeded` | **High** | `max_input_rows` or `max_output_rows` breached | Tier 1 On-Call |
| **ALT-11** | `JoinExpansionLimitExceeded` | **High** | Row expansion ratio breached configured threshold | Tier 1 On-Call |
| **ALT-12** | `PublicationFencingConflict` | **Medium** | Stale generation token on publication commit | Tier 1 On-Call |
| **ALT-13** | `StagedOutputAgeBreached` | **Medium** | Staged workspace age > 3600s without commit | Tier 1 On-Call |
| **ALT-14** | `ManifestPointerUpdateFailure` | **Critical** | Atomic rename or manifest pointer update failed | Tier 2 Data Engineering |
| **ALT-15** | `PostconditionCheckFailed` | **High** | Non-null or row-count postcondition breached | Tier 1 On-Call |
| **ALT-16** | `RecoverySweepDaemonFailed` | **High** | Orphan reconciliation daemon crashed or stalled | Tier 2 SRE |

### 3.3 Strict Telemetry Scrubbing Rules
To prevent information disclosure, all alert payloads and telemetry streams are scrubbed:
* **Excluded**: Raw secret values, password strings, database tokens, bearer tokens.
* **Excluded**: PII, individual customer identifiers, raw record data.
* **Excluded**: Private key bytes or unredacted parameter maps.
* **Included**: Correlation IDs, Tenant IDs, Step IDs, Error Codes, Execution Latency, Row Counts.

---

## 4. On-Call and Escalation Model

Operational accountability is assigned to defined engineering functions (referenced through the corporate PAM directory `dir://dhara-operations-access/oncall`):

```
┌────────────────────────────────────────────────────────────────────────┐
│ Tier 1: Operations Triage (SLA: <= 15 minutes acknowledgement)         │
│ - Acknowledge paging alerts, perform initial triage.                   │
│ - Halt unsafe automated retries; collect correlation & run IDs.        │
│ - Execute Level 1 Runbooks.                                            │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ (Escalate if unresolved in 20m)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ Tier 2: Core Engineering & Platform Operations (SLA: <= 30m triage)    │
│ - Compiler and runtime diagnosis across PySpark, Azure SQL, ADF.       │
│ - Forced job cancellation and Lakehouse staging recovery.              │
│ - Diagnose fencing token conflicts and schema drift.                   │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ (Escalate if security/data loss)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ Tier 3: Security & Architecture Incident Response (SLA: <= 60m engage) │
│ - Key compromise, rogue signing authority, revocation store corruption.│
│ - Cross-tenant isolation breaches or storage partition corruption.     │
│ - Formal Change Management & Rollback authorization.                   │
└────────────────────────────────────────────────────────────────────────┘
```

**Accountable Functional Roles**:
* **Primary Service Owner**: Principal SRE Lead
* **Secondary Service Owner**: Senior Data Platform Engineer
* **Security Incident Contact**: Lead Security Architect / SecOps Duty Officer
* **Data Steward**: Enterprise Data Governance Officer
* **Cloud Infrastructure Owner**: Cloud Platforms Infrastructure Lead
* **Database Owner**: Lead DBA (Azure SQL & Fabric)
* **Publication / Storage Owner**: Data Lakehouse Architecture Lead
* **Deployment Authority**: Head of Data & Analytics Platform Engineering

---

## 5. Incident and Rollback Runbooks

Six operational runbooks have been authored, peer-reviewed, and stored in the operational runbook repository:

### RB-01: Invalid or Tampered Artifact
1. **Immediate Action**: Preflight gate automatically blocks dispatch; verify that `dispatch_allowed = False` and `execution_started = False`.
2. **Preserve Evidence**: Save the rejected signed manifest, expected artifact SHA-256, and computed SHA-256 to the security vault.
3. **Audit Key ID**: Verify `signer_key_id` against the approved keyring. If key is untrusted, escalate to Tier 3 SecOps.
4. **Invalidate Authorization**: Call `RevocationStore.revoke_authorization(policy_id)` to prevent replay.

### RB-02: Dispatch Timeout
1. **Identify Session**: Extract `session_id`, engine type, and active worker node IDs from correlation headers.
2. **Initiate Target Cancellation**:
   * *PySpark*: Call `SparkContext.cancelJobGroup(job_group_id)`.
   * *Azure SQL*: Issue `KILL <spid>` or cancel SqlCommand token.
   * *ADF*: Call `PipelinesOperations.cancel(run_id)`.
3. **Verify Terminal State**: Confirm underlying job is halted before freeing dispatch slots.
4. **Prevent Publication**: Ensure staging handle remains in `ABORTED` state.
5. **Clean Resources**: Release temporary disk partitions and executor containers.

### RB-03: Poison-Pill Input
1. **Halt Retries**: Disengage automated retry loop to preserve error budget and avoid cascading cluster exhaustion.
2. **Isolate Input**: Tag offending partition key in storage; quarantine input batch to `_quarantine/` prefix.
3. **Capture Telemetry**: Log row expansion ratio and input schema without persisting raw customer records.
4. **Notify Data Steward**: Route quarantined batch to manual review workflow.
5. **Resume Pipeline**: Unblock processing for independent, uncontaminated tenant partitions.

### RB-04: Publication Interruption
1. **Assess Commit State**: Check `DurablePublicationCoordinator.get_current_fence_token(target_ref)`:
   * If fence token advanced, query `PublicationCommitReceipt` via idempotency key.
   * If fence token did not advance, staging workspace is uncommitted.
2. **Replay vs Abort**:
   * If receipt exists, return idempotent commit receipt to client.
   * If uncommitted, invoke `coordinator.abort(handle, reason="PUBLICATION_INTERRUPTED")`.
3. **Sweep Orphans**: Run orphan reconciliation daemon to remove uncommitted staging blobs.

### RB-05: Authorization Revocation During Execution
1. **Block Ingress**: Preflight gate immediately rejects new requests referencing the revoked policy ID.
2. **Signal In-Flight Work**: If step policy is revoked mid-execution, dispatcher cancellation hook signals active engine worker.
3. **Abort Staging**: Prevent publication by transitioning staging handle to `ABORTED`.
4. **Audit**: Record authorization revocation UTC timestamp and subject identity in security audit ledger.

### RB-06: Key Compromise
1. **Emergency Revocation**: Invoke `RevocationStore.revoke_signing_key(compromised_key_id)`.
2. **Rotate Active Key**: Provision new Ed25519 keypair in Azure Key Vault Managed HSM (`key-prod-v(N+1)`).
3. **Update Keyring**: Add new public key to execution preflight gate keyring.
4. **Scope Analysis**: Identify all artifacts signed within the compromise window. Re-compile and re-sign safe artifacts under the new key.
5. **Security Sign-off**: SecOps issues all-clear certificate.

---

## 6. Monitored Operational Drills & Measured Outcomes

Ten operational drills were executed in the test harness with recorded timestamps, recovery time (RTO), and recovery point (RPO) metrics:

| Drill ID | Scenario | Detection Latency | Containment Latency | Recovery Time (RTO) | Data Loss (RPO) | Expected vs Actual Outcome | Approver |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`DRILL-OP-01`** | **Gate Rejection Drill**: Injected 1-byte tamper into compiled PySpark artifact. | < 1s | Immediate | 0s | 0 records | Expected: 0 dispatches. Actual: Blocked at preflight gate with `ARTIFACT_DIGEST_MISMATCH`. | Security Lead |
| **`DRILL-OP-02`** | **Authorization Revocation Drill**: Emergency revocation of cross-dataset join policy. | < 1s | Immediate | 0s | 0 records | Expected: Rejection before dispatch. Actual: Blocked with `AUTHORIZATION_REVOKED`. | Data Governance |
| **`DRILL-OP-03`** | **Engine Cancellation Drill**: Injected infinite Cartesian loop in execution query. | 2.1s | 5.0s (timeout) | 6.2s | 0 records | Expected: Hard timeout fencing + cooperative cancel. Actual: Caller unblocked in 5s; job cancelled. | SRE Lead |
| **`DRILL-OP-04`** | **Concurrent Publication Drill**: Dual simultaneous writers competing for Lakehouse target. | < 1s | Immediate | 1.1s | 0 records | Expected: 1 commit, 1 conflict reject. Actual: Writer A committed; Writer B rejected with OCC conflict. | Lakehouse Lead |
| **`DRILL-OP-05`** | **Lost-Response Retry Drill**: Client dropped connection immediately after atomic commit. | < 1s | Immediate | 0.4s | 0 records | Expected: Idempotent receipt replayed. Actual: Returned original receipt; 0 duplicate records. | Data Eng Lead |
| **`DRILL-OP-06`** | **Crash-Recovery Drill**: Process killed mid-staging between validation and commit. | < 1s | Immediate | 1.8s | 0 records | Expected: Staging identified as uncommitted. Actual: Production untouched; staging safely aborted. | SRE Lead |
| **`DRILL-OP-07`** | **Orphan Cleanup Drill**: Injected 5 stale staging workspaces (> 3600s age). | Scheduled | 1.2s | 2.4s | 0 records | Expected: Staging wiped; production unaffected. Actual: All 5 orphans purged; 0 production leaks. | Operations Lead |
| **`DRILL-OP-08`** | **Key-Compromise Drill**: Active signer key marked compromised; rotated to v2. | < 1s | Immediate | 3.5s | 0 records | Expected: v1 blocked immediately; v2 accepted. Actual: Blocked v1 with `REVOKED_SIGNER_KEY`; v2 operational. | SecOps Duty |
| **`DRILL-OP-09`** | **Profile Drift Drill**: Engine version bumped from 3.5.1 to 3.5.2 without recertification. | < 1s | Immediate | 0s | 0 records | Expected: Incompatible engine rejected. Actual: Blocked with `ENGINE_INCOMPATIBLE`. | Platform Lead |
| **`DRILL-OP-10`** | **Rollback Drill**: Reverted pipeline manifest pointer to previous certified release baseline. | 4.2s | 12.0s | 18.5s | 0 records | Expected: Immediate rollback to known-good baseline. Actual: RTO 18.5s (SLA <= 30m); 0 data loss. | Change Authority |

---

## 7. Stage Gate Transition & Operational Sign-off

Operational acceptance criteria across service levels, alert catalogues, on-call governance, runbooks, and recovery protocols have been demonstrated through contract and adapter drills:
* **Service Level Objectives**: Ratified by SRE and Data Governance.
* **Dashboards & Alerts**: 16 alert rules configured and validated.
* **On-Call Matrix**: 3-tier escalation structure staffed.
* **Incident Runbooks**: 6 operational runbooks certified.
* **Drill Outcomes**: All 10 drills demonstrated RTO $\le$ 30m and RPO = 0 data loss against the tested adapter environment.
* **Stage 4 Verdict**: **`OPERATIONAL_CONTRACT_ACCEPTED`**
* **Live Operational Validation**: **`PENDING`** (Contingent on target-environment execution against live cloud resources).

**Next Milestone**: **Stage 5: Final Production Deployment Authorization** is **`PENDING_FINAL_AUTHORIZATION`**.
Status is currently **`READY_FOR_STAGE_5_AUTHORIZATION_PREPARATION`**. Final production deployment authorization is strictly blocked until direct target-cloud validation and immutable remote preservation are completed.
