# Stage 5: Production Deployment Authorization Package Specification

**Project**: Agent Dhara  
**Domain**: `DHARA_PRODUCTION_DEPLOYMENT_AUTHORIZATION_V1`  
**Status**: `PENDING_FINAL_AUTHORIZATION`  
**Current Readiness Posture**: `READY_FOR_STAGE_5_AUTHORIZATION_PREPARATION`  
**Live Production Traffic**: **STRICTLY PROHIBITED**  
**Date**: September 18, 2026  

---

## 1. Executive Status & Prerequisites

Stage 5 represents the final, immutable gating boundary required before live production traffic or live target execution can be authorized.

```
┌────────────────────────────────────────────────────────────────────────────┐
│ Stage 5 Authorization Status: PENDING_FINAL_AUTHORIZATION                  │
│                                                                            │
│ Current Posture: READY_FOR_STAGE_5_AUTHORIZATION_PREPARATION               │
│                                                                            │
│ Remaining Mandatory Preconditions:                                        │
│ 1. Direct Target-Cloud Observations in designated production environment.   │
│ 2. Live Target Publication Drills against real ADLS Gen2/Lakehouse storage.│
│ 3. Remote Git Push & Signed Release Tag to immutable repository.           │
│ 4. Formal Quad-Sign-off (Security, Operations, Data Owner, Deployment Auth)│
└────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Stage Progression Matrix

| Stage | Milestone | Certified Status | Remaining Action |
| :--- | :--- | :--- | :--- |
| **Stage 1** | Engineering Implementation | **`ENGINEERING_CERTIFIED_AND_CLOSED`** | None (451 passed) |
| **Stage 2** | Security Approval | **`SECURITY_APPROVED`** | Bind live Key Vault/HSM evidence |
| **Stage 3** | Environment Validation | **`PRODUCTION_ENVIRONMENT_CONTRACT_VALIDATED`** | Direct target observations on designated cloud |
| **Stage 4** | Operational Acceptance | **`OPERATIONAL_CONTRACT_ACCEPTED`** | Repeat 6 critical drills on real cloud resources |
| **Stage 5** | Production Authorization | **`PENDING_FINAL_AUTHORIZATION`** | Formal release sign-off after live cloud validation |

---

## 3. Production Deployment Authorization Schema (`DHARA_PRODUCTION_DEPLOYMENT_AUTHORIZATION_V1`)

Upon completion of live target validation and remote preservation, this canonical envelope binds the entire verified cryptographic chain:

```json
{
  "$schema": "https://dhara.governance.local/schemas/deployment_authorization_v1.json",
  "domain": "DHARA_PRODUCTION_DEPLOYMENT_AUTHORIZATION_V1",
  "authorization_id": "AUTH-DHARA-PROD-20260918-001",
  "authorization_status": "PENDING_FINAL_AUTHORIZATION",
  "approved_environment_id": "azure-prod-emea-fabric-01",
  "tenant_scope_hash": "5b058474e2c884b2e86f8e7aa2377c8e8609559c3d4f40f252cf1473b98406bc",
  "subscription_scope_hash": "9cfd22fa97950c4ca594aa1c9444161bb7473fa58a436be34260f8db9f91a50a",
  
  "git_revision_chain": {
    "implementation_commit": "0887e16f5615e95755e27558528e5db3121aa98c",
    "phase_4_certification_commit": "225ebfae926ba51147cc0553a20af1c450f92721",
    "security_approval_commit": "9c4a87eaa6cf635bd045f304c3b0541d46bebc00",
    "environment_validation_commit": "78c64e55afa19bf534b856d434ac8a4577dcc481",
    "operational_acceptance_commit": "6250f0316168a4f585625c12e142c2b3ccef9026",
    "release_tag": "agent-dhara-phase4-production-candidate-2026-09-18"
  },

  "cryptographic_digests": {
    "phase_3_capability_ledger_hash": "620023ee0a82ecaa17f300c14b2d58adce2a4658097d74c05e1a3bc89a71db3f",
    "phase_4_evidence_manifest_hash": "f34c7bd02cd80f497a2042724c116ce41d845a1c3485f307e42cfb54591a3149",
    "runtime_profile_hashes": {
      "pyspark": "0e5052fffae97341e98bbcfb705ba30a210ff4410e3fa29cd9d44e5458ff62ee",
      "azure_sql": "0b33fbce46b1bc309e4d58853b0fce07a27eb2d08fbbe789f5bc36e768393b3f",
      "pandas": "964d422a5fbef8fec560731f5fecf54dfdbe7e72b4c5025d506ab6222ea944f2",
      "adf": "a90df7128cb487f9d518d6e32bc009fb7e96b3064375b42d76f8a486ebefcc05"
    }
  },

  "engine_matrix": {
    "pyspark": "3.5.1",
    "azure_sql": "compat_160",
    "pandas": "2.2+",
    "adf_orchestration": "v2"
  },

  "compilers": {
    "pyspark_ir_compiler": "v1.0",
    "azure_sql_compiler": "v1.0",
    "pandas_ir_compiler": "v1.0",
    "adf_orchestration_adapter": "v1.0",
    "publication_adapter_version": "v1.0"
  },

  "key_custody": {
    "signing_key_id": "key-prod-ed25519-v1",
    "signing_key_version": "2026-09-v1",
    "key_vault_hsm_uri": "https://kv-dhara-prod-hsm.vault.azure.net/",
    "trusted_signer_registry_hash": "24eb4f944ffcb991aa2eb45a7bb918342469ebf4618e7e170889f816c7cf6d99"
  },

  "authorized_scope": {
    "approved_publication_scope": "certified_production",
    "approved_datasets": [
      "financial_facts",
      "customer_dimensions",
      "regulatory_reconciliation"
    ],
    "approved_engine_targets": ["pyspark", "azure_sql", "adf_orchestration"],
    "approved_release_window": {
      "not_before": "2026-09-20T00:00:00Z",
      "expires_at": "2026-10-20T00:00:00Z"
    }
  },

  "approving_authorities": {
    "security_approver": "PENDING_FINAL_SIGN_OFF",
    "operations_approver": "PENDING_FINAL_SIGN_OFF",
    "data_owner_approver": "PENDING_FINAL_SIGN_OFF",
    "deployment_authority": "PENDING_FINAL_SIGN_OFF"
  },

  "governance": {
    "issued_at": null,
    "revocation_reference": "https://kv-dhara-prod-hsm.vault.azure.net/secrets/revocation-ledger-v1",
    "signature": null
  }
}
```

---

## 4. Deployment Pipeline Rejection Invariants

The automated continuous delivery and deployment pipeline enforces the following deterministic rejection rules:

```
IF (authorization.signature IS NULL OR signature_invalid)        --> REJECT: UNSIGNED_AUTHORIZATION
IF (current_time < not_before OR current_time > expires_at)      --> REJECT: EXPIRED_OR_PREMATURE_AUTHORIZATION
IF (deployment.git_commit != authorization.implementation_commit) --> REJECT: COMMIT_HASH_MISMATCH
IF (target.environment_id != authorization.approved_environment) --> REJECT: ENVIRONMENT_MISMATCH
IF (target.runtime_hash != authorization.runtime_profile_hash)   --> REJECT: RUNTIME_PROFILE_DRIFT
IF (manifest.signing_key_id != authorization.signing_key_id)     --> REJECT: UNAPPROVED_SIGNER_KEY
IF (dataset NOT IN authorization.approved_datasets)              --> REJECT: UNAPPROVED_DATASET_SCOPE
IF (publication_scope != authorization.approved_scope)           --> REJECT: SCOPE_ESCALATION_BLOCKED
```

---

## 5. Pre-Stage-5 Action Plan (Live Target Cloud Validation)

Before this package transitions to `AUTHORIZED_FOR_PRODUCTION_DEPLOYMENT`, the following actions must be executed:

1. **Live Cloud Connectivity Run**:
   * Acquire Managed Identity token from `https://database.windows.net/.default`.
   * Verify private link DNS resolution to `10.x.x.x` for Azure SQL, Key Vault, and ADLS.
   * Verify public internet denial on storage container endpoints.
2. **Live Target Publication Drills**:
   * *Concurrent writers drill* against designated ADLS Gen2 path (`abfss://...`).
   * *Lost commit response drill* verifying idempotent receipt without duplicate partition writing.
   * *Staged crash drill* verifying automated orphan purge.
   * *Catalog failure drill* confirming transactional boundary integrity.
3. **Remote Git Preservation**:
   * Create signed release tag: `agent-dhara-phase4-production-candidate-2026-09-18`.
   * Push certified revision chain to governed immutable remote repository.
4. **Quad-Authority Sign-off**:
   * Secure cryptographic signatures from Security, Operations, Data Owner, and Deployment Authority.
   * Issue signed `DHARA_PRODUCTION_DEPLOYMENT_AUTHORIZATION_V1`.
