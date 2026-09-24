# Agent Dhara: Development & Architecture Progress Report (`dev.md`)

This document tracks all completed developments, existing module capabilities, and pending work items according to the compiler evolution implementation plan. Specifically, it highlights where core AI/compiler capabilities have been implemented but lack corresponding REST/HTTP API endpoints, specifying the exact files and paths that require updates.

---

## 1. Executive Summary & Current Maturity

Agent Dhara has transitioned from an LLM-assisted template generator into a **Compiler-Driven Data Engineering Platform**. 

- **Compiler Unit & Integration Tests:** `33/33 PASSED` (`tests/compiler/`)
- **Legacy Regression Suite:** `18/18 PASSED` (`tests/test_tsql_codegen_quality.py`)
- **Total Validated Suite:** `51/51 PASSED` (0 regressions)
- **Supported Compilation Targets:** 
  1. `python` (Pandas / Pure Python)
  2. `sql` (Azure SQL / T-SQL Stored Procedures)
  3. `pyspark` (PySpark DataFrames / Delta Lake)
  4. `fabric` (Microsoft Fabric Lakehouse `.ipynb` + Synapse Warehouse CTAS)
  5. `adf` (Azure Data Factory Pipeline JSON + Linked Services + Datasets)
  6. `databricks` (Databricks Delta Live Tables DLT + Unity Catalog + Workflow Jobs)

---

## 2. Completed Development (What Has Been Built)

### A. Compiler Core & Canonical Models
| Subsystem / Agent | File Path | API Status | Description |
| :--- | :--- | :--- | :--- |
| **Canonical Type System** | `agent/compiler/models/` (`profile.py`, `rules.py`, `issues.py`, `logical.py`, `physical.py`, `artifacts.py`, `validation.py`, `context.py`) | 🟡 Internal Python Only | Pydantic v2 typed models ensuring zero untyped raw dict exchange between agents. |
| **Event Bus** | `agent/compiler/events/event_bus.py` | 🟡 Internal Python Only | Decoupled publish-subscribe telemetry bus tracking compilation lifecycle events. |
| **Issue Compiler** | `agent/compiler/planners/issue_compiler.py` | 🟡 Internal Python Only | Normalizes disparate assessment findings and rules into canonical issues. |
| **Logical Planner & Confidence** | `agent/compiler/planners/logical_planner.py`, `confidence_agent.py` | 🟡 Internal Python Only | Builds deterministic, topologically ordered DAG of cleanup transformations with confidence gating. |
| **AST Optimization Agent** | `agent/compiler/optimizers/optimization_agent.py` | 🟡 Internal Python Only | Performs AST operator fusion (e.g., `lower(trim(x))`) and redundant step elimination. |
| **Physical Planner** | `agent/compiler/physical/physical_planner.py` | 🟡 Internal Python Only | Maps abstract logical DAG operations into concrete engine strategies for all 6 targets. |
| **Template Engine** | `agent/compiler/templates/engine.py` | 🟡 Internal Python Only | High-performance Jinja2 template renderer with dialect-specific templates in `templates/`. |
| **Language Router** | `agent/compiler/router/language_router.py` | 🟡 Internal Python Only | Dispatches physical plans to dedicated codegen agents based on target engine. |

### B. Multi-Dialect Code Generation & Validation
| Codegen Agent | Generator Path | Validator Path | Output Artifact | API Status |
| :--- | :--- | :--- | :--- | :--- |
| **Python Agent** | `agent/codegen/python/python_codegen_agent.py` | `agent/compiler/validators/python_validator.py` | `pipeline.py` (Pandas) | 🟡 Internal Python Only |
| **Azure SQL Agent** | `agent/codegen/sql/sql_codegen_agent.py` | `agent/compiler/validators/sql_validator.py` | `sp_clean_*.sql` (T-SQL) | 🟡 Internal Python Only |
| **PySpark Agent** | `agent/codegen/pyspark/pyspark_codegen_agent.py` | `agent/compiler/validators/spark_validator.py` | `spark_etl_*.py` (PySpark) | 🟡 Internal Python Only |
| **Fabric Agent** | `agent/codegen/fabric/fabric_codegen_agent.py` | `agent/compiler/validators/fabric_validator.py` | `fabric_notebook_*.ipynb` (OneLake + CTAS) | 🟡 Internal Python Only |
| **ADF Agent** | `agent/codegen/adf/adf_codegen_agent.py` | `agent/compiler/validators/adf_validator.py` | `adf_pipeline_*.json` (ADF activities + datasets) | 🟡 Internal Python Only |
| **Databricks Agent** | `agent/codegen/databricks/databricks_codegen_agent.py` | `agent/compiler/validators/databricks_validator.py` | `dlt_pipeline_*.py` (Delta Live Tables + Unity) | 🟡 Internal Python Only |

### C. Self-Healing Closed-Loop Auto-Repair
| Component | File Path | API Status | Capabilities |
| :--- | :--- | :--- | :--- |
| **Repair Classifier** | `agent/compiler/repair/repair_classifier.py` | 🟡 Internal Python Only | Categorizes diagnostic errors into `SYNTAX`, `IMPORT`, `REFERENCE`, or `DIALECT`. |
| **Repair Strategies** | `agent/compiler/repair/strategies/` (`syntax_repair.py`, `import_repair.py`, `reference_repair.py`, `dialect_repair.py`) | 🟡 Internal Python Only | Deterministic code-fix heuristics (bracket balancing, missing imports, unaliased column refs, SQL function dialect rewrites). |
| **Repair Agent** | `agent/compiler/repair/repair_agent.py` | 🟡 Internal Python Only | Orchestrates the iterative Generate $\rightarrow$ Validate $\rightarrow$ Classify $\rightarrow$ Fix $\rightarrow$ Re-validate cycle with max retry limit. |

### D. Governance, Testing & Packaging
| Component | File Path | API Status | Capabilities |
| :--- | :--- | :--- | :--- |
| **Companion Test Generator** | `agent/compiler/testing/test_generator.py` | 🟡 Internal Python Only | Synthesizes executable PyTest test suites and T-SQL `THROW` assertion blocks. |
| **Data Contract Agent** | `agent/compiler/contracts/contract_agent.py` | 🟡 Internal Python Only | Generates OpenDataContract v1.0.0 YAML schema contracts. |
| **Lineage Agent** | `agent/compiler/lineage/lineage_agent.py` | 🟡 Internal Python Only | Generates column-level lineage graph DAGs and Mermaid visual diagrams. |
| **Artifact Manifest** | `agent/compiler/manifests/artifact_manifest.py` | 🟡 Internal Python Only | Bundles code, tests, contracts, lineage, repair diagnostics into a unified `DeploymentPackage`. |
| **Cost Estimator** | `agent/compiler/planners/cost_estimator.py` | 🟡 Internal Python Only | Computes pre-deployment resource sizing (rows, memory GB, execution duration, driver/executor nodes). |
| **Pattern Library** | `agent/compiler/patterns/pattern_library.py` | 🟡 Internal Python Only | Pre-built enterprise schemas (Customer Master, Invoice Processing, HR Employee, Sales Orders, Product Catalog). |
| **Compiler Metrics** | `agent/compiler/metrics/compiler_metrics.py` | 🟡 Internal Python Only | Tracks Generation, Validation, Repair, Planning, and Quality metrics. |
| **Snapshot Store** | `agent/compiler/snapshot_store.py` | 🟡 Internal Python Only | In-memory audit store tracking full compiler snapshots per session with rollback capability. |
| **Compiler Pipeline** | `agent/compiler/compiler_pipeline.py` | 🟡 Internal Python Only | Master compilation orchestrator uniting all 13 stages synchronously. |

---

## 3. What Needs To Be Done (Pending Roadmap & Missing APIs)

While the compiler engine functions with 100% test pass rate, **several AI and compiler capabilities are only callable via internal Python code and lack external HTTP / REST API routes, UI integration, and autonomous decision layers**.

Below is the detailed list of work items, missing APIs, and the exact files that must be modified or created.

---

### Priority 1: Expose Compiler Core via REST API (Missing Backend Endpoints)

Currently, the existing endpoint `POST /etl/generate` in `agent/api_routes.py` routes through the legacy `etl_generate_code()` in `agent/etl_handlers.py`. The new `CompilerPipeline` has not yet been directly surfaced to the API layer.

#### 1.1 Direct Compiler Endpoint (`POST /api/compiler/compile`)
- **What it does:** Allows the UI or external callers to invoke the 13-stage `CompilerPipeline` directly with session context, choice of target engine, and custom rules.
- **Files Requiring Changes:**
  - ✏️ `agent/api_routes.py`:
    - Add Pydantic request model: `CompilerCompileRequest(session_id, target_engine, options, custom_rules)`
    - Add route `@router.post("/compiler/compile")` calling `CompilerPipeline().compile(context)`.
    - Return `DeploymentPackage` serialized as JSON.
  - ✏️ `agent/etl_handlers.py`:
    - Add helper `compile_etl_pipeline(session_id, target_engine, options)` to bridge session data into `CompilerContext`.
  - ✏️ `app/api/etl/compile/route.ts` *(NEW)*:
    - Next.js edge route proxying frontend requests to FastAPI backend.

#### 1.2 Dedicated Cost Estimator Endpoint (`POST /api/compiler/estimate-cost`)
- **What it does:** Allows the UI to display estimated memory (GB), runtime (seconds), and cluster node sizing **before** the user approves or downloads code.
- **Files Requiring Changes:**
  - ✏️ `agent/api_routes.py`:
    - Add `@router.post("/compiler/estimate-cost")`.
    - Input: `session_id`, `target_engine`, `estimated_row_count`.
    - Invokes: `agent.compiler.planners.cost_estimator.CostEstimator`.
  - ✏️ `app/api/etl/cost-estimate/route.ts` *(NEW)*:
    - Next.js route proxying cost estimation to the frontend UI.

#### 1.3 Enterprise Pattern Matching Endpoint (`POST /api/compiler/patterns/match`)
- **What it does:** Scans active dataset profiles against the enterprise Pattern Library (Customer Master, Invoices, HR, Orders) and returns matched patterns and confidence.
- **Files Requiring Changes:**
  - ✏️ `agent/api_routes.py`:
    - Add `@router.post("/compiler/patterns/match")`.
    - Invokes: `agent.compiler.patterns.pattern_library.PatternLibrary`.
  - ✏️ `app/api/etl/patterns/route.ts` *(NEW)*:
    - Next.js route for displaying matched pattern cards in the UI.

#### 1.4 Deployment Package Download (`GET /api/compiler/package/{session_id}/download`)
- **What it does:** Zips and downloads the complete deployment package:
  - `code/pipeline.py` (or `.ipynb` / `.json` / `.sql`)
  - `tests/test_pipeline.py`
  - `contracts/data_contract.yaml`
  - `lineage/lineage.json` + `lineage.mmd`
  - `manifest.json` (includes cost estimate, metrics, validation status)
- **Files Requiring Changes:**
  - ✏️ `agent/api_routes.py`:
    - Add `@router.get("/compiler/package/{session_id}/download")` returning `StreamingResponse(io.BytesIO(zip_buffer), media_type="application/zip")`.
  - ✏️ `app/api/etl/download-package/route.ts` *(NEW)*:
    - Route handling zip file download streaming for the browser.

#### 1.5 Update Existing `/etl/generate` to Support All 6 Targets
- **What it does:** Currently `EtlGeneratePayload` in `agent/api_routes.py` and `etl_generate_code` in `agent/etl_handlers.py` only route `python`, `sql`, `pyspark`. Fabric, ADF, and Databricks need to be accepted.
- **Files Requiring Changes:**
  - ✏️ `agent/api_routes.py` (Line 760):
    - Update `EtlGeneratePayload.engine` validation to accept `Literal["python", "sql", "pyspark", "fabric", "adf", "databricks"]`.
  - ✏️ `agent/etl_handlers.py` (Lines 1250–1350):
    - When `codegen_mode == "compiler"`, delegate directly to `CompilerPipeline`.

---

### Priority 2: Autonomous Intelligence Layer (Next Development Phases)

#### 2.1 Autonomous Architecture Recommender Agent (Phase 9)
- **Problem:** Currently, the human must manually select whether to generate Python, Spark, Fabric, SQL, ADF, or Databricks.
- **Goal:** Agent evaluates data volume, growth rate, SLA latency (e.g. 15m vs 24h), and budget, and autonomously recommends the optimal architecture.
- **Files to Create / Modify:**
  - 📄 `agent/compiler/recommender/architecture_recommender.py` *(NEW)*:
    - Rule-based & heuristic scoring engine evaluating:
      - Rows < 1M & Latency > 1h $\rightarrow$ Python Pandas
      - Rows > 10M $\rightarrow$ PySpark / Fabric
      - Real-time streaming or Delta $\rightarrow$ Databricks DLT
      - Azure native orchestration $\rightarrow$ ADF
  - ✏️ `agent/api_routes.py`:
    - Add `@router.post("/compiler/recommend-architecture")`.

#### 2.2 Transformation Knowledge Graph & Memory Store (Phase 7)
- **Problem:** When an issue is fixed once (e.g., regex email repair in a specific domain), the solution is lost on the next run.
- **Goal:** Store proven empirical solutions `(domain, issue, resolution_strategy, success_rate)` so the compiler gets smarter over time.
- **Files to Create / Modify:**
  - 📄 `agent/compiler/memory/knowledge_graph.py` *(NEW)*:
    - Persistent SQLite/JSON knowledge store.
    - Records pattern success rates and repair outcomes.
  - ✏️ `agent/compiler/compiler_pipeline.py`:
    - Query `TransformationKnowledgeGraph` before Logical Planning.

#### 2.3 Runtime Execution Feedback Loop (Phase 8)
- **Problem:** Cost estimation is theoretical; it needs feedback from actual cluster runs to calibrate.
- **Goal:** Ingest actual cluster execution runtimes, memory usage, rows fixed, and compare against `CostEstimate` predictions to auto-calibrate multipliers.
- **Files to Create / Modify:**
  - 📄 `agent/compiler/feedback/runtime_feedback.py` *(NEW)*:
    - Computes variance $\Delta$ between actual vs. estimated execution metrics.
  - ✏️ `agent/api_routes.py`:
    - Add `@router.post("/compiler/feedback/execution")` to receive telemetry from pipeline executions.

#### 2.4 Multi-Agent Code Review Board (Phase 11)
- **Problem:** Validation is currently focused on syntax and basic dialect checking.
- **Goal:** Replace single validator with a 4-agent Review Board:
  1. `SecurityReviewAgent`: Plaintext credential detection, SQL injection, unsafe calls.
  2. `PerformanceReviewAgent`: Spark wide-shuffles, cartesian joins, unindexed scans.
  3. `GovernanceReviewAgent`: PII masking compliance, lineage completeness.
  4. `BusinessReviewAgent`: Unmapped DQ findings, invariant coverage.
- **Files to Create / Modify:**
  - 📄 `agent/compiler/governance/review_board.py` *(NEW)*:
    - Orchestrates concurrent review agents and returns a unified sign-off verdict.
  - ✏️ `agent/compiler/compiler_pipeline.py`:
    - Replace basic validator step with `ReviewBoard`.

#### 2.5 Semantic Business Ontology Layer (Phase 10)
- **Problem:** Rules require explicit column references (`customer_id`, `email`).
- **Goal:** Understand domain entity relationships (`Customer` $\rightarrow$ `Identity`, `Address`, `Contact`, `Status`). When a rule states *"Customer must be active"*, it automatically binds `status = 'Active'`.
- **Files to Create / Modify:**
  - 📄 `agent/compiler/ontology/business_ontology.py` *(NEW)*:
    - Entity schemas and synonym mapping engine.
  - ✏️ `agent/compiler/planners/logical_planner.py`:
    - Resolve semantic ontology references to physical dataset columns.

---

### Priority 3: Frontend UI Integration (Next.js Application)

| UI Component | Current State | Target State | Target Files |
| :--- | :--- | :--- | :--- |
| **Engine Selector Dropdown** | Limited to Python/SQL/Spark | Add Microsoft Fabric, ADF, and Databricks DLT options | `app/data-pipeline/page.tsx` |
| **Cost Estimator Widget** | Not visible in UI | Display pre-run Memory (GB), Estimated Runtime, Recommended Cluster shape | `app/data-pipeline/components/CostEstimateWidget.tsx` *(NEW)* |
| **Review Board Sign-off Card** | Only shows basic validation pass/fail | Show Security, Performance, Governance, and Business audit badges | `app/data-pipeline/components/ReviewBoardModal.tsx` *(NEW)* |
| **Deployment Package Exporter** | Only downloads raw script | Download `.zip` containing script, tests, contract YAML, and lineage DAG | `app/data-pipeline/page.tsx` |
| **Compiler Metrics Telemetry** | Stored in snapshot JSON | Display compilation latency, operator fusion count, and repair analytics | `app/data-pipeline/components/CompilerMetricsPanel.tsx` *(NEW)* |

---

## 4. Development Matrix & Status Summary

| Capability | Backend Core | Tests | API Endpoint | Frontend UI | Status |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Logical & Physical Compiler** | ✅ | ✅ (33/33) | ❌ Needs API | 🟡 Partial | **Core Complete** |
| **Python / SQL / Spark Codegen** | ✅ | ✅ | 🟡 Legacy only | 🟡 Partial | **Core Complete** |
| **Fabric Lakehouse Notebooks** | ✅ | ✅ | ❌ Needs API | ❌ Missing | **Core Complete** |
| **ADF Pipeline JSON** | ✅ | ✅ | ❌ Needs API | ❌ Missing | **Core Complete** |
| **Databricks DLT Pipelines** | ✅ | ✅ | ❌ Needs API | ❌ Missing | **Core Complete** |
| **Closed-Loop Auto-Repair** | ✅ | ✅ | ❌ Needs API | ❌ Missing | **Core Complete** |
| **Companion Test Synthesis** | ✅ | ✅ | ❌ Needs API | ❌ Missing | **Core Complete** |
| **OpenDataContracts YAML** | ✅ | ✅ | ❌ Needs API | ❌ Missing | **Core Complete** |
| **Column Lineage DAGs** | ✅ | ✅ | ❌ Needs API | 🟡 Partial | **Core Complete** |
| **Artifact Manifest Package** | ✅ | ✅ | ❌ Needs API | ❌ Missing | **Core Complete** |
| **Cost Estimator** | ✅ | ✅ | ❌ Needs API | ❌ Missing | **Core Complete** |
| **Pattern Library** | ✅ | ✅ | ❌ Needs API | ❌ Missing | **Core Complete** |
| **Compiler Metrics Collector** | ✅ | ✅ | ❌ Needs API | ❌ Missing | **Core Complete** |
| **Architecture Recommender** | 🔲 Planned | 🔲 | 🔲 | 🔲 | **Phase 9** |
| **Knowledge Graph Memory** | 🔲 Planned | 🔲 | 🔲 | 🔲 | **Phase 7** |
| **Runtime Feedback Loop** | 🔲 Planned | 🔲 | 🔲 | 🔲 | **Phase 8** |
| **Multi-Agent Review Board** | 🔲 Planned | 🔲 | 🔲 | 🔲 | **Phase 11** |
| **Semantic Business Ontology** | 🔲 Planned | 🔲 | 🔲 | 🔲 | **Phase 10** |

---

## 5. Environment & API Key Configuration Guide

### A. Primary Configuration Files (Where You Must Provide Your API Keys)

To configure API keys and credentials, you only need to edit **two primary configuration files** in the root of the workspace. All backend agents and frontend routes automatically read from these:

| Priority | Target File Path | Purpose | Key Variables to Set |
| :---: | :--- | :--- | :--- |
| **1 (Backend)** | [`.env`](file:///c:/Users/srevanku/OneDrive%20-%20Capgemini/Desktop/test/Agent-Dhara---Backend-App/.env) | Python backend server, MCP server, compiler agents, LLM enhancers | `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_DEPLOYMENT`, `AZURE_SQL_PASSWORD`, `AZURE_STORAGE_ACCOUNT_KEY` |
| **2 (Frontend)** | [`.env.local`](file:///c:/Users/srevanku/OneDrive%20-%20Capgemini/Desktop/test/Agent-Dhara---Backend-App/.env.local) | Next.js frontend, Server-Side API routes (`lib/azure-agent.ts`, chat proxy) | `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_DEPLOYMENT_NAME`, `BACKEND_AUTH_TOKEN` |

---

### B. Azure AI Foundry / Azure OpenAI Credentials Mapping

Based on the Azure AI Foundry deployment configuration:

```ini
# Endpoint URL
AZURE_OPENAI_ENDPOINT=https://data-sogeti-aifoundry.cognitiveservices.azure.com/

# Subscription API Key (Paste your Azure AI Foundry key here)
AZURE_OPENAI_API_KEY=your_azure_subscription_key_here

# API Version (matches Azure deployment)
AZURE_OPENAI_API_VERSION=2024-12-01-preview

# Model & Deployment Name
AZURE_OPENAI_DEPLOYMENT=gpt-5.4-mini
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-5.4-mini
AZURE_OPENAI_MODEL_NAME=gpt-5.4-mini

# Optional separate model for ETL code generation
# AZURE_OPENAI_CODEGEN_DEPLOYMENT=gpt-5.4-mini

# Fallback for standard OpenAI SDK calls
OPENAI_API_KEY=your_azure_subscription_key_here
```

---

### C. Exhaustive List of Files That Read API Keys & Credentials

The table below catalogs every file in the codebase that consumes environment keys and credentials:

#### 1. Azure OpenAI & LLM Inference
| File Path | Variables Consumed | How It Uses the Key |
| :--- | :--- | :--- |
| [`agent/model_config.py`](file:///c:/Users/srevanku/OneDrive%20-%20Capgemini/Desktop/test/Agent-Dhara---Backend-App/agent/model_config.py) | `AZURE_OPENAI_ENDPOINT`<br>`AZURE_OPENAI_API_KEY`<br>`AZURE_OPENAI_API_VERSION`<br>`AZURE_OPENAI_DEPLOYMENT`<br>`AZURE_OPENAI_CODEGEN_DEPLOYMENT`<br>`OPENAI_API_KEY` | **Central LLM configuration loader** for all backend agents. Instantiates `AzureOpenAI` or `OpenAI` client. |
| [`lib/azure-agent.ts`](file:///c:/Users/srevanku/OneDrive%20-%20Capgemini/Desktop/test/Agent-Dhara---Backend-App/lib/azure-agent.ts) | `AZURE_OPENAI_ENDPOINT`<br>`AZURE_OPENAI_API_KEY`<br>`AZURE_OPENAI_DEPLOYMENT_NAME`<br>`AZURE_OPENAI_API_VERSION` | **Next.js server-side chat & agent router**. Dispatches prompts to Azure OpenAI chat completions. |
| [`agent/chat_graph/llm.py`](file:///c:/Users/srevanku/OneDrive%20-%20Capgemini/Desktop/test/Agent-Dhara---Backend-App/agent/chat_graph/llm.py) | `AZURE_OPENAI_ENDPOINT`<br>`AZURE_OPENAI_API_KEY`<br>`AZURE_OPENAI_DEPLOYMENT`<br>`AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT` | LangGraph agent chat nodes and embedding generators. |
| [`agent/llm_assessment_enhancer.py`](file:///c:/Users/srevanku/OneDrive%20-%20Capgemini/Desktop/test/Agent-Dhara---Backend-App/agent/llm_assessment_enhancer.py) | `AZURE_OPENAI_ENDPOINT`<br>`AZURE_OPENAI_API_KEY`<br>`AZURE_OPENAI_DEPLOYMENT` | Generates executive summaries and business risk assessment from data profiling. |
| [`agent/sql_nl_query.py`](file:///c:/Users/srevanku/OneDrive%20-%20Capgemini/Desktop/test/Agent-Dhara---Backend-App/agent/sql_nl_query.py) | `AZURE_OPENAI_ENDPOINT`<br>`AZURE_OPENAI_API_KEY`<br>`AZURE_OPENAI_DEPLOYMENT` | Natural Language to SQL converter. |
| [`agent/mcp_server.py`](file:///c:/Users/srevanku/OneDrive%20-%20Capgemini/Desktop/test/Agent-Dhara---Backend-App/agent/mcp_server.py) | `AZURE_OPENAI_ENDPOINT`<br>`AZURE_OPENAI_API_KEY` | Model Context Protocol (MCP) server startup check. |
| [`agent/specialists/semantic_infer_agent.py`](file:///c:/Users/srevanku/OneDrive%20-%20Capgemini/Desktop/test/Agent-Dhara---Backend-App/agent/specialists/semantic_infer_agent.py) | Via `load_llm_config()` | Infers column business semantics and PII classifications. |
| [`agent/specialists/semantic_rules_generator.py`](file:///c:/Users/srevanku/OneDrive%20-%20Capgemini/Desktop/test/Agent-Dhara---Backend-App/agent/specialists/semantic_rules_generator.py) | Via `load_llm_config()` | Proposes data quality business rules using LLM analysis. |
| [`agent/llm_schema_enricher.py`](file:///c:/Users/srevanku/OneDrive%20-%20Capgemini/Desktop/test/Agent-Dhara---Backend-App/agent/llm_schema_enricher.py) | Via `load_llm_config()` | Generates schema documentation and column descriptions. |
| [`agent/business_requirements_parser.py`](file:///c:/Users/srevanku/OneDrive%20-%20Capgemini/Desktop/test/Agent-Dhara---Backend-App/agent/business_requirements_parser.py) | Via `load_llm_config()` | Parses natural language requirements into structured rules. |

#### 2. Azure Storage & Database Credentials
| File Path | Variables Consumed | Purpose |
| :--- | :--- | :--- |
| [`connectors/azure_blob.py`](file:///c:/Users/srevanku/OneDrive%20-%20Capgemini/Desktop/test/Agent-Dhara---Backend-App/connectors/azure_blob.py) | `AZURE_STORAGE_CONNECTION_STRING`<br>`AZURE_STORAGE_ACCOUNT_NAME`<br>`AZURE_STORAGE_ACCOUNT_KEY` | Ingests source CSV/Parquet files from Azure Blob / ADLS Gen2. |
| [`connectors/azure_sql.py`](file:///c:/Users/srevanku/OneDrive%20-%20Capgemini/Desktop/test/Agent-Dhara---Backend-App/connectors/azure_sql.py) | `AZURE_SQL_SERVER`<br>`AZURE_SQL_DATABASE`<br>`AZURE_SQL_USERNAME`<br>`AZURE_SQL_PASSWORD`<br>`AZURE_SQL_CONNECTION_STRING` | Connects to Azure SQL Database for assessment and stored procedure deployment. |
| [`agent/fabric_lakehouse_mirror.py`](file:///c:/Users/srevanku/OneDrive%20-%20Capgemini/Desktop/test/Agent-Dhara---Backend-App/agent/fabric_lakehouse_mirror.py) | `FABRIC_CLIENT_ID`<br>`FABRIC_CLIENT_SECRET`<br>`FABRIC_TENANT_ID`<br>`FABRIC_WORKSPACE_ID` | Connects to Microsoft Fabric OneLake Lakehouse API. |

#### 3. Backend-to-Frontend Service Security
| File Path | Variables Consumed | Purpose |
| :--- | :--- | :--- |
| [`agent/api_routes.py`](file:///c:/Users/srevanku/OneDrive%20-%20Capgemini/Desktop/test/Agent-Dhara---Backend-App/agent/api_routes.py) | `BACKEND_AUTH_TOKEN`<br>`CORS_ALLOW_ORIGINS` | Bearer token authentication guard on backend API endpoints. |
| [`lib/backend-auth.ts`](file:///c:/Users/srevanku/OneDrive%20-%20Capgemini/Desktop/test/Agent-Dhara---Backend-App/lib/backend-auth.ts) | `BACKEND_AUTH_TOKEN`<br>`BACKEND_BASE_URL` | Next.js API client attaches this bearer token when forwarding requests to Python backend. |

---

*This document is maintained as the single source of truth for architecture development and environment deployment.*

