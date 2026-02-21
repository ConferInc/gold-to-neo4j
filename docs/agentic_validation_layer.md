# Checking & Validation Layer (Agentic Orchestrator)

## Core Idea
The ingestion pipeline stays deterministic and safe, while a dedicated **agentic validation layer** handles ambiguous cases. The agentic layer never writes data directly; it only **proposes decisions** that are validated and gated by strict policies before any change is applied. This improves resilience without sacrificing safety—critical for allergen, health, and diet data.

---

## Architecture Overview
1. **Deterministic Core Pipeline**
   - Batch ingestion and realtime workers remain the source of truth.
   - Heuristics and policy checks run first.

2. **Agentic Orchestrator (Single Service)**
   - Stateless FastAPI microservice that routes tasks.
   - Uses **LangGraph** to run task-specific workflows.
   - Uses **LiteLLM** to call `gpt-4.1-mini`.

3. **Task-Specific Workflows (LangGraph)**
   - `schema_drift_resolver`
   - `failure_triage`
   - `reconciliation_backfill`

4. **Validation & Policy Gate (Pipeline Side)**
   - Validates agent outputs and enforces safety policies.
   - Safety-critical changes require review.

---

## Agentic Workflow Diagram

```text
                ┌──────────────────────────────────────────┐
                │      Deterministic Ingestion Pipeline    │
                │  (batch jobs + realtime worker services) │
                └──────────────────────────────────────────┘
                                   │
                                   │ heuristics fail / ambiguity
                                   ▼
                     ┌────────────────────────────────┐
                     │  Agentic Orchestrator (API)    │
                     │  FastAPI + LangGraph + LiteLLM │
                     └────────────────────────────────┘
                                   │
                   ┌───────────────┼────────────────┐
                   │               │                │
                   ▼               ▼                ▼
        ┌────────────────┐  ┌────────────────┐  ┌────────────────────┐
        │ Schema Drift   │  │ Failure Triage │  │ Reconciliation     │
        │ Workflow       │  │ Workflow       │  │ Workflow           │
        └────────────────┘  └────────────────┘  └────────────────────┘
                   │               │                │
                   └───────────────┼────────────────┘
                                   ▼
                     ┌────────────────────────────────┐
                     │ Validation + Policy Gate       │
                     │ (type checks, safety rules)    │
                     └────────────────────────────────┘
                                   │
                      ┌────────────┴────────────┐
                      │                         │
                      ▼                         ▼
             ┌─────────────────┐      ┌────────────────────┐
             │ Auto-Apply Safe │      │ Review Required    │
             │ (low-risk only) │      │ (safety-critical)  │
             └─────────────────┘      └────────────────────┘
```

---

## Why This Is Agentic
The orchestrator is agentic because it runs **multi-step workflows** with tool calls, LLM reasoning, and explicit validation gates. Each workflow uses tools and constraints to narrow the decision space and prevent unsafe actions.

---

## Frameworks Used
- **FastAPI** — service routing and HTTP interface
- **LangGraph** — workflow orchestration per task
- **LangChain Tools** — wrapped Python helpers
- **LiteLLM** — LLM client (model: `gpt-4.1-mini`)
- **Pydantic** — strict JSON schemas + validation

---

## Heuristics First, Agent Second
The pipeline uses heuristics before invoking the agentic layer. The agent is called **only when heuristics cannot confidently resolve the issue.** After the agent responds, its output is **validated again** by deterministic checks and policy rules before any action is taken.

---

# Task 1: Schema Drift Resolution

### Purpose
Keep batch ingestion running when upstream schemas change slightly.

### Heuristics Used
- Name similarity (token matching, edit distance)
- Type compatibility checks
- Foreign-key / `_id` pattern matching
- Synonym dictionary

### Agent Workflow
- If heuristics can’t resolve missing columns, the agent suggests alias mappings.
- Output must pass:
  - column existence checks
  - type compatibility
  - no duplicate aliasing
  - safety-critical policy checks

### Safety Rule
Mappings touching allergens, health conditions, or diet preferences require manual review before applying.

---

# Task 2: Failure Triage (Realtime Outbox)

### Purpose
Prevent poison events from clogging the queue while retrying transient failures.

### Heuristics Used
- Known error taxonomy
- Retry counter and backoff rules

### Agent Workflow
- If classification is ambiguous, agent returns:
  - `retryable`
  - `poison`
  - `needs_review`

### Validation/Policy
- Only known error codes are allowed.
- Retryable errors get scheduled backoff.
- Poison events are marked failed.

---

# Task 3: Reconciliation / Backfill

### Purpose
Detect silent drift between Supabase and Neo4j and propose targeted recovery.

### Heuristics Used
- Counts comparisons
- Threshold checks
- Checksum sampling

### Agent Workflow
- If drift exceeds threshold, agent proposes a backfill time window.
- Output must pass:
  - range sanity checks
  - maximum window size policy

---

## Safety & Governance
- All agent outputs are validated against strict JSON schemas.
- All actions are logged for auditability.
- Safety-critical suggestions are reviewed before application.
- If the agent fails or returns invalid output, the system falls back to heuristics or fails safely.

---

## Why This Design
- **Deterministic core** for safety-critical ingestion
- **Agentic intelligence** only where ambiguity exists
- **Strict validation and policy gates** ensure safety
- **Modular workflows** allow future extension without breaking existing logic
