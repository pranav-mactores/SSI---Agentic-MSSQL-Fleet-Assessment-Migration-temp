"""
agent/loop.py  –  Claude tool-use agent loop.
"""
import json
import os
import textwrap
from datetime import datetime
from typing import Any
import anthropic

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
from db.connection import ServerContext
from config.settings import CLAUDE_MODEL
from tools.schema        import tool_list_databases, tool_analyze_schema
from tools.features      import tool_analyze_sql_features
from tools.procedural    import tool_analyze_procedural_code
from tools.jobs          import tool_analyze_jobs
from tools.performance   import tool_analyze_performance
from tools.backups       import tool_analyze_backups
from tools.linked_servers import tool_analyze_linked_servers
from tools.mail          import tool_analyze_database_mail
from tools.sql_exec      import tool_execute_sql
from tools.proc_analyzer import tool_deep_analyze_procedures

def build_tools() -> list[dict]:
    return [
        {
            "name": "list_databases",
            "description": "List all online user databases with size, recovery model, and compatibility level. Call first.",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "analyze_schema",
            "description": (
                "Analyse all schema objects in a database: every table with row counts, "
                "views, SPs, functions, triggers, and migration risk flags."
            ),
            "input_schema": {"type": "object",
                "properties": {"database": {"type": "string"}}, "required": ["database"]},
        },
        {
            "name": "analyze_sql_features",
            "description": (
                "Inventory ALL SQL Server native features in a database "
                "(version/edition-gated automatically): Service Broker, CDC, Change Tracking, "
                "Replication, Always On, Mirroring, FTS, In-Memory OLTP, FILESTREAM, "
                "Partitioning, Temporal, Columnstore, Graph, Ledger, XML, Spatial, "
                "RLS, DDM, TDE, Always Encrypted, Audit, CLR, UDTs, Sequences, "
                "Synonyms, Certs, Orphaned Users, Missing PKs, Permissions."
            ),
            "input_schema": {"type": "object",
                "properties": {"database": {"type": "string"}}, "required": ["database"]},
        },
        {
            "name": "analyze_jobs",
            "description": "All SQL Agent jobs, steps, schedules, and last run status (skipped on Express).",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "analyze_performance",
            "description": "Top wait stats, top missing index recommendations, and memory usage.",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "analyze_backups",
            "description": "Backup history for all databases (last 30 days) from msdb.",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "analyze_linked_servers",
            "description": "All configured linked servers and objects referencing them.",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "analyze_database_mail",
            "description": "Database Mail profiles and accounts configured in msdb.",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "deep_analyze_procedures",
            "description": (
                "Claude-powered deep analysis of stored procedures and functions. "
                "Call this AFTER analyze_procedural_code for each user database. "
                "It reads actual source code — not just metadata — for prioritised objects "
                "(those with CRITICAL/HIGH risk flags OR > 300 lines). "
                "Returns: business logic summary, migration complexity rating "
                "(Simple/Moderate/Complex/Very Complex), risks beyond pattern matching, "
                "rewrite recommendations, migrate-as-is flag, and estimated rewrite hours. "
                "Large SPs (10 000+ lines) are chunked automatically — you don't need to manage this."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "database": {"type": "string", "description": "Database name to analyse."},
                },
                "required": ["database"],
            },
        },
        {
            "name": "execute_sql",
            "description": (
                "ERROR RECOVERY TOOL. Call this when any standard tool returns "
                "a row containing '_error'. Use it to run a corrected, "
                "version-appropriate SELECT query as a replacement. "
                "The error row includes '_server_year', '_edition', and '_hint' "
                "to help you write the right query. "
                "Rules: SELECT only — no INSERT/UPDATE/DELETE/DROP/EXEC/xp_cmdshell. "
                "Results are capped at 500 rows. "
                "Always set 'purpose' to describe what data you are recovering."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "sql":      {"type": "string",
                                 "description": "A SELECT query compatible with the server version."},
                    "purpose":  {"type": "string",
                                 "description": "What feature/data this query is recovering."},
                    "database": {"type": "string",
                                 "description": "Database context. Omit to run against master."},
                },
                "required": ["sql", "purpose"],
            },
        },
        {
            "name": "analyze_procedural_code",
            "description": (
                "Deep analysis of every stored procedure, scalar/TVF/inline function, "
                "and CLR object in a database. Call for every user database. "
                "Scans all source code for 19 risk patterns: xp_cmdshell, "
                "OPENROWSET/OPENQUERY, dynamic EXEC string concat, four-part linked names, "
                "CURSORs, global temp tables, deprecated *=/=* joins, SET ROWCOUNT, "
                "NOLOCK hints, @@ERROR without TRY/CATCH, undocumented SPs, GOTO, "
                "SELECT *, WAITFOR, RECOMPILE hints, other xp_ calls, RAISERROR, "
                "EXECUTE AS, sp_executesql. Also provides per-object complexity metrics, "
                "full parameter inventory, dependency mapping, and CLR assembly details."
            ),
            "input_schema": {
                "type": "object",
                "properties": {"database": {"type": "string", "description": "Database name."}},
                "required": ["database"],
            },
        },
    ]



def build_dispatch(ctx: ServerContext, out_dir: str) -> dict[str, Any]:
    return {
        "list_databases":       lambda _: tool_list_databases(ctx, out_dir),
        "analyze_schema":       lambda i: tool_analyze_schema(ctx, i["database"], out_dir),
        "analyze_sql_features": lambda i: tool_analyze_sql_features(ctx, i["database"], out_dir),
        "analyze_jobs":         lambda _: tool_analyze_jobs(ctx, out_dir),
        "analyze_performance":  lambda _: tool_analyze_performance(ctx, out_dir),
        "analyze_backups":      lambda _: tool_analyze_backups(ctx, out_dir),
        "analyze_linked_servers": lambda _: tool_analyze_linked_servers(ctx, out_dir),
        "analyze_database_mail":  lambda _: tool_analyze_database_mail(ctx, out_dir),
        "analyze_procedural_code": lambda i: tool_analyze_procedural_code(ctx, i["database"], out_dir),
        "deep_analyze_procedures": lambda i: tool_deep_analyze_procedures(ctx, i["database"], out_dir),
        "execute_sql":             lambda i: tool_execute_sql(ctx, i["sql"], i["purpose"], i.get("database")),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────────────────────


def build_system_prompt(ctx: ServerContext) -> str:
    return textwrap.dedent(f"""
        You are a SQL Server migration analyst agent performing a dual-target migration
        feasibility assessment.

        SOURCE SERVER
        Connected server  : {ctx.server}
        Detected version  : SQL Server {ctx.year}  ({ctx.version_str})
        Edition           : {ctx.edition}

        TARGET ENVIRONMENTS (assess all three):
        1. Amazon RDS for SQL Server 2022 (SE or EE)
        2. Amazon RDS Custom for SQL Server 2022
        3. Amazon Aurora PostgreSQL (latest LTS)

        IMPORTANT NOTES FOR THIS SERVER:
        - SQL Agent available : {"YES" if ctx.has_agent() else "NO (Express edition)"}
        - Enterprise features : {"YES (full feature set)" if ctx.is_enterprise else "NO — some features gated"}
        - Temporal Tables     : {"YES (2016+)" if ctx.v(13) else "NO (requires SQL 2016+)"}
        - Graph Tables        : {"YES (2017+)" if ctx.v(14) else "NO (requires SQL 2017+)"}
        - Ledger Tables       : {"YES (2022+)" if ctx.v(16) else "NO (requires SQL 2022+)"}
        - RLS / DDM / AE      : {"YES (2016+)" if ctx.v(13) else "NO (requires SQL 2016+)"}

        PROCESS:
        1. Call list_databases first.
        2. For EACH user database call analyze_schema, analyze_sql_features,
           analyze_procedural_code, AND deep_analyze_procedures.
        3. Call analyze_jobs, analyze_performance, analyze_backups,
           analyze_linked_servers, analyze_database_mail once each.
        4. Synthesise a detailed Markdown report covering BOTH migration targets
           and a side-by-side comparison. Be specific — reference exact object
           names, counts, and findings from the tool results.

        REPORT SECTIONS:
        ## Executive Summary
        ## Source Server Profile
        ## Database Inventory
        ## Schema Analysis  (per database)
        ## Native SQL Server Features  (per database)
        ## SQL Agent Jobs
        ## Linked Servers & Cross-DB Dependencies
        ## Database Mail
        ## Backup Strategy & RPO Analysis
        ## Performance Observations

        ## Migration Path 1 — Amazon RDS for SQL Server 2022
        ### Compatibility Assessment
          - Which SQL Server features are supported / unsupported on RDS
          - RDS-specific limitations: no FILESTREAM, no linked servers to on-prem,
            limited Windows Auth (needs AWS Directory Service), SQL Agent restrictions
            (only T-SQL and SSIS steps supported), no MSDTC, backups go to S3,
            no OS-level access, no third-party tools or agents
          - Required configuration changes
          - AWS DMS feasibility for data migration
        ### Key Pointers
          - List 4-6 bullet points summarising the most important facts for this workload
        ### Effort & Risk Estimate
          - Complexity rating (Low / Medium / High)
          - Estimated effort in weeks
          - Key risks and mitigations

        ## Migration Path 2 — Amazon RDS Custom for SQL Server 2022
        ### Compatibility Assessment
          - RDS Custom gives OS-level access to the underlying EC2 instance
          - Supports features blocked by standard RDS: FILESTREAM, CLR assemblies,
            linked servers (including to on-prem), MSDTC distributed transactions,
            Windows Authentication without AWS Directory Service, custom SQL Agent
            job types (PowerShell, CmdExec, ActiveX), third-party monitoring agents,
            custom startup parameters and trace flags
          - Still AWS-managed patching and backups but with operator override capability
          - Supports SQL Server 2019 and 2022 Enterprise and Standard editions
          - Higher operational responsibility than standard RDS
          - Assess which of the detected features in this instance specifically benefit
            from RDS Custom vs standard RDS
        ### Key Pointers
          - List 4-6 bullet points summarising the most important facts for this workload
        ### Effort & Risk Estimate
          - Complexity rating (Low / Medium / High)
          - Estimated effort in weeks
          - Key risks and mitigations

        ## Migration Path 3 — Amazon Aurora PostgreSQL
        ### Schema Conversion Requirements
          - Data type mapping (e.g. DATETIME→TIMESTAMP, NVARCHAR→VARCHAR,
            BIT→BOOLEAN, UNIQUEIDENTIFIER→UUID, MONEY→NUMERIC, IMAGE→BYTEA)
          - T-SQL → PL/pgSQL conversion for every stored procedure and function
            (list each object and what needs to change)
          - Syntax differences: TOP→LIMIT, ISNULL→COALESCE, GETDATE()→NOW(),
            +string concat→||, TRY/CATCH→EXCEPTION, IDENTITY→SERIAL/GENERATED,
            ##temp tables→session temp tables, four-part names unsupported
          - SQL Server-specific features with no Aurora equivalent:
            CLR, FILESTREAM, Service Broker, SQL Agent (use AWS Lambda/EventBridge),
            linked servers, Database Mail (use SES), Full-Text Search (use pg_trgm/pg_fts)
          - AWS Schema Conversion Tool (SCT) applicability and conversion rate estimate
          - AWS DMS feasibility and CDC support
        ### Key Pointers
          - List 4-6 bullet points summarising the most important facts for this workload
        ### Effort & Risk Estimate
          - Complexity rating (Low / Medium / High)
          - Estimated effort in weeks
          - Key risks and mitigations

        ## Migration Feasibility Comparison
        First produce a summary paragraph (3-4 sentences) of the overall picture.

        Then produce this comparison table — one row per factor, one column per target:

        | Factor                              | RDS SQL Server 2022 | RDS Custom SQL Server 2022 | Aurora PostgreSQL |
        |-------------------------------------|---------------------|---------------------------|-------------------|
        | Schema migration effort             |                     |                           |                   |
        | Stored procedure / code rewrite     |                     |                           |                   |
        | Data type compatibility             |                     |                           |                   |
        | Feature parity with source          |                     |                           |                   |
        | OS / infrastructure access          |                     |                           |                   |
        | SQL Agent compatibility             |                     |                           |                   |
        | Linked server support               |                     |                           |                   |
        | CLR / FILESTREAM support            |                     |                           |                   |
        | Windows Auth support                |                     |                           |                   |
        | Application changes required        |                     |                           |                   |
        | AWS migration tooling (SCT / DMS)   |                     |                           |                   |
        | Estimated migration duration        |                     |                           |                   |
        | Operational complexity (post-mig)   |                     |                           |                   |
        | Licensing cost impact               |                     |                           |                   |
        | Overall feasibility                 | 🟢/🟡/🔴            | 🟢/🟡/🔴                  | 🟢/🟡/🔴         |

        Fill every cell with a specific, concise value based on the actual findings
        from this server. Do not leave cells blank or generic.

        ## Edition Requirements — Standard vs Enterprise

        For each SQL Server target (RDS SQL Server 2022 and RDS Custom SQL Server 2022),
        analyse whether this specific workload needs Enterprise Edition or can run on
        Standard Edition. Aurora PostgreSQL has no edition distinction — note that instead.

        ### Features Detected in This Instance — Edition Impact
        For every active feature found by the tools, state whether it requires Enterprise
        Edition or is available in Standard Edition on the target. Use this as your guide:

        Features that require Enterprise Edition on RDS / RDS Custom:
        - Always On Availability Groups (SE has Basic AG — single database, limited)
        - Online index rebuild (SE must use offline rebuild)
        - Unlimited virtualisation rights
        - Advanced auditing beyond basic
        - Resource Governor
        - Distributed partitioned views (cross-server)
        - In-Memory OLTP beyond 32 GB RAM limit (SE is limited)

        Features available in Standard Edition (SQL Server 2016 SP1+):
        - Columnstore indexes (non-clustered and clustered)
        - Table and index partitioning
        - Data compression (row and page)
        - Basic In-Memory OLTP (up to 32 GB per database)
        - Change Data Capture
        - Row-Level Security
        - Dynamic Data Masking
        - Transparent Data Encryption
        - Always Encrypted
        - Temporal Tables
        - Full-Text Search
        - Replication (as subscriber)
        - Basic Availability Groups (1 database, no readable secondary)

        Features NOT available in Standard Edition:
        - Read-scale Availability Groups with readable secondaries
        - Unlimited database size (SE: 524 GB max per database on older versions;
          no hard limit from SQL 2019+, but check RDS documentation)
        - Some RDS-specific EE features (check AWS documentation)

        ### Edition Recommendation Table
        Produce this table based on actual features detected:

        | Factor                              | RDS SQL Server 2022 | RDS Custom SQL Server 2022 |
        |-------------------------------------|---------------------|---------------------------|
        | Features requiring Enterprise       | (list them)         | (list them)               |
        | Can workload run on Standard?       | Yes / No / Partial  | Yes / No / Partial        |
        | Recommended edition                 | SE or EE            | SE or EE                  |
        | Key reason for recommendation       |                     |                           |
        | Estimated licence cost impact       | Lower/Higher/Same   | Lower/Higher/Same         |

        Then give 3-5 bullet points summarising the edition decision for this workload,
        referencing the specific features detected (e.g. CDC, columnstore, partitioning).

        ## Recommendation
        State clearly which target is the best fit for this specific workload and why.
        If the answer depends on a business decision (e.g. cost vs compatibility),
        say so explicitly and explain the trade-off.
        Reference specific findings (object names, counts, features) to justify.

        ERROR RECOVERY:
        If ANY tool returns a result containing "_error", do NOT skip that feature.
        Instead:
          1. Read "_hint" in the error row — it explains why the query failed.
          2. Read "_server_year" and "_edition" from the error row.
          3. Compose a corrected SELECT query for SQL Server {ctx.year}.
          4. Call execute_sql with your corrected query and a clear "purpose".
          5. If execute_sql also fails, note the limitation in the report.

        Common version differences:
          STRING_AGG()                        → SQL 2017+; use STUFF(FOR XML PATH) on 2014/2016
          sys.tables.is_node / is_edge        → SQL 2017+
          sys.tables.ledger_type              → SQL 2022+
          sys.tables.temporal_type            → SQL 2016+
          sys.databases.is_accelerated_*     → SQL 2019+
          sys.databases.is_change_feed_*     → SQL 2022+
          sys.availability_groups.is_contained → SQL 2022+

        FLAGGING RULES:
        - Orphaned users         → ❌ HIGH PRIORITY BLOCKER
        - Tables missing PKs     → ⚠️  RISK
        - Active Service Broker  → 🔔 SPECIAL CUTOVER REQUIRED
        - Active CDC             → 🔔 SPECIAL CUTOVER REQUIRED
        - Active Replication     → 🔔 SPECIAL CUTOVER REQUIRED
        - Always On / Mirroring  → 🔔 SPECIAL CUTOVER REQUIRED
        - CLR assemblies         → 🔍 MANUAL REVIEW REQUIRED
        - FILESTREAM / FileTable → 🔍 MANUAL REVIEW REQUIRED
        - In-Memory OLTP         → 🔍 MANUAL REVIEW REQUIRED
        - TDE / Always Encrypted → 🔑 KEY MIGRATION REQUIRED
    """).strip()


# ─────────────────────────────────────────────────────────────────────────────
# DATA COLLECTION  (Python-driven, guaranteed coverage)
# ─────────────────────────────────────────────────────────────────────────────


_TRIM_MAX_ROWS = 5

def _trim(obj: Any, max_rows: int = _TRIM_MAX_ROWS, max_str: int = 400) -> Any:
    """
    Recursively truncate tool results to stay within Claude's 200k token limit.
    Strings are capped at max_str chars; lists keep at most max_rows non-error rows.
    Full data is in CSV — Claude only needs key metrics and error rows.
    """
    if isinstance(obj, str):
        if len(obj) > max_str:
            return obj[:max_str] + f"[{len(obj) - max_str} chars truncated]"
        return obj
    if isinstance(obj, dict):
        return {k: _trim(v, max_rows, max_str) for k, v in obj.items()}
    if isinstance(obj, list):
        errors    = [x for x in obj if isinstance(x, dict) and x.get("_error")]
        non_err   = [x for x in obj if not (isinstance(x, dict) and x.get("_error"))]
        keep_n    = max(0, max_rows - len(errors))
        kept      = errors + [_trim(x, max_rows, max_str) for x in non_err[:keep_n]]
        omitted   = len(non_err) - keep_n
        if omitted > 0:
            kept.append({"_note": f"{omitted} more rows omitted — see CSV for full data"})
        return kept
    return obj


def _compact_features(result: dict) -> dict:
    """
    Replace per-feature row lists with one-liner status strings for Phase 2.
    Reduces analyze_sql_features from hundreds of rows to ~30 key:string pairs.
    Full feature data is already in the features CSV.
    """
    SKIP = {"database", "csv_path"}
    out: dict = {}
    for k, v in result.items():
        if k in SKIP or not isinstance(v, list):
            out[k] = v
            continue
        errors  = [r for r in v if isinstance(r, dict) and r.get("_error")]
        actives = [r for r in v if isinstance(r, dict) and not r.get("_error") and not r.get("_note")]
        if errors:
            out[k] = f"ERROR: {str(errors[0].get('_error', ''))[:120]}"
        elif actives:
            out[k] = f"DETECTED ({len(actives)} rows) — see CSV"
        else:
            out[k] = "not detected / inactive"
    return out


def _collect_all_data(
    dispatch: dict[str, Any], ctx: ServerContext
) -> tuple[list[dict], list[dict]]:
    """
    Call every analysis tool directly in Python — no Claude involvement.

    Returns (tool_use_blocks, tool_result_blocks) formatted as Anthropic
    API content blocks.  Callers inject these as a single synthetic
    assistant + user turn so Claude sees exactly what it would have seen
    if it had called the tools itself, but every database is guaranteed
    to be covered regardless of turn limits.
    """
    tool_use_blocks: list[dict]    = []
    tool_result_blocks: list[dict] = []
    _idx = 0
    _max_rows = _TRIM_MAX_ROWS  # updated below after we know the database count

    def _run(name: str, inputs: dict) -> dict:
        nonlocal _idx
        call_id = f"pre_{_idx}"
        _idx += 1
        print(f"  [collect] {name}({json.dumps(inputs) if inputs else ''})")
        try:
            result = dispatch[name](inputs)
        except Exception as e:
            print(f"  [collect]   ✗ {name} error: {e}")
            result = {"_error": str(e)}
        tool_use_blocks.append(
            {"type": "tool_use", "id": call_id, "name": name, "input": inputs}
        )
        # Features tool: collapse per-feature rows to one-liner summaries.
        # Everything else: trim lists to _max_rows (adaptive per db count).
        if name == "analyze_sql_features":
            payload = _compact_features(result)
        else:
            payload = _trim(result, _max_rows)
        tool_result_blocks.append(
            {"type": "tool_result", "tool_use_id": call_id,
             "content": json.dumps(payload, default=str)}
        )
        return result

    # 1. Discover all user databases
    db_data   = _run("list_databases", {})
    databases = [
        r["name"] for r in db_data.get("databases", [])
        if isinstance(r, dict) and r.get("name") and not r.get("_error")
    ]
    print(f"  [collect] {len(databases)} user database(s): {', '.join(databases)}")

    # Scale down rows-per-list so total payload stays under 200k tokens.
    # Budget: ~60 effective rows across all per-db calls; min 2 to keep errors visible.
    _max_rows = max(2, min(_TRIM_MAX_ROWS, 60 // max(1, len(databases))))

    # 2. Per-database tools — Python loop guarantees every DB is covered
    for db in databases:
        _run("analyze_schema",          {"database": db})
        _run("analyze_sql_features",    {"database": db})
        _run("analyze_procedural_code", {"database": db})

    # 3. Server-wide tools (run once)
    _run("analyze_jobs",           {})
    _run("analyze_performance",    {})
    _run("analyze_backups",        {})
    _run("analyze_linked_servers", {})
    _run("analyze_database_mail",  {})

    return tool_use_blocks, tool_result_blocks


# ─────────────────────────────────────────────────────────────────────────────
# AGENT LOOP
# ─────────────────────────────────────────────────────────────────────────────


def run_agent(ctx: ServerContext, out_dir: str) -> str:
    """
    Two-phase execution:
      Phase 1 — Python drives all data collection deterministically.
                 Every database in 01_databases.csv is analysed.
      Phase 2 — Claude synthesises the report.  execute_sql is the only
                 tool available; Claude uses it to retry any _error rows
                 with version/edition-correct queries before writing the
                 final report.
    """
    client   = anthropic.AnthropicBedrock(
        aws_access_key=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        aws_region=os.environ.get("AWS_REGION", "us-east-1"),
    )
    dispatch = build_dispatch(ctx, out_dir)
    system   = build_system_prompt(ctx)

    # ── Phase 1: collect all data ─────────────────────────────────────────────
    print("  [agent] Phase 1 — collecting data from all databases...")
    tool_use_blocks, tool_result_blocks = _collect_all_data(dispatch, ctx)

    # Inject collected results as a synthetic conversation turn so Claude
    # receives the same structured data it would have gathered itself.
    messages: list[dict] = [
        {
            "role": "user",
            "content": "Analyse this SQL Server instance and produce a full migration assessment report.",
        },
        {
            "role": "assistant",
            "content": tool_use_blocks,
        },
        {
            "role": "user",
            "content": tool_result_blocks,
        },
    ]

    # ── Phase 2: synthesis + DMV error recovery ───────────────────────────────
    # Expose only execute_sql so Claude can retry failed DMV queries with
    # version-appropriate rewrites.  All other data is already collected.
    recovery_tools = [t for t in build_tools() if t["name"] == "execute_sql"]

    print(f"  [agent] Phase 2 — synthesizing "
          f"({len(tool_use_blocks)} tool results; execute_sql available for error recovery)...")

    for turn in range(1, 25):
        if turn > 1:
            print(f"  [agent] Phase 2 turn {turn} (DMV error recovery)...")
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=8096,
            system=system,
            tools=recovery_tools,
            messages=messages,
        )
        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            for block in response.content:
                if hasattr(block, "text"):
                    return block.text
            return "(No text output)"

        # Handle execute_sql calls — the only tool Claude can call here
        tool_results = []
        for block in response.content:
            if block.type != "tool_use":
                continue
            inp = block.input or {}
            print(f"  [agent]   >> execute_sql  purpose={inp.get('purpose', '')[:60]}")
            try:
                result = dispatch["execute_sql"](inp)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result, default=str),
                })
            except Exception as e:
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps({"_error": str(e)}),
                    "is_error": True,
                })

        if not tool_results:
            for block in response.content:
                if hasattr(block, "text"):
                    return block.text
            return "(No text output)"

        messages.append({"role": "user", "content": tool_results})

    return "(Reached error-recovery turn limit)"


# ─────────────────────────────────────────────────────────────────────────────
# STATE MANAGER  – persists progress across runs
# ─────────────────────────────────────────────────────────────────────────────

class StateManager:
    """
    Persists per-server progress to <output_dir>/state.json.

    Server states:
      PENDING          – not yet started
      IN_PROGRESS      – started but not finished (crash / Ctrl-C)
      SUCCESS          – completed successfully  → skip on next run
      CONNECTION_FAILED – could not connect      → retry on next run by default
      AGENT_FAILED     – connected but agent error → retry on next run by default

    Flags:
      --force          reprocess ALL servers regardless of state
      --retry-failed   also reprocess CONNECTION_FAILED / AGENT_FAILED servers
                       (SUCCESS servers are still skipped unless --force is set)
    """

    SKIP_STATUSES = {"SUCCESS"}          # statuses skipped by default
    RETRY_STATUSES = {"CONNECTION_FAILED", "AGENT_FAILED", "IN_PROGRESS"}

    def __init__(self, state_path: str, force: bool = False,
                 retry_failed: bool = False) -> None:
        self.path         = state_path
        self.force        = force
        self.retry_failed = retry_failed
        self._data: dict  = self._load()

    # ── persistence ──────────────────────────────────────────────────────────

    def _load(self) -> dict:
        if os.path.exists(self.path):
            try:
                with open(self.path, encoding="utf-8") as f:
                    data = json.load(f)
                print(f"[state] Loaded checkpoint: {self.path}")
                return data
            except Exception as e:
                print(f"[state] Warning – could not read state file ({e}); starting fresh.")
        return {"created": _now(), "last_updated": _now(), "servers": {}}

    def _save(self) -> None:
        self._data["last_updated"] = _now()
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, default=str)

    # ── per-server accessors ──────────────────────────────────────────────────

    def get(self, label: str) -> dict:
        return self._data["servers"].get(label, {})

    def should_skip(self, label: str) -> bool:
        """Return True if the server should be skipped on this run."""
        if self.force:
            return False
        status = self.get(label).get("status", "PENDING")
        if status in self.SKIP_STATUSES:
            return True
        if status in self.RETRY_STATUSES and not self.retry_failed:
            # IN_PROGRESS means a previous run crashed mid-server → always retry
            return status != "IN_PROGRESS"
        return False

    def mark_in_progress(self, label: str, server: str, port: int) -> None:
        self._data["servers"][label] = {
            "status":     "IN_PROGRESS",
            "server":     server,
            "port":       port,
            "started_at": _now(),
            "attempt":    self.get(label).get("attempt", 0) + 1,
        }
        self._save()

    def mark_success(self, label: str, version: str, edition: str,
                     db_count: int, report_path: str) -> None:
        self._data["servers"][label].update({
            "status":       "SUCCESS",
            "completed_at": _now(),
            "version":      version,
            "edition":      edition,
            "db_count":     db_count,
            "report":       report_path,
        })
        self._save()

    def mark_failed(self, label: str, status: str, error: str,
                    version: str = "", edition: str = "") -> None:
        self._data["servers"][label].update({
            "status":    status,
            "failed_at": _now(),
            "error":     error,
            "version":   version,
            "edition":   edition,
        })
        self._save()

    # ── summary helpers ───────────────────────────────────────────────────────

    def print_summary(self, total: int) -> None:
        servers = self._data["servers"]
        counts  = {}
        for v in servers.values():
            s = v.get("status", "UNKNOWN")
            counts[s] = counts.get(s, 0) + 1
        pending = total - len(servers)
        if pending > 0:
            counts["PENDING"] = counts.get("PENDING", 0) + pending
        print("\n[state] Run summary:")
        for status, n in sorted(counts.items()):
            icon = {"SUCCESS": "✓", "IN_PROGRESS": "~", "PENDING": "·",
                    "CONNECTION_FAILED": "✗", "AGENT_FAILED": "✗"}.get(status, "?")
            print(f"  {icon}  {status:<22} {n}")

    def all_server_records(self) -> list[dict]:
        return list(self._data["servers"].values())


