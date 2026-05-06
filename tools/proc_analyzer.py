"""
tools/proc_analyzer.py  -  Claude-powered deep analysis of stored procedures,
                            functions, and CLR objects.

TWO-TIER STRATEGY
-----------------
Tier 1  (tools/procedural.py, already running):
  T-SQL pattern scan -> 09_proc_overview_{db}.csv, 09_proc_risks_{db}.csv
  Zero API calls. Covers ALL objects in seconds.

Tier 2  (this file):
  Claude reads actual source code for PRIORITISED objects only.
  Prioritised = CRITICAL/HIGH Tier-1 flag, OR > MIN_LINES_FOR_DEEP lines.

SIZING STRATEGY
---------------
<= SMALL_LINES  -> BATCH mode : up to BATCH_SIZE objects per Claude call.
>  SMALL_LINES  -> CHUNK mode : CHUNK_LINES windows with OVERLAP_LINES of
                                carry-forward context per chunk, then one
                                synthesis call to merge findings.

Example - 10 000-line SP:
  3 000-line chunks  ->  4 chunk calls + 1 synthesis call
  Each chunk: header (60 lines) + chunk body + rolling summary (~20k tokens)
  Total: ~5 API calls per massive SP.
"""

from __future__ import annotations
import json, os, re, textwrap
from typing import Any

import anthropic

from config.settings import CLAUDE_MODEL
from db.connection   import ServerContext
from db.query        import rq
from reports.csv_writer import write_csv, safe_slug

# ── Tunable constants ─────────────────────────────────────────────────────────
SMALL_LINES    = 300     # objects at or below this are batched
BATCH_SIZE     = 5       # small objects per Claude call
CHUNK_LINES    = 3_000   # source lines per chunk
OVERLAP_LINES  = 80      # lines carried between chunks for context
HEADER_LINES   = 60      # procedure header prepended to every chunk
MAX_DEEP       = 50      # hard cap on objects deep-analysed per database
MAX_RESULT_LEN = 2_000   # chars of source included in CSV output column

# Tier-1 risk flag columns that trigger deep analysis regardless of size
DEEP_TRIGGER_FLAGS = {
    "risk_xp_cmdshell", "risk_external_data", "risk_four_part_name",
    "risk_dynamic_exec", "risk_sp_executesql", "risk_cursor",
    "risk_global_temp_table", "risk_deprecated_join",
    "risk_undocumented_proc", "risk_waitfor", "risk_xp_other",
}

_client: anthropic.Anthropic | None = None

def _claude() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic()   # reads ANTHROPIC_API_KEY from env
    return _client


# ── Source fetching ────────────────────────────────────────────────────────────

def _fetch_source(ctx: ServerContext, database: str,
                  schema: str, name: str) -> str:
    rows = rq(ctx, """
        SELECT m.definition
        FROM   sys.sql_modules m
        JOIN   sys.objects     o ON m.object_id = o.object_id
        JOIN   sys.schemas     s ON o.schema_id = s.schema_id
        WHERE  s.name = ? AND o.name = ?
          AND  o.is_ms_shipped = 0;
    """.replace("?", "'{}'").format(schema, name), database)
    if rows and not rows[0].get("_error") and "definition" in rows[0]:
        return rows[0]["definition"] or ""
    return ""


def _clr_stub(ctx: ServerContext, database: str, schema: str, name: str) -> str:
    rows = rq(ctx, """
        SELECT a.name AS assembly_name, a.clr_name, a.permission_set_desc,
               am.assembly_class, am.assembly_method
        FROM   sys.assembly_modules am
        JOIN   sys.assemblies       a  ON am.assembly_id = a.assembly_id
        JOIN   sys.objects          o  ON am.object_id   = o.object_id
        JOIN   sys.schemas          s  ON o.schema_id    = s.schema_id
        WHERE  s.name = '{}' AND o.name = '{}';
    """.format(schema, name), database)
    if not rows or rows[0].get("_error"):
        return f"[CLR object — source not available in sys.sql_modules]"
    r = rows[0]
    return (
        f"-- CLR Object: [{schema}].[{name}]\n"
        f"-- Assembly  : {r.get('assembly_name')}  ({r.get('clr_name')})\n"
        f"-- Class     : {r.get('assembly_class')}.{r.get('assembly_method')}\n"
        f"-- Permission: {r.get('permission_set_desc')}\n"
        f"-- Source is compiled .NET — analyse migration requirements,\n"
        f"-- permission set risks, and whether functionality can be rewritten in T-SQL."
    )


# ── Prioritisation ─────────────────────────────────────────────────────────────

def _select_for_deep_analysis(overview_rows: list[dict]) -> list[dict]:
    """
    Return objects from the Tier-1 overview that warrant Claude deep analysis.
    Priority order:
      1. Any object with a CRITICAL/HIGH Tier-1 flag (regardless of size)
      2. Any object >= SMALL_LINES lines (complexity alone justifies it)
    Hard cap: MAX_DEEP objects total.
    """
    flagged, large, rest = [], [], []

    for obj in overview_rows:
        if obj.get("_error"):
            continue
        has_flag = any(str(obj.get(f, "0")) == "1" for f in DEEP_TRIGGER_FLAGS)
        lines    = int(obj.get("line_count") or 0)

        if has_flag:
            flagged.append(obj)
        elif lines >= SMALL_LINES:
            large.append(obj)
        else:
            rest.append(obj)

    # Sort each group by line count descending (biggest / riskiest first)
    flagged.sort(key=lambda x: int(x.get("line_count") or 0), reverse=True)
    large.sort(  key=lambda x: int(x.get("line_count") or 0), reverse=True)

    selected = (flagged + large)[:MAX_DEEP]
    print(f"    [deep] {len(selected)} objects selected for deep analysis "
          f"({len(flagged)} flagged, {len(large)} large)")
    return selected


# ── Chunking helpers ───────────────────────────────────────────────────────────

def _chunk_source(source: str) -> list[str]:
    """Split source into CHUNK_LINES windows with OVERLAP_LINES overlap."""
    lines  = source.splitlines(keepends=True)
    header = "".join(lines[:HEADER_LINES])
    chunks = []
    start  = 0

    while start < len(lines):
        end   = min(start + CHUNK_LINES, len(lines))
        body  = "".join(lines[start:end])
        chunks.append(header + body if start > 0 else body)
        if end >= len(lines):
            break
        start = end - OVERLAP_LINES   # carry-forward overlap

    return chunks


# ── System prompt (shared for all code-review calls) ─────────────────────────

_CODE_REVIEW_SYSTEM = textwrap.dedent("""
    You are a senior SQL Server DBA and migration specialist.
    Your task is to deeply analyse T-SQL or CLR stored procedure / function
    source code and return a structured JSON assessment.

    FOCUS ON:
    1. Business logic summary  (what does this object actually do?)
    2. Migration complexity    (Simple / Moderate / Complex / Very Complex)
    3. Risk findings BEYOND what basic pattern matching caught
       (e.g. subtle dynamic SQL built across multiple steps, hidden cursor logic,
       implicit conversions, undocumented behaviour, logic that assumes a
       specific collation or compatibility level)
    4. Specific rewrite recommendations for migration
    5. Whether the object can be migrated AS-IS or needs changes

    ALWAYS return valid JSON — no prose before or after.
    Schema for a single object:
    {
      "object_name": "string",
      "schema_name":  "string",
      "business_logic": "2-4 sentence plain-English description",
      "migration_complexity": "Simple|Moderate|Complex|Very Complex",
      "complexity_reason": "one sentence",
      "additional_risks": [
        {"risk": "short label", "severity": "Critical|High|Medium|Low",
         "detail": "one sentence", "line_hint": "approx line or range if known"}
      ],
      "rewrite_recommendations": ["..."],
      "migrate_as_is": true|false,
      "estimated_rewrite_hours": number_or_null,
      "notes": "any other migration-relevant observations"
    }
""").strip()


# ── Single-object analysis (chunk + synthesis) ────────────────────────────────

def _analyse_one_chunked(obj_name: str, schema_name: str, obj_type: str,
                          source: str, ctx: ServerContext) -> dict:
    """Analyse a large object by chunking its source."""
    chunks          = _chunk_source(source)
    total           = len(chunks)
    rolling_summary = ""

    print(f"      [chunk] {schema_name}.{obj_name}  "
          f"({source.count(chr(10))} lines → {total} chunks)")

    for i, chunk in enumerate(chunks, 1):
        is_last = (i == total)
        prompt  = textwrap.dedent(f"""
            You are reviewing [{schema_name}].[{obj_name}] ({obj_type}).
            SQL Server {ctx.year} ({ctx.edition}).

            This is chunk {i} of {total}.
            {"This is the FINAL chunk." if is_last else ""}

            {"--- FINDINGS FROM PREVIOUS CHUNKS ---\\n" + rolling_summary + "\\n" if rolling_summary else ""}
            --- SOURCE (chunk {i}/{total}) ---
            {chunk[:40_000]}

            {"Return the FULL final JSON object (schema in system prompt)." if is_last else
             "Return ONLY a JSON object with one key: 'partial_findings' (a list of risk strings found so far)."}
        """).strip()

        response = _claude().messages.create(
            model      = CLAUDE_MODEL,
            max_tokens = 2_048 if not is_last else 4_096,
            system     = _CODE_REVIEW_SYSTEM,
            messages   = [{"role": "user", "content": prompt}],
        )
        raw = _extract_json_text(response)

        if is_last:
            try:
                result = json.loads(raw)
                result.setdefault("object_name", obj_name)
                result.setdefault("schema_name",  schema_name)
                return result
            except json.JSONDecodeError:
                return _fallback(obj_name, schema_name, raw, "chunk synthesis JSON parse error")
        else:
            # Accumulate rolling summary
            try:
                partial = json.loads(raw)
                findings = partial.get("partial_findings", [])
                if findings:
                    rolling_summary += f"\nChunk {i}: " + "; ".join(str(f) for f in findings)
            except json.JSONDecodeError:
                rolling_summary += f"\nChunk {i}: (parse error — continuing)"

    return _fallback(obj_name, schema_name, "", "no final chunk produced")


def _analyse_batch(batch: list[dict], ctx: ServerContext,
                   database: str) -> list[dict]:
    """Analyse a batch of small objects in one Claude call."""
    items = []
    for obj in batch:
        schema = obj.get("schema_name", "dbo")
        name   = obj.get("object_name", "")
        otype  = obj.get("object_type", "")

        if "CLR" in otype.upper():
            source = _clr_stub(ctx, database, schema, name)
        else:
            source = _fetch_source(ctx, database, schema, name)

        if not source:
            source = f"-- Source not available for [{schema}].[{name}]"

        items.append({
            "schema":  schema,
            "name":    name,
            "type":    otype,
            "lines":   int(obj.get("line_count") or 0),
            "source":  source[:20_000],   # cap per object in batch
        })

    objects_json = json.dumps(
        [{"schema": x["schema"], "name": x["name"],
          "type": x["type"], "source": x["source"]} for x in items],
        ensure_ascii=False,
    )

    prompt = textwrap.dedent(f"""
        SQL Server {ctx.year} ({ctx.edition}).
        Analyse the following {len(items)} T-SQL object(s) for migration.

        Return a JSON ARRAY — one element per object — using the schema
        defined in the system prompt. Keep "business_logic" to 2 sentences.

        Objects:
        {objects_json[:60_000]}
    """).strip()

    response = _claude().messages.create(
        model      = CLAUDE_MODEL,
        max_tokens = 4_096,
        system     = _CODE_REVIEW_SYSTEM,
        messages   = [{"role": "user", "content": prompt}],
    )
    raw = _extract_json_text(response)

    try:
        results = json.loads(raw)
        if isinstance(results, dict):
            results = [results]
        return results
    except json.JSONDecodeError:
        return [_fallback(x["name"], x["schema"], raw, "batch JSON parse error")
                for x in items]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_json_text(response) -> str:
    """Pull the text content from a Claude response."""
    for block in response.content:
        if hasattr(block, "text"):
            text = block.text.strip()
            # Strip markdown fences if Claude added them
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$",          "", text)
            return text.strip()
    return "{}"


def _fallback(obj_name: str, schema_name: str, raw: str, reason: str) -> dict:
    return {
        "object_name":          obj_name,
        "schema_name":          schema_name,
        "business_logic":       f"[Analysis error: {reason}]",
        "migration_complexity": "Unknown",
        "complexity_reason":    reason,
        "additional_risks":     [],
        "rewrite_recommendations": [],
        "migrate_as_is":        None,
        "estimated_rewrite_hours": None,
        "notes":                raw[:500] if raw else "",
    }


def _flatten_result(r: dict, tier1: dict) -> dict:
    """Merge Claude findings with Tier-1 metadata into one CSV-ready row."""
    risks_str = "; ".join(
        f"[{x.get('severity','?')}] {x.get('risk','?')}: {x.get('detail','')}"
        for x in (r.get("additional_risks") or [])
    )
    rewrites_str = "; ".join(r.get("rewrite_recommendations") or [])
    return {
        # Identity
        "schema_name":          r.get("schema_name", tier1.get("schema_name","")),
        "object_name":          r.get("object_name", tier1.get("object_name","")),
        "object_type":          tier1.get("object_type",""),
        # Tier-1 metrics
        "line_count":           tier1.get("line_count",""),
        "param_count":          tier1.get("param_count",""),
        "dependency_count":     tier1.get("dependency_count",""),
        # Tier-2 Claude output
        "business_logic":       r.get("business_logic",""),
        "migration_complexity": r.get("migration_complexity",""),
        "complexity_reason":    r.get("complexity_reason",""),
        "migrate_as_is":        r.get("migrate_as_is",""),
        "estimated_rewrite_hours": r.get("estimated_rewrite_hours",""),
        "additional_risks":     risks_str,
        "rewrite_recommendations": rewrites_str,
        "notes":                r.get("notes",""),
        # Tier-1 key risk flags for easy filtering
        "t1_risk_xp_cmdshell":  tier1.get("risk_xp_cmdshell","0"),
        "t1_risk_external_data":tier1.get("risk_external_data","0"),
        "t1_risk_dynamic_exec": tier1.get("risk_dynamic_exec","0"),
        "t1_risk_cursor":       tier1.get("risk_cursor","0"),
        "t1_risk_deprecated_join": tier1.get("risk_deprecated_join","0"),
    }


# ── Main entry point ──────────────────────────────────────────────────────────

def tool_deep_analyze_procedures(ctx: ServerContext, database: str,
                                  out_dir: str) -> dict:
    """
    Claude-powered deep analysis of prioritised stored procedures and functions.

    Reads Tier-1 CSV (09_proc_overview_{db}.csv) to find candidates,
    fetches actual source from sys.sql_modules, analyses with Claude,
    and writes 09_proc_deep_{db}.csv.

    Returns a summary dict for the agent to include in the report.
    """
    slug      = safe_slug(database)
    overview_path = os.path.join(out_dir, f"09_proc_overview_{slug}.csv")

    # ── Load Tier-1 data ──────────────────────────────────────────────────────
    if not os.path.exists(overview_path):
        msg = f"Tier-1 overview CSV not found: {overview_path}. Run analyze_procedural_code first."
        print(f"    [deep] WARNING: {msg}")
        return {"_error": msg}

    import csv
    with open(overview_path, newline="", encoding="utf-8") as f:
        overview_rows = list(csv.DictReader(f))

    if not overview_rows:
        return {"database": database, "analysed": 0, "message": "No objects in Tier-1 data."}

    # ── Prioritise ────────────────────────────────────────────────────────────
    candidates = _select_for_deep_analysis(overview_rows)
    if not candidates:
        return {"database": database, "analysed": 0,
                "message": "No objects met the prioritisation threshold."}

    # Build lookup for Tier-1 metadata
    tier1_map = {
        (r.get("schema_name",""), r.get("object_name","")): r
        for r in overview_rows
    }

    # ── Split into small (batch) and large (chunk) ────────────────────────────
    small = [o for o in candidates if int(o.get("line_count") or 0) <= SMALL_LINES]
    large = [o for o in candidates if int(o.get("line_count") or 0) >  SMALL_LINES]

    all_results: list[dict] = []
    api_calls = 0

    # ── Process small objects in batches ──────────────────────────────────────
    for i in range(0, len(small), BATCH_SIZE):
        batch = small[i : i + BATCH_SIZE]
        names = ", ".join(o.get("object_name","") for o in batch)
        print(f"    [batch] {names}  ({len(batch)} objects)")
        try:
            results = _analyse_batch(batch, ctx, database)
            api_calls += 1
            for r in results:
                key  = (r.get("schema_name","dbo"), r.get("object_name",""))
                t1   = tier1_map.get(key, {})
                all_results.append(_flatten_result(r, t1))
        except Exception as e:
            print(f"    [batch] ERROR: {e}")
            for obj in batch:
                key = (obj.get("schema_name",""), obj.get("object_name",""))
                all_results.append(_flatten_result(
                    _fallback(obj.get("object_name",""), obj.get("schema_name",""),
                              "", str(e)),
                    tier1_map.get(key, obj)
                ))

    # ── Process large objects one at a time (chunked) ─────────────────────────
    for obj in large:
        schema = obj.get("schema_name", "dbo")
        name   = obj.get("object_name", "")
        otype  = obj.get("object_type", "")
        key    = (schema, name)

        print(f"    [large] {schema}.{name}  ({obj.get('line_count')} lines)")

        try:
            if "CLR" in otype.upper():
                source = _clr_stub(ctx, database, schema, name)
            else:
                source = _fetch_source(ctx, database, schema, name)

            if not source:
                source = f"-- Source not available for [{schema}].[{name}]"

            line_count = source.count("\n")

            if line_count <= CHUNK_LINES:
                # Fits in one call despite being "large" — analyse directly
                result = _analyse_batch([obj], ctx, database)
                api_calls += 1
                r = result[0] if result else _fallback(name, schema, "", "no result")
            else:
                r = _analyse_one_chunked(name, schema, otype, source, ctx)
                api_calls += (line_count // CHUNK_LINES) + 2  # approx

            all_results.append(_flatten_result(r, tier1_map.get(key, obj)))

        except Exception as e:
            print(f"    [large] ERROR {name}: {e}")
            all_results.append(_flatten_result(
                _fallback(name, schema, "", str(e)),
                tier1_map.get(key, obj)
            ))

    # ── Write output ──────────────────────────────────────────────────────────
    out_path = os.path.join(out_dir, f"09_proc_deep_{slug}.csv")
    write_csv(out_path, all_results)

    # ── Summary stats ─────────────────────────────────────────────────────────
    complexity_counts: dict[str, int] = {}
    for r in all_results:
        c = r.get("migration_complexity", "Unknown")
        complexity_counts[c] = complexity_counts.get(c, 0) + 1

    needs_rewrite = sum(1 for r in all_results
                        if str(r.get("migrate_as_is","")).lower() == "false")

    critical_objects = [
        r["object_name"] for r in all_results
        if any(f"[Critical]" in r.get("additional_risks","")
               or "[High]"   in r.get("additional_risks","")
               for _ in [1])
    ]

    return {
        "database":          database,
        "analysed":          len(all_results),
        "api_calls_used":    api_calls,
        "complexity_counts": complexity_counts,
        "objects_needing_rewrite": needs_rewrite,
        "critical_objects":  critical_objects[:10],
        "output_csv":        out_path,
    }
