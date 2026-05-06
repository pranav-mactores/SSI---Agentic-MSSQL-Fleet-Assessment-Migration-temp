#!/usr/bin/env python3
"""
run.py  –  Entry point for the SQL Server Migration Analyst Agent.

Usage
-----
  # First-time setup
  cp .env.example .env
  # Edit .env and set ANTHROPIC_API_KEY=sk-ant-...
  pip install -r requirements.txt

  # Run against all servers in servers.csv
  python run.py --servers inputs/servers.csv

  # Resume after a crash (skips completed servers automatically)
  python run.py --servers inputs/servers.csv

  # Check what has been done without running anything
  python run.py --servers inputs/servers.csv --status

  # Retry failed servers
  python run.py --servers inputs/servers.csv --retry-failed

  # Reprocess everything from scratch
  python run.py --servers inputs/servers.csv --force

API Key
-------
  The Anthropic API key is read from the ANTHROPIC_API_KEY environment variable.
  The easiest way to set it is via the .env file in this directory:

    ANTHROPIC_API_KEY=sk-ant-api03-XXXXXXXX

  You can also export it in your shell:
    export ANTHROPIC_API_KEY=sk-ant-api03-XXXXXXXX
"""

import argparse
import csv
import os
import textwrap
from datetime import datetime

# Validate API key is present before doing anything else
from config.settings import get_anthropic_api_key, AGENT_MAX_TURNS
get_anthropic_api_key()   # raises a clear error if missing

from db.connection import connect, detect_server
from agent.loop    import run_agent
from agent.state   import StateManager
from reports.csv_writer import write_csv, safe_slug
from reports.summary    import generate_server_summary_csv


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def process_server(row: dict, out_root: str,
                   state: StateManager, summary_rows: list[dict]) -> None:
    server   = row.get("server", "").strip()
    port     = int(row.get("port", 1433) or 1433)
    username = row.get("username", "").strip()
    password = row.get("password", "").strip()
    label    = safe_slug(f"{server}_{port}")

    print(f"\n{'='*60}")
    print(f"[server] {server}:{port}  (label: {label})")

    if state.should_skip(label):
        prev = state.get(label)
        print(f"  [skip] Already {prev.get('status')} at {prev.get('completed_at','')} — skipping.")
        summary_rows.append({
            "server": server, "port": port,
            "status":  prev.get("status"),
            "version": prev.get("version",""),
            "edition": prev.get("edition",""),
            "databases": prev.get("db_count", 0),
            "report":  prev.get("report",""),
            "error":   prev.get("error",""),
            "skipped": True,
        })
        return

    out_dir = os.path.join(out_root, label)
    os.makedirs(out_dir, exist_ok=True)
    state.mark_in_progress(label, server, port)

    try:
        conn = connect(server, port, username, password)
        ctx  = detect_server(conn, label, server, port)
    except Exception as e:
        print(f"  [ERROR] Connection failed: {e}")
        state.mark_failed(label, "CONNECTION_FAILED", str(e))
        summary_rows.append({
            "server": server, "port": port, "status": "CONNECTION_FAILED",
            "version": "", "edition": "", "databases": 0,
            "report": "", "error": str(e), "skipped": False,
        })
        write_csv(os.path.join(out_dir, "CONNECTION_FAILED.csv"),
                  [{"server": server, "port": port, "error": str(e), "timestamp": _now()}])
        return

    try:
        report_md = run_agent(ctx, out_dir, max_turns=AGENT_MAX_TURNS)

        header = textwrap.dedent(f"""
            # SQL Server Migration Feasibility Assessment
            Source    : {server}:{port}
            Version   : SQL Server {ctx.year} ({ctx.version_str})
            Edition   : {ctx.edition}
            Targets   : Amazon RDS SQL Server 2022 | Amazon RDS Custom SQL Server 2022 | Amazon Aurora PostgreSQL
            Generated : {_now()}
            ---
        """).lstrip()

        md_path = os.path.join(out_dir, "migration_report.md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(header + report_md)
        print(f"  [done] Narrative report → {md_path}")

        summary_csv = generate_server_summary_csv(ctx, out_dir, server, port)

        db_csv   = os.path.join(out_dir, "01_databases.csv")
        db_count = 0
        if os.path.exists(db_csv):
            with open(db_csv) as f:
                db_count = max(0, sum(1 for _ in f) - 1)

        state.mark_success(label, f"SQL {ctx.year}", ctx.edition, db_count, md_path)
        summary_rows.append({
            "server": server, "port": port, "status": "SUCCESS",
            "version": f"SQL {ctx.year}", "edition": ctx.edition,
            "databases": db_count, "report": md_path,
            "summary_csv": summary_csv, "error": "", "skipped": False,
        })

    except Exception as e:
        print(f"  [ERROR] Agent failed: {e}")
        state.mark_failed(label, "AGENT_FAILED", str(e),
                          version=f"SQL {ctx.year}", edition=ctx.edition)
        summary_rows.append({
            "server": server, "port": port, "status": "AGENT_FAILED",
            "version": f"SQL {ctx.year}", "edition": ctx.edition,
            "databases": 0, "report": "", "error": str(e), "skipped": False,
        })
    finally:
        try: conn.close()
        except Exception: pass


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python run.py",
        description="SQL Server Migration Analyst Agent — Multi-Server Edition",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
            API key setup:
              Copy .env.example → .env and set ANTHROPIC_API_KEY=sk-ant-...
              OR export ANTHROPIC_API_KEY=sk-ant-... in your shell.
        """),
    )
    parser.add_argument("--servers",      required=True,
                        help="CSV file: server,port,username,password")
    parser.add_argument("--output-dir",   default="./outputs",
                        help="Root output directory  (default: ./outputs)")
    parser.add_argument("--force",        action="store_true",
                        help="Reprocess ALL servers, ignoring saved state")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Retry CONNECTION_FAILED and AGENT_FAILED servers")
    parser.add_argument("--status",       action="store_true",
                        help="Print current checkpoint state and exit")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    state_path = os.path.join(args.output_dir, "state.json")
    state = StateManager(state_path, force=args.force, retry_failed=args.retry_failed)

    with open(args.servers, newline="", encoding="utf-8") as f:
        server_rows = list(csv.DictReader(f))

    if args.status:
        print(f"\n[state] Checkpoint : {state_path}")
        print(f"[state] Servers in CSV: {len(server_rows)}\n")
        state.print_status(server_rows, safe_slug)
        return

    print(f"[main] {len(server_rows)} server(s)  |  "
          f"force={args.force}  retry-failed={args.retry_failed}")

    summary_rows: list[dict] = []
    for row in server_rows:
        process_server(row, args.output_dir, state, summary_rows)

    summary_path = os.path.join(args.output_dir, "00_run_summary.csv")
    write_csv(summary_path, summary_rows)
    state.print_run_summary(len(server_rows))
    print(f"\n[main] Done. Summary → {summary_path}  |  State → {state_path}")


if __name__ == "__main__":
    main()
