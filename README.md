# SQL Server Migration Analyst Agent

An agentic AI tool that connects to one or more SQL Server instances, runs ~70 targeted discovery queries, and produces:
- A structured Markdown migration assessment report per server
- Individual CSV files per analysis task
- A flat `00_server_summary.csv` per server with all findings and risk flags
- A cross-server `00_run_summary.csv`
- A `state.json` checkpoint so interrupted runs resume automatically

Supports SQL Server 2014 → 2022, all editions (Enterprise, Standard, Web, Express, Developer).

---

## Quick Start

### 1. Prerequisites

```bash
# Python 3.11+
pip install -r requirements.txt

# ODBC Driver 17 for SQL Server must be installed
# Windows: https://aka.ms/downloadmsodbcsql
# Linux:   https://docs.microsoft.com/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server
```

### 2. Set your Anthropic API key

```bash
cp .env.example .env
```

Open `.env` and replace the placeholder:

```
ANTHROPIC_API_KEY=sk-ant-api03-REPLACE_WITH_YOUR_KEY_HERE
```

Get your key at **https://console.anthropic.com/settings/api-keys**

> **.env is in .gitignore — it will never be committed to version control.**

### 3. Configure your servers

Edit `inputs/servers.csv`:

```csv
server,port,username,password
SQLPROD01,1433,migration_analyst,StrongP@ssword123!
SQLPROD02\INST1,1433,migration_analyst,StrongP@ssword123!
SQLDEV01,1433,,
```

- Leave `username` and `password` blank for Windows Authentication
- The `migration_analyst` login must exist with the permissions in `inputs/permissions.sql`

### 4. Run

```bash
# Full run
python run.py --servers inputs/servers.csv

# Check progress without running
python run.py --servers inputs/servers.csv --status

# Resume after a crash (completed servers are skipped automatically)
python run.py --servers inputs/servers.csv

# Retry failed servers
python run.py --servers inputs/servers.csv --retry-failed

# Reprocess everything from scratch
python run.py --servers inputs/servers.csv --force
```

---

## Output Structure

```
outputs/
├── state.json                      ← checkpoint; auto-updated after each server
├── 00_run_summary.csv              ← one row per server across all runs
│
├── SQLPROD01_1433/
│   ├── 00_server_summary.csv       ← executive flat view of all findings + risk flags
│   ├── 01_databases.csv            ← database inventory with sizes
│   ├── 02_schema_{db}_objects.csv  ← object counts per database
│   ├── 02_schema_{db}_tables.csv   ← all tables with row counts
│   ├── 02_schema_{db}_flags.csv    ← migration risk flags
│   ├── 03_features_{db}.csv        ← all 71 native SQL Server features
│   ├── 04_jobs.csv                 ← SQL Agent jobs
│   ├── 04_job_steps.csv
│   ├── 04_job_schedules.csv
│   ├── 05_perf_waits.csv           ← top wait statistics
│   ├── 05_perf_missing_idx.csv     ← missing index recommendations
│   ├── 05_perf_memory.csv
│   ├── 06_backups.csv              ← backup history (30 days)
│   ├── 07_linked_servers.csv
│   ├── 07_linked_server_usage.csv
│   ├── 08_database_mail.csv
│   ├── 09_proc_overview_{db}.csv   ← per-object complexity metrics
│   ├── 09_proc_risks_{db}.csv      ← risk findings with severity
│   ├── 09_proc_params_{db}.csv     ← procedure/function parameters
│   ├── 09_clr_detail_{db}.csv      ← CLR assembly → object mapping
│   ├── 09_proc_deps_{db}.csv       ← object dependency graph
│   └── migration_report.md         ← Claude's narrative assessment
│
└── SQLPROD02_INST1_1433/
    └── ...
```

---

## Project Structure

```
sql-migration-analyst/
├── run.py                    ← entry point (python run.py --help)
├── requirements.txt
├── .env.example              ← copy to .env, add your API key
├── .env                      ← YOUR SECRETS (gitignored)
├── .gitignore
│
├── config/
│   └── settings.py           ← loads .env, exposes CLAUDE_MODEL etc.
│
├── db/
│   ├── connection.py         ← ServerContext, connect(), detect_server()
│   └── query.py              ← rq(), na_row()
│
├── agent/
│   ├── loop.py               ← Claude tool-use loop, system prompt
│   └── state.py              ← StateManager (checkpoint / resume)
│
├── tools/
│   ├── schema.py             ← list_databases, analyze_schema
│   ├── features.py           ← analyze_sql_features (71 feature keys)
│   ├── procedural.py         ← analyze_procedural_code
│   ├── jobs.py               ← analyze_jobs
│   ├── performance.py        ← analyze_performance
│   ├── backups.py            ← analyze_backups
│   ├── linked_servers.py     ← analyze_linked_servers
│   └── mail.py               ← analyze_database_mail
│
├── reports/
│   ├── csv_writer.py         ← write_csv, read_csv, helpers
│   └── summary.py            ← generate_server_summary_csv
│
└── inputs/
    ├── servers.csv           ← your server list
    └── permissions.sql       ← least-privilege setup script
```

---

## Where is the API key used?

```
.env  ──(loaded by)──▶  config/settings.py  ──(os.environ)──▶  anthropic.Anthropic()
                                                                    (in agent/loop.py)
```

The Anthropic SDK reads `ANTHROPIC_API_KEY` from the environment automatically. You never pass it as a code argument. `config/settings.py` validates it is present at startup and raises a clear error if missing.

---

## Required SQL Server permissions

Run `inputs/permissions.sql` on each source instance before analysing it. The script grants least-privilege access: `VIEW SERVER STATE`, `VIEW ANY DATABASE`, `VIEW ANY DEFINITION`, `SELECT ALL USER SECURABLES`, plus targeted grants on `msdb`.
