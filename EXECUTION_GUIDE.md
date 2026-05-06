# SQL Migration Analyst — Execution Guide

---

## 1. One-time setup

```bash
# Clone / unzip the project
unzip sql_migration_analyst.zip
cd sql_migration_project

# Create a virtual environment (recommended)
python -m venv .venv

# Activate it
# Windows
.venv\Scripts\activate
# Linux / macOS
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# ODBC Driver 17 must also be installed on the machine
# Windows  → https://aka.ms/downloadmsodbcsql
# RHEL/CentOS
sudo yum install msodbcsql17
# Ubuntu/Debian
sudo apt-get install msodbcsql17
```

---

## 2. Configure your API key

```bash
# Copy the example file
cp .env.example .env
```

Open `.env` and set:

```
ANTHROPIC_API_KEY=sk-ant-api03-YOUR_KEY_HERE
```

Get your key at → **https://console.anthropic.com/settings/api-keys**

> `.env` is in `.gitignore` — it will never be committed.

---

## 3. Configure your servers

Edit `inputs/servers.csv`:

```
server,port,username,password
SQLPROD01,1433,migration_analyst,StrongP@ssword123!
SQLPROD02\INST1,1433,migration_analyst,StrongP@ssword123!
10.0.0.5,1433,migration_analyst,StrongP@ssword123!
SQLDEV01,1433,,
```

> Leave `username` and `password` blank for Windows Authentication.

---

## 4. Create the analysis login on each SQL Server

Run `inputs/permissions.sql` on **each** source instance as sysadmin:

```bash
# Using sqlcmd
sqlcmd -S SQLPROD01 -E -i inputs/permissions.sql

# Or with SQL auth
sqlcmd -S SQLPROD01 -U sa -P "YourSAPassword" -i inputs/permissions.sql
```

---

## 5. Run commands

### Full run — T-SQL scan only  (default, no API cost for procedures)
```bash
python run.py --servers inputs/servers.csv
```

### Full run WITH Claude deep procedure analysis
```bash
python run.py --servers inputs/servers.csv --deep-analysis
```

> `--deep-analysis` triggers Tier-2: Claude reads actual SP/function source code for
> every object with a CRITICAL/HIGH risk flag or > 300 lines. Off by default because
> Tier-1 T-SQL scan already catches the vast majority of migration risks for free.

### Specify a custom output directory
```bash
python run.py --servers inputs/servers.csv --output-dir ./reports/april-2025
```

### Resume after a crash
```bash
# Just re-run the same command — completed servers are skipped automatically
python run.py --servers inputs/servers.csv
```

### Check what has been done (no processing)
```bash
python run.py --servers inputs/servers.csv --status
```

### Retry failed servers (CONNECTION_FAILED or AGENT_FAILED)
```bash
python run.py --servers inputs/servers.csv --retry-failed
```

### Retry failed AND resume any in-progress servers
```bash
python run.py --servers inputs/servers.csv --retry-failed
```

### Reprocess everything from scratch (ignore saved state)
```bash
python run.py --servers inputs/servers.csv --force
```

### Use a different output directory for a fresh run
```bash
python run.py --servers inputs/servers.csv --output-dir ./reports/run2 --force
```

---

## 6. Optional environment overrides

Set these in `.env` or export in your shell before running:

```bash
# Use a different Claude model
export CLAUDE_MODEL=claude-opus-4-20250514

# Increase max agent turns for large environments
export AGENT_MAX_TURNS=60

# Use ODBC Driver 18 instead of 17
export ODBC_DRIVER="ODBC Driver 18 for SQL Server"

# Increase connection timeout (seconds)
export CONN_TIMEOUT=60
```

---

## 7. Output locations

After a successful run, outputs are under `--output-dir` (default: `./outputs`):

```
outputs/
├── state.json                        ← checkpoint; never delete mid-run
├── 00_run_summary.csv                ← one row per server: status, version, edition, dbs
│
├── SQLPROD01_1433/
│   ├── 00_server_summary.csv         ← executive flat view, all findings + risk flags
│   ├── 01_databases.csv
│   ├── 02_schema_{db}_objects.csv
│   ├── 02_schema_{db}_tables.csv
│   ├── 02_schema_{db}_flags.csv
│   ├── 03_features_{db}.csv          ← 71 native SQL Server feature checks
│   ├── 04_jobs.csv
│   ├── 04_job_steps.csv
│   ├── 04_job_schedules.csv
│   ├── 05_perf_waits.csv
│   ├── 05_perf_missing_idx.csv
│   ├── 05_perf_memory.csv
│   ├── 06_backups.csv
│   ├── 07_linked_servers.csv
│   ├── 07_linked_server_usage.csv
│   ├── 08_database_mail.csv
│   ├── 09_proc_overview_{db}.csv     ← Tier-1: all objects, metrics, 19 risk flags
│   ├── 09_proc_risks_{db}.csv        ← Tier-1: one row per risk finding
│   ├── 09_proc_params_{db}.csv
│   ├── 09_clr_detail_{db}.csv
│   ├── 09_proc_deps_{db}.csv
│   ├── 09_proc_deep_{db}.csv         ← Tier-2: Claude code review (prioritised objects)
│   └── migration_report.md           ← Claude's full narrative assessment
│
└── SQLPROD02_INST1_1433/
    └── ...
```

---

## 8. --status output example

```
[state] Checkpoint : ./outputs/state.json
[state] Servers in CSV: 4

  ✓  SUCCESS                SQLPROD01:1433  done=2025-04-29 09:35  dbs=12
  ✗  CONNECTION_FAILED      SQLPROD02:1433  err=Login timeout expired
  ~  IN_PROGRESS            10.0.0.5:1433   started=2025-04-29 10:11  attempt=1
  ·  PENDING                SQLDEV01:1433
```

---

## 9. Common errors and fixes

| Error | Fix |
|---|---|
| `Missing required environment variable: ANTHROPIC_API_KEY` | Add key to `.env` or export in shell |
| `[Microsoft][ODBC Driver 17] Login failed` | Run `inputs/permissions.sql` on that server |
| `[Microsoft][ODBC Driver 17] TCP Provider: Connection timed out` | Check firewall, port 1433 open, server name correct |
| `Data source name not found` | ODBC Driver 17 not installed — see step 1 |
| `AGENT_FAILED: max_turns reached` | Increase `AGENT_MAX_TURNS=60` in `.env`, re-run with `--retry-failed` |
| `ModuleNotFoundError: No module named 'anthropic'` | `pip install -r requirements.txt` |

---

## 10. Tuning deep procedure analysis

Edit constants at the top of `tools/proc_analyzer.py`:

```python
SMALL_LINES    = 300    # objects at/below this go into batch mode
BATCH_SIZE     = 5      # small objects per Claude call
CHUNK_LINES    = 3_000  # lines per chunk for large objects
OVERLAP_LINES  = 80     # overlap lines between chunks
HEADER_LINES   = 60     # header lines prepended to every chunk
MAX_DEEP       = 50     # max objects deep-analysed per database (cost guard)
```

To analyse more objects per database, raise `MAX_DEEP`. To reduce cost, lower it.

---

## 11. Approximate runtime

| Servers | Databases each | Approx time |
|---|---|---|
| 1 | 5 | 8–15 min |
| 5 | 5 | 40–70 min |
| 10 | 10 | 2–3 hrs |
| 20 | 5 | 3–4 hrs |

Deep procedure analysis adds roughly 5–15 min per database depending on how many objects are prioritised. Use `--retry-failed` and the checkpoint system freely — runs are safe to interrupt and resume.
