"""
reports/merge_csvs.py  –  Merge per-database CSVs into single combined files.

Each per-database file (e.g. 09_proc_overview_MyDB.csv) is read and combined
into one file (09_proc_overview_all.csv).  Databases with no data still get a
placeholder row so every database is represented in every merged file.
"""
import csv
import os
from reports.csv_writer import read_csv, safe_slug


# (file_prefix, file_suffix, merged_basename, db_column_name)
# db_column_name is the column used to identify the source database.
# 03_features already has '_database'; all others get a new 'database' column.
_PATTERNS = [
    ("02_schema_", "_objects", "02_schema_objects", "database"),
    ("02_schema_", "_tables",  "02_schema_tables",  "database"),
    ("02_schema_", "_flags",   "02_schema_flags",   "database"),
    ("03_features_", "",       "03_features",       "_database"),
    ("09_proc_overview_", "",  "09_proc_overview",  "database"),
    ("09_proc_risks_", "",     "09_proc_risks",     "database"),
    ("09_proc_params_", "",    "09_proc_params",    "database"),
    ("09_clr_detail_", "",     "09_clr_detail",     "database"),
    ("09_proc_deps_", "",      "09_proc_deps",      "database"),
]


def _norm(v):
    if v is True:  return "true"
    if v is False: return "false"
    return v


def _write(path: str, rows: list[dict], first_col: str) -> None:
    """Write CSV with first_col guaranteed as the first column."""
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            f.write("(no data)\n")
        return
    seen: dict = {first_col: None}
    for row in rows:
        for k in row:
            seen[k] = None
    keys = list(seen.keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore", restval="")
        w.writeheader()
        for row in rows:
            w.writerow({k: _norm(v) for k, v in row.items()})
    print(f"    [merge] {os.path.basename(path)}  ({len(rows)} rows)")


def merge_per_db_csvs(out_dir: str, databases: list[str]) -> None:
    """
    For every per-database CSV family in _PATTERNS, read each database's file,
    tag rows with the database name if needed, append a placeholder row for
    databases with no data, and write a single combined *_all.csv file.
    """
    for prefix, suffix, merged_base, db_col in _PATTERNS:
        all_rows: list[dict] = []

        for db in databases:
            slug  = safe_slug(db)
            path  = os.path.join(out_dir, f"{prefix}{slug}{suffix}.csv")
            rows  = read_csv(path)

            if rows:
                tagged = []
                for row in rows:
                    if db_col not in row:
                        # Prepend db column so it sorts first in the output
                        new_row = {db_col: db}
                        new_row.update(row)
                        tagged.append(new_row)
                    else:
                        tagged.append(row)
                all_rows.extend(tagged)
            else:
                # No data for this database — one placeholder row, all attrs empty
                all_rows.append({db_col: db})

        out_path = os.path.join(out_dir, f"{merged_base}_all.csv")
        _write(out_path, all_rows, db_col)
