"""
agent/state.py  –  Checkpoint manager: skip completed servers, retry failures.

Server statuses
  PENDING           not yet started
  IN_PROGRESS       started but not finished (crash recovery – always retried)
  SUCCESS           completed OK              → skipped by default
  CONNECTION_FAILED could not connect         → skipped unless --retry-failed
  AGENT_FAILED      connected but agent error → skipped unless --retry-failed

CLI flags
  --force          reprocess everything
  --retry-failed   also retry CONNECTION_FAILED / AGENT_FAILED
"""
import json, os
from datetime import datetime

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

class StateManager:
    SKIP_BY_DEFAULT = {"SUCCESS"}
    RETRY_ON_FLAG   = {"CONNECTION_FAILED", "AGENT_FAILED"}

    def __init__(self, state_path: str, force: bool = False,
                 retry_failed: bool = False) -> None:
        self.path         = state_path
        self.force        = force
        self.retry_failed = retry_failed
        self._data        = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.path):
            try:
                with open(self.path, encoding="utf-8") as f:
                    data = json.load(f)
                print(f"[state] Loaded checkpoint: {self.path}")
                return data
            except Exception as e:
                print(f"[state] Warning – bad state file ({e}); starting fresh.")
        return {"created": _now(), "last_updated": _now(), "servers": {}}

    def _save(self) -> None:
        self._data["last_updated"] = _now()
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, default=str)

    def get(self, label: str) -> dict:
        return self._data["servers"].get(label, {})

    def should_skip(self, label: str) -> bool:
        if self.force:
            return False
        status = self.get(label).get("status", "PENDING")
        if status in self.SKIP_BY_DEFAULT:
            return True
        if status in self.RETRY_ON_FLAG:
            return not self.retry_failed
        # IN_PROGRESS = crashed mid-run, always retry
        return False

    def mark_in_progress(self, label: str, server: str, port: int) -> None:
        self._data["servers"][label] = {
            "status": "IN_PROGRESS", "server": server, "port": port,
            "started_at": _now(),
            "attempt": self.get(label).get("attempt", 0) + 1,
        }
        self._save()

    def mark_success(self, label: str, version: str, edition: str,
                     db_count: int, report_path: str) -> None:
        self._data["servers"][label].update({
            "status": "SUCCESS", "completed_at": _now(),
            "version": version, "edition": edition,
            "db_count": db_count, "report": report_path,
        })
        self._save()

    def mark_failed(self, label: str, status: str, error: str,
                    version: str = "", edition: str = "") -> None:
        self._data["servers"][label].update({
            "status": status, "failed_at": _now(),
            "error": error, "version": version, "edition": edition,
        })
        self._save()

    def print_status(self, server_rows: list[dict], safe_slug) -> None:
        """Print current state for --status mode."""
        for row in server_rows:
            server = row.get("server","").strip()
            port   = int(row.get("port", 1433) or 1433)
            label  = safe_slug(f"{server}_{port}")
            rec    = self.get(label)
            status = rec.get("status","PENDING")
            icon   = {"SUCCESS":"✓","IN_PROGRESS":"~","PENDING":"·",
                      "CONNECTION_FAILED":"✗","AGENT_FAILED":"✗"}.get(status,"?")
            extra  = ""
            if status == "SUCCESS":
                extra = f"  done={rec.get('completed_at','')}  dbs={rec.get('db_count','?')}"
            elif status in ("CONNECTION_FAILED","AGENT_FAILED"):
                extra = f"  err={rec.get('error','')[:60]}"
            elif status == "IN_PROGRESS":
                extra = f"  started={rec.get('started_at','')}  attempt={rec.get('attempt','?')}"
            print(f"  {icon}  {status:<22}  {server}:{port}{extra}")

    def print_run_summary(self, total: int) -> None:
        counts: dict[str,int] = {}
        for v in self._data["servers"].values():
            s = v.get("status","UNKNOWN")
            counts[s] = counts.get(s, 0) + 1
        pending = total - len(self._data["servers"])
        if pending > 0:
            counts["PENDING"] = counts.get("PENDING", 0) + pending
        print("\n[state] Run summary:")
        for status, n in sorted(counts.items()):
            icon = {"SUCCESS":"✓","IN_PROGRESS":"~","PENDING":"·",
                    "CONNECTION_FAILED":"✗","AGENT_FAILED":"✗"}.get(status,"?")
            print(f"  {icon}  {status:<22} {n}")
