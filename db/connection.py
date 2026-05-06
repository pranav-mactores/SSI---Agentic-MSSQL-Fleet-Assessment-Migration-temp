"""
db/connection.py  –  ServerContext dataclass, connection factory, version detection.
"""
import pyodbc
from dataclasses import dataclass
from config.settings import ODBC_DRIVER, CONN_TIMEOUT

VERSION_YEAR = {12: 2014, 13: 2016, 14: 2017, 15: 2019, 16: 2022}

@dataclass
class ServerContext:
    label:        str
    server:       str
    port:         int
    conn:         pyodbc.Connection
    version_int:  int  = 0
    version_str:  str  = ""
    year:         int  = 0
    edition:      str  = ""
    is_enterprise: bool = False
    is_express:    bool = False

    def v(self, minimum: int) -> bool:
        return self.version_int >= minimum

    def ed(self, *editions: str) -> bool:
        low = self.edition.lower()
        return any(e.lower() in low for e in editions)

    def has_agent(self) -> bool:
        return not self.is_express

    def has_feature(self, min_ver: int, *req_editions: str) -> bool:
        if not self.v(min_ver):
            return False
        if req_editions and not self.ed(*req_editions):
            return False
        return True


def connect(server: str, port: int, username: str, password: str) -> pyodbc.Connection:
    # Named instances (containing \) rely on SQL Server Browser for port resolution;
    # appending ,port forces a specific TCP port and breaks that lookup.
    server_part = server if "\\" in server else f"{server},{port}"
    base = f"DRIVER={{{ODBC_DRIVER}}};SERVER={server_part};DATABASE=master;TrustServerCertificate=yes;"
    if username and password:
        cs = base + f"UID={username};PWD={password};"
    else:
        cs = base + "Trusted_Connection=yes;"
    return pyodbc.connect(cs, autocommit=True, timeout=CONN_TIMEOUT)


def detect_server(conn: pyodbc.Connection, label: str,
                  server: str, port: int) -> ServerContext:
    cur = conn.cursor()
    cur.execute("""
        SELECT
            CAST(SERVERPROPERTY('ProductMajorVersion') AS INT)       AS maj,
            CAST(SERVERPROPERTY('ProductVersion')      AS NVARCHAR(50)) AS ver,
            CAST(SERVERPROPERTY('Edition')             AS NVARCHAR(100)) AS edition;
    """)
    row = cur.fetchone()
    maj  = int(row.maj or 12)
    ver  = str(row.ver or "12.0")
    ed   = str(row.edition or "Unknown")
    ctx = ServerContext(
        label       = label,
        server      = server,
        port        = port,
        conn        = conn,
        version_int = maj,
        version_str = ver,
        year        = VERSION_YEAR.get(maj, maj * 100),
        edition     = ed,
        is_enterprise = "enterprise" in ed.lower() or "developer" in ed.lower(),
        is_express    = "express"    in ed.lower(),
    )
    print(f"  [detect] SQL Server {ctx.year} ({ver}) – {ed}")
    return ctx
