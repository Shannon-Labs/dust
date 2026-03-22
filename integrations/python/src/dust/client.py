"""DustDB client — thin Python wrapper around the dust CLI for AI agents."""

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class DustResult:
    """Result from a dust query."""

    columns: list[str]
    rows: list[list]
    raw: str

    @classmethod
    def from_json(cls, raw: str) -> "DustResult":
        try:
            data = json.loads(raw)
            if isinstance(data, list) and data:
                columns = list(data[0].keys())
                rows = [list(row.values()) for row in data]
                return cls(columns=columns, rows=rows, raw=raw)
            return cls(columns=[], rows=[], raw=raw)
        except (json.JSONDecodeError, AttributeError):
            return cls(columns=[], rows=[], raw=raw)

    def to_dicts(self) -> list[dict]:
        return [dict(zip(self.columns, row)) for row in self.rows]

    def __repr__(self) -> str:
        return f"DustResult({len(self.rows)} rows, {len(self.columns)} columns)"


class DustDB:
    """Python client for the Dust branchable SQL database.

    Wraps the dust CLI binary. For MCP integration, use the dust-mcp
    server directly via JSON-RPC.

    Usage:
        db = DustDB("./my-project")
        db.exec("CREATE TABLE items (id INTEGER, name TEXT)")
        db.exec("INSERT INTO items VALUES (1, 'hello')")
        result = db.query("SELECT * FROM items")
        print(result.to_dicts())

        # Branching
        db.branch_create("scratch/test")
        db.branch_switch("scratch/test")
        db.query("INSERT INTO items VALUES (2, 'branch-only')")
        db.branch_switch("main")
    """

    def __init__(
        self,
        path: str | Path = ".",
        dust_binary: str = "dust",
    ):
        self.path = Path(path)
        self.dust_binary = dust_binary

    def _run(self, *args: str) -> str:
        """Run a dust CLI command and return stdout."""
        result = subprocess.run(
            [self.dust_binary, *args],
            capture_output=True,
            text=True,
            cwd=str(self.path),
        )
        if result.returncode != 0:
            raise RuntimeError(f"dust command failed: {result.stderr.strip()}")
        return result.stdout.strip()

    def query(self, sql: str, fmt: str = "json") -> DustResult:
        """Execute a SQL query and return results."""
        raw = self._run("query", "--format", fmt, sql)
        return DustResult.from_json(raw)

    def exec(self, sql: str) -> str:
        """Execute a SQL statement that modifies the database."""
        return self._run("query", sql)

    def import_file(
        self,
        file: str | Path,
        table: Optional[str] = None,
    ) -> str:
        """Import a data file (CSV, JSON, JSONL, XLSX, Parquet, SQL)."""
        args = ["import", str(file)]
        if table:
            args.extend(["--table", table])
        return self._run(*args)

    def schema(self, table: Optional[str] = None) -> str:
        """Get schema for a table or all tables."""
        args = ["query", ".schema"]
        if table:
            args.append(table)
        return self._run(*args)

    def status(self) -> dict:
        """Get project status as a dict."""
        raw = self._run("status", "--format", "json")
        return json.loads(raw) if raw else {}

    def branch_create(self, name: str) -> str:
        """Create a new branch."""
        return self._run("branch", "create", name)

    def branch_switch(self, name: str) -> str:
        """Switch to a branch."""
        return self._run("branch", "switch", name)

    def branch_list(self) -> list[str]:
        """List all branches."""
        raw = self._run("branch", "list")
        return [line.strip() for line in raw.splitlines() if line.strip()]

    def branch_diff(
        self,
        from_branch: str = "main",
        to_branch: Optional[str] = None,
    ) -> str:
        """Compare two branches."""
        args = ["branch", "diff", from_branch]
        if to_branch:
            args.append(to_branch)
        return self._run(*args)
