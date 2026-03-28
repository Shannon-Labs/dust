from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class DustClientError(RuntimeError):
    pass


@dataclass
class DustClient:
    root: str | Path = "."
    binary: str = "dust"

    def _run(self, *args: str, json_output: bool = False) -> Any:
        command = [self.binary, *args]
        if json_output:
            command.extend(["--format", "json"])

        result = subprocess.run(
            command,
            cwd=Path(self.root),
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip()
            raise DustClientError(detail or f"command failed: {' '.join(command)}")

        stdout = result.stdout.strip()
        if json_output:
            return json.loads(stdout or "[]")
        return stdout

    def query(self, sql: str) -> list[dict[str, Any]]:
        return self._run("query", sql, json_output=True)

    def exec(self, sql: str) -> str:
        return self._run("query", sql)

    def status(self) -> str:
        return self._run("status")

    def branch_create(self, name: str) -> str:
        return self._run("branch", "create", name)

    def branch_switch(self, name: str) -> str:
        return self._run("branch", "switch", name)
