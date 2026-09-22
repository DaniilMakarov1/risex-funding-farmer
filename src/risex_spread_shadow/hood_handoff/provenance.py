"""Local implementation identity; called before market preparation, no credentials."""
from __future__ import annotations

from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping


@lru_cache(maxsize=1)
def _implementation() -> dict[str, Any]:
    package = Path(__file__).resolve().parent
    files = [{"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
             for path in sorted(package.glob("*.py"))]
    git_head = None
    dirty = None
    try:
        git_head = subprocess.check_output(["git", "-C", str(package), "rev-parse", "HEAD"],
                                           stderr=subprocess.DEVNULL, timeout=1, text=True).strip()
        dirty = bool(subprocess.check_output(["git", "-C", str(package), "status", "--porcelain",
                                             "--untracked-files=no", "--", "."],
                                            stderr=subprocess.DEVNULL, timeout=1, text=True).strip())
    except (OSError, subprocess.SubprocessError):
        pass
    return {"git_head": git_head, "dirty": dirty, "files": files}


def capture_provenance(binding: Mapping[str, Any]) -> dict[str, Any]:
    # Binding is an allowlisted execution configuration, never a raw credential
    # store, signer, prepared mutation, environment or arbitrary SDK response.
    configuration = json.dumps(dict(binding), sort_keys=True, separators=(",", ":"), default=str).encode()
    imports = [{"module": name, "path": str(Path(module.__file__).resolve())}
               for name, module in sorted(tuple(sys.modules.items()))
               if name.startswith("risex_spread_shadow.hood_handoff")
               and getattr(module, "__file__", None)]
    declared_interface = os.environ.get('RISEX_HOOD_OPERATOR_INTERFACE')
    interface = declared_interface if declared_interface in {'telegram', 'terminal'} else 'unknown'
    return {**_implementation(), "configuration_sha256": hashlib.sha256(configuration).hexdigest(),
            "operator_interface": interface,  # Diagnostic label only, never an authority or execution input.
            "imports": imports, "python_version": sys.version.split()[0]}
