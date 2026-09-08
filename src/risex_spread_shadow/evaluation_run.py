"""Durable lifecycle for one bounded offline evaluation process.

This is intentionally a small process envelope for the saved-input S1b
calculation.  It is not a service, watcher, scheduler, or recovery framework.
The child remains a normal foreground subprocess.  The envelope's durable
manifest is the authoritative run record: a missing terminal marker is an
incomplete run with an unknown cause, never a successful zero-result run.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
from typing import Any


RUN_SCHEMA = "SCV1_S1B_OFFLINE_EVALUATION_RUN_V1"
RUN_STOP = "RUN_STOP"
RUN_FAILED = "RUN_FAILED"
_STARTING = "STARTING"
_RUNNING = "RUNNING"
_COMPLETE = "COMPLETE"
_FAILED = "FAILED"
_SHA256_RE = re.compile(r"^[0-9a-f]{40}$")
_HASH_CHUNK_BYTES = 1024 * 1024


class EvaluationRunError(RuntimeError):
    """Base error for invalid or duplicate bounded evaluation runs."""


class DuplicateEvaluationRun(EvaluationRunError):
    """Raised when a run directory is not fresh and therefore cannot relaunch."""


class EvaluationRunContractError(EvaluationRunError):
    """Raised when the durable run envelope is malformed or cannot be bound."""


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _json_safe(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value, sort_keys=True, separators=(",", ":")))
    except (TypeError, ValueError) as exc:
        raise EvaluationRunContractError("contract metadata must be JSON serializable") from exc


def file_identity(path: Path) -> dict[str, Any]:
    """Return a streaming path/size/SHA-256 identity without loading the file."""

    path = path.resolve()
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as stream:
            while True:
                chunk = stream.read(_HASH_CHUNK_BYTES)
                if not chunk:
                    break
                digest.update(chunk)
                size += len(chunk)
    except OSError as exc:
        raise EvaluationRunContractError(f"cannot fingerprint {path}") from exc
    return {"path": str(path), "bytes": size, "sha256": digest.hexdigest()}


def _git_head(source_root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=source_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise EvaluationRunContractError("source Git identity is unavailable") from exc
    value = result.stdout.strip()
    if not _SHA256_RE.fullmatch(value):
        raise EvaluationRunContractError("source Git identity is not a full commit SHA")
    return value


def _git_clean(source_root: Path) -> bool:
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=source_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise EvaluationRunContractError("source Git cleanliness is unavailable") from exc
    return not result.stdout.strip()


def _source_snapshot(source_root: Path) -> dict[str, Any]:
    """Capture one checkout state; the terminal copy is historical evidence."""

    head: str | None = None
    clean: bool | None = None
    try:
        head = _git_head(source_root)
        clean = _git_clean(source_root)
    except EvaluationRunContractError:
        pass
    return {"head": head, "clean": clean}


def _source_binding_is_valid(observation: Any, source_sha: str) -> bool:
    return (
        isinstance(observation, dict)
        and observation.get("head") == source_sha
        and observation.get("clean") is True
    )


def _result_contract(path: Path) -> dict[str, Any]:
    """Validate the small result envelope required for an authoritative stop."""

    try:
        if path.stat().st_size == 0:
            return {"valid": False, "reason": "EMPTY"}
        with path.open(encoding="utf-8") as stream:
            value = json.load(stream)
    except FileNotFoundError:
        return {"valid": False, "reason": "MISSING"}
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {"valid": False, "reason": "MALFORMED_JSON"}
    if not isinstance(value, dict):
        return {"valid": False, "reason": "JSON_OBJECT_REQUIRED"}
    status = value.get("status")
    if status == "COMPLETE":
        return {"valid": True, "status": "COMPLETE"}
    if status == "INCOMPLETE":
        return {"valid": False, "status": "INCOMPLETE", "reason": "EXPLICIT_INCOMPLETE"}
    if "status" not in value:
        return {"valid": False, "reason": "STATUS_MISSING"}
    return {
        "valid": False,
        "status": status if isinstance(status, str) else None,
        "reason": "STATUS_NOT_COMPLETE",
    }


def _relative_artifact(root: Path, value: str, field_name: str) -> Path:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise EvaluationRunContractError(f"{field_name} must be a relative path")
    path = Path(value)
    if ".." in path.parts or str(path) in {".", ""}:
        raise EvaluationRunContractError(f"{field_name} must stay inside the run directory")
    candidate = (root / path).resolve()
    if not candidate.is_relative_to(root.resolve()):
        raise EvaluationRunContractError(f"{field_name} must stay inside the run directory")
    return candidate


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _write_json_exclusive(path: Path, value: Mapping[str, Any]) -> None:
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise DuplicateEvaluationRun(f"durable run claim already exists: {path}") from exc
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        try:
            path.unlink()
        except OSError:
            pass
        raise
    _fsync_directory(path.parent)


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    temporary = path.with_name(
        f".{path.name}.tmp-{os.getpid()}-{time.monotonic_ns()}"
    )
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except BaseException:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise


def _read_manifest(root: Path) -> dict[str, Any]:
    path = root / "run.json"
    try:
        with path.open(encoding="utf-8") as stream:
            value = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise EvaluationRunContractError(f"cannot read durable run manifest: {path}") from exc
    if not isinstance(value, dict) or value.get("schema") != RUN_SCHEMA:
        raise EvaluationRunContractError("durable run manifest schema mismatch")
    return value


def _artifact_identity(root: Path, relative: str) -> dict[str, Any]:
    path = _relative_artifact(root, relative, "artifact")
    if not path.is_file():
        return {"path": str(path), "present": False}
    return {"present": True, **file_identity(path)}


def _signal_name(returncode: int) -> str | None:
    if returncode >= 0:
        return None
    try:
        return signal.Signals(-returncode).name
    except ValueError:
        return f"SIGNAL_{-returncode}"


@dataclass(slots=True)
class EvaluationRun:
    """A claimed, single-use, bounded offline child-process run."""

    root: Path
    command: tuple[str, ...]
    cwd: Path
    expected_result: str

    @property
    def manifest_path(self) -> Path:
        return self.root / "run.json"

    @property
    def log_path(self) -> Path:
        return self.root / "run.log"

    def _update(self, manifest: Mapping[str, Any]) -> None:
        _write_json_atomic(self.manifest_path, manifest)

    def _terminal(
        self,
        manifest: dict[str, Any],
        *,
        status: str,
        kind: str,
        failure_class: str | None,
        returncode: int | None,
        result: dict[str, Any] | None,
        result_contract: dict[str, Any] | None,
        source_observation: dict[str, Any],
    ) -> dict[str, Any]:
        child = dict(manifest["child"])
        child["returncode"] = returncode
        child["finished_utc"] = _utc_now()
        terminal = {
            "kind": kind,
            "status": status,
            "failure_class": failure_class,
            "child_returncode": returncode,
            "child_signal": None if returncode is None else _signal_name(returncode),
            "observed_utc": _utc_now(),
            "observed_monotonic_ns": time.monotonic_ns(),
            "source": source_observation,
        }
        if result is not None:
            terminal["result"] = result
        if result_contract is not None:
            terminal["result_contract"] = result_contract
        manifest = dict(manifest)
        manifest["status"] = status
        manifest["child"] = child
        manifest["terminal"] = terminal
        self._update(manifest)
        return read_evaluation_run(self.root)

    def execute(self) -> dict[str, Any]:
        """Run the child once and persist a terminal state when observable."""

        manifest = _read_manifest(self.root)
        if manifest.get("status") != _STARTING or manifest.get("terminal") is not None:
            raise DuplicateEvaluationRun("a claimed evaluation run cannot be resumed or relaunched")

        try:
            fd = os.open(
                self.log_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            log = os.fdopen(fd, "w", encoding="utf-8")
        except FileExistsError as exc:
            raise DuplicateEvaluationRun(f"run log already exists: {self.log_path}") from exc

        child: subprocess.Popen[bytes] | None = None
        returncode: int | None = None
        try:
            source_record = manifest.get("source")
            if not isinstance(source_record, dict):
                raise EvaluationRunContractError("durable source binding is missing")
            source_root_value = source_record.get("root")
            source_sha = source_record.get("sha")
            if not isinstance(source_root_value, str) or not isinstance(source_sha, str):
                raise EvaluationRunContractError("durable source binding is invalid")
            source_root = Path(source_root_value).resolve()
            source_at_execution = _source_snapshot(source_root)
            if not _source_binding_is_valid(source_at_execution, source_sha):
                log.write("SOURCE_CHANGED_BEFORE_EXECUTION\n")
                log.flush()
                os.fsync(log.fileno())
                return self._terminal(
                    manifest,
                    status=_FAILED,
                    kind=RUN_FAILED,
                    failure_class="SOURCE_CHANGED_BEFORE_EXECUTION",
                    returncode=None,
                    result=None,
                    result_contract=None,
                    source_observation=source_at_execution,
                )

            manifest = dict(manifest)
            source_record = dict(manifest["source"])
            source_record["execution"] = source_at_execution
            manifest["source"] = source_record
            manifest["status"] = _RUNNING
            self._update(manifest)
            try:
                child = subprocess.Popen(
                    list(self.command),
                    cwd=self.cwd,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    close_fds=True,
                )
            except OSError as exc:
                log.write(f"LAUNCH_FAILED {exc!r}\n")
                log.flush()
                os.fsync(log.fileno())
                return self._terminal(
                    manifest,
                    status=_FAILED,
                    kind=RUN_FAILED,
                    failure_class="LAUNCH_FAILED",
                    returncode=None,
                    result=None,
                    result_contract=None,
                    source_observation=source_at_execution,
                )

            manifest = dict(manifest)
            child_record = dict(manifest["child"])
            child_record["pid"] = child.pid
            manifest["child"] = child_record
            try:
                self._update(manifest)
            except BaseException:
                child.terminate()
                try:
                    child.wait(timeout=2)
                except (subprocess.TimeoutExpired, OSError):
                    pass
                raise
            returncode = child.wait()
        finally:
            log.flush()
            try:
                os.fsync(log.fileno())
            except OSError:
                pass
            log.close()

        if returncode is None:
            raise EvaluationRunContractError("child return code was not observed")

        source_at_terminal = _source_snapshot(source_root)
        result_path = _relative_artifact(self.root, self.expected_result, "artifact")
        if result_path.is_file():
            try:
                os.chmod(result_path, 0o600)
            except OSError:
                pass
        result_identity = _artifact_identity(self.root, self.expected_result)

        if not _source_binding_is_valid(source_at_terminal, source_sha):
            return self._terminal(
                manifest,
                status=_FAILED,
                kind=RUN_FAILED,
                failure_class="SOURCE_CHANGED_DURING_EXECUTION",
                returncode=returncode,
                result=result_identity,
                result_contract=None,
                source_observation=source_at_terminal,
            )

        if returncode != 0:
            failure_class = "CHILD_SIGNALLED" if returncode < 0 else "CHILD_NONZERO"
            return self._terminal(
                manifest,
                status=_FAILED,
                kind=RUN_FAILED,
                failure_class=failure_class,
                returncode=returncode,
                result=result_identity if result_identity["present"] else None,
                result_contract=None,
                source_observation=source_at_terminal,
            )

        if not result_identity["present"]:
            return self._terminal(
                manifest,
                status=_FAILED,
                kind=RUN_FAILED,
                failure_class="RESULT_ARTIFACT_MISSING",
                returncode=returncode,
                result=result_identity,
                result_contract={"valid": False, "reason": "MISSING"},
                source_observation=source_at_terminal,
            )
        result_contract = _result_contract(result_path)
        if not result_contract["valid"]:
            failure_class = (
                "RESULT_ARTIFACT_INCOMPLETE"
                if result_contract.get("reason") == "EXPLICIT_INCOMPLETE"
                else "RESULT_ARTIFACT_INVALID"
            )
            return self._terminal(
                manifest,
                status=_FAILED,
                kind=RUN_FAILED,
                failure_class=failure_class,
                returncode=returncode,
                result=result_identity,
                result_contract=result_contract,
                source_observation=source_at_terminal,
            )
        return self._terminal(
            manifest,
            status=_COMPLETE,
            kind=RUN_STOP,
            failure_class=None,
            returncode=returncode,
            result=result_identity,
            result_contract=result_contract,
            source_observation=source_at_terminal,
        )


def prepare_evaluation_run(
    root: Path,
    *,
    run_id: str,
    owner: str,
    source_root: Path,
    source_sha: str,
    input_path: Path,
    command: Sequence[str],
    cwd: Path | None = None,
    expected_result: str = "results.json",
    contract: Mapping[str, Any] | None = None,
) -> EvaluationRun:
    """Claim one fresh run and bind its source/input before launching a child."""

    root = root.resolve()
    source_root = source_root.resolve()
    input_path = input_path.resolve()
    cwd = (cwd or root).resolve()
    if not isinstance(run_id, str) or not run_id or "/" in run_id or "\\" in run_id:
        raise EvaluationRunContractError("run_id must be a non-empty path-safe value")
    if not isinstance(owner, str) or not owner:
        raise EvaluationRunContractError("owner must be non-empty")
    if not isinstance(source_sha, str) or _SHA256_RE.fullmatch(source_sha) is None:
        raise EvaluationRunContractError("source_sha must be a full Git commit SHA")
    if (
        not source_root.is_dir()
        or not input_path.is_file()
        or (cwd != root and not cwd.is_dir())
    ):
        raise EvaluationRunContractError("source root, run cwd, and input file must exist")
    actual_source = _git_head(source_root)
    if actual_source != source_sha:
        raise EvaluationRunContractError("source Git identity does not match requested SHA")
    if not _git_clean(source_root):
        raise EvaluationRunContractError("source checkout must be clean before launch")
    command_tuple = tuple(command)
    if not command_tuple or any(not isinstance(item, str) or not item for item in command_tuple):
        raise EvaluationRunContractError("command must be a non-empty argv sequence")
    _relative_artifact(root, expected_result, "expected_result")
    if expected_result in {"run.json", "run.log"}:
        raise EvaluationRunContractError("expected_result cannot be a lifecycle artifact")
    input_identity = file_identity(input_path)
    contract_value = {} if contract is None else _json_safe(contract)
    try:
        root.mkdir(mode=0o700, parents=True, exist_ok=False)
    except FileExistsError as exc:
        raise DuplicateEvaluationRun(f"fresh run directory required: {root}") from exc
    manifest = {
        "schema": RUN_SCHEMA,
        "run_id": run_id,
        "owner": owner,
        "status": _STARTING,
        "source": {"root": str(source_root), "sha": source_sha, "clean_at_launch": True},
        "input": input_identity,
        "command": list(command_tuple),
        "cwd": str(cwd),
        "contract": contract_value,
        "artifacts": {"log": "run.log", "result": expected_result},
        "runner": {
            "pid": os.getpid(),
            "started_utc": _utc_now(),
            "started_monotonic_ns": time.monotonic_ns(),
        },
        "child": {"pid": None, "returncode": None, "finished_utc": None},
        "terminal": None,
    }
    try:
        _write_json_exclusive(root / "run.json", manifest)
    except BaseException:
        try:
            root.rmdir()
        except OSError:
            pass
        raise
    return EvaluationRun(root, command_tuple, cwd, expected_result)


def read_evaluation_run(root: Path) -> dict[str, Any]:
    """Read a run without a live tool/session and classify it fail-closed."""

    root = root.resolve()
    manifest = _read_manifest(root)
    source = manifest.get("source")
    recorded_input = manifest.get("input")
    artifacts = manifest.get("artifacts")
    if (
        not isinstance(source, dict)
        or not _SHA256_RE.fullmatch(str(source.get("sha", "")))
        or not isinstance(recorded_input, dict)
        or not isinstance(artifacts, dict)
    ):
        raise EvaluationRunContractError("durable source/input/artifact identity is incomplete")

    current_input: dict[str, Any] | None
    try:
        current_input = file_identity(Path(recorded_input["path"]))
    except (KeyError, TypeError, EvaluationRunContractError):
        current_input = None
    input_matches = current_input == recorded_input
    source_root_value = source.get("root")
    if not isinstance(source_root_value, str) or not source_root_value:
        raise EvaluationRunContractError("durable source root is invalid")
    source_root = Path(source_root_value).resolve()
    source_sha = source["sha"]
    current_source = _source_snapshot(source_root)
    terminal = manifest.get("terminal")
    historical_source_matches = _source_binding_is_valid(
        terminal.get("source") if isinstance(terminal, dict) else None,
        source_sha,
    )
    current_source_matches = _source_binding_is_valid(current_source, source_sha)

    result_name = artifacts.get("result")
    log_name = artifacts.get("log")
    if not isinstance(result_name, str) or not isinstance(log_name, str):
        raise EvaluationRunContractError("durable artifact names are invalid")
    result_artifact = _artifact_identity(root, result_name)
    log_artifact = _artifact_identity(root, log_name)
    status = manifest.get("status")
    base = {
        "schema": RUN_SCHEMA,
        "run_id": manifest.get("run_id"),
        "owner": manifest.get("owner"),
        "status": status,
        "source": {
            **source,
            "current_head": current_source["head"],
            "current_clean": current_source["clean"],
            "current_available": (
                current_source["head"] is not None
                and current_source["clean"] is not None
            ),
            "current_matches": current_source_matches,
            "historical_matches": historical_source_matches,
            "matches": (
                historical_source_matches
                if terminal is not None
                else current_source_matches
            ),
        },
        "input": {"recorded": recorded_input, "current": current_input, "matches": input_matches},
        "child": manifest.get("child"),
        "runner": manifest.get("runner"),
        "contract": manifest.get("contract"),
        "artifacts": {"log": log_artifact, "result": result_artifact},
        "terminal": terminal,
        "authoritative": False,
    }

    if status in {_STARTING, _RUNNING} or terminal is None:
        base.update(
            classification="INCOMPLETE_NO_TERMINAL",
            cause="UNKNOWN",
            terminal_present=False,
            process_observation="NOT_USED_TO_INFER_CAUSE",
        )
        return base
    if status not in {_COMPLETE, _FAILED} or not isinstance(terminal, dict):
        raise EvaluationRunContractError("durable terminal state is malformed")
    kind = terminal.get("kind")
    returncode = terminal.get("child_returncode")
    if kind not in {RUN_STOP, RUN_FAILED}:
        raise EvaluationRunContractError("durable terminal kind is invalid")

    if not input_matches:
        base.update(
            classification="INVALID_DURABLE_BINDING",
            cause="SOURCE_OR_INPUT_IDENTITY_CHANGED",
            terminal_present=True,
        )
        return base
    if not historical_source_matches:
        failure_class = terminal.get("failure_class")
        if failure_class in {
            "SOURCE_CHANGED_BEFORE_EXECUTION",
            "SOURCE_CHANGED_DURING_EXECUTION",
        }:
            classification = failure_class
        else:
            classification = "INVALID_DURABLE_BINDING"
        base.update(
            classification=classification,
            cause="SOURCE_BINDING_NOT_PROVEN",
            terminal_present=True,
        )
        return base
    if kind == RUN_STOP:
        if status != _COMPLETE or returncode != 0:
            raise EvaluationRunContractError("RUN_STOP does not prove a zero child exit")
        recorded_result = terminal.get("result")
        if not isinstance(recorded_result, dict) or result_artifact != recorded_result:
            base.update(
                classification="INCOMPLETE_RESULT_ARTIFACT_CHANGED",
                cause="RESULT_IDENTITY_MISMATCH",
                terminal_present=True,
            )
            return base
        if not result_artifact.get("present"):
            base.update(
                classification="INCOMPLETE_RESULT_ARTIFACT_MISSING",
                cause="RESULT_IDENTITY_MISMATCH",
                terminal_present=True,
            )
            return base
        recorded_contract = terminal.get("result_contract")
        if not isinstance(recorded_contract, dict) or not (
            recorded_contract.get("valid") is True
            and recorded_contract.get("status") == "COMPLETE"
        ):
            base.update(
                classification="INCOMPLETE_RESULT_ARTIFACT_INVALID",
                cause="RESULT_CONTRACT_NOT_COMPLETE",
                terminal_present=True,
            )
            return base
        base.update(
            classification="COMPLETE",
            cause=None,
            terminal_present=True,
            authoritative=True,
        )
        return base

    if status != _FAILED or kind != RUN_FAILED:
        raise EvaluationRunContractError("RUN_FAILED terminal state is malformed")
    failure_class = terminal.get("failure_class")
    if not isinstance(failure_class, str) or not failure_class:
        raise EvaluationRunContractError("RUN_FAILED lacks a failure classification")
    if failure_class == "CHILD_NONZERO" and (
        type(returncode) is not int or returncode == 0
    ):
        raise EvaluationRunContractError("CHILD_NONZERO lacks a non-zero child exit")
    if failure_class == "CHILD_SIGNALLED" and (
        type(returncode) is not int or returncode >= 0
    ):
        raise EvaluationRunContractError("CHILD_SIGNALLED lacks a negative child exit")
    if failure_class in {
        "RESULT_ARTIFACT_MISSING",
        "RESULT_ARTIFACT_INVALID",
        "RESULT_ARTIFACT_INCOMPLETE",
    } and returncode != 0:
        raise EvaluationRunContractError(
            "result-artifact failure must follow a zero child exit"
        )
    if failure_class == "LAUNCH_FAILED" and returncode is not None:
        raise EvaluationRunContractError("launch failure cannot claim a child exit")
    if failure_class == "SOURCE_CHANGED_BEFORE_EXECUTION" and returncode is not None:
        raise EvaluationRunContractError(
            "pre-execution source failure cannot claim a child exit"
        )
    if failure_class == "SOURCE_CHANGED_DURING_EXECUTION" and type(returncode) is not int:
        raise EvaluationRunContractError(
            "during-execution source failure lacks a child exit"
        )
    base.update(
        classification=failure_class,
        cause=failure_class,
        terminal_present=True,
    )
    return base


__all__ = [
    "DuplicateEvaluationRun",
    "EvaluationRun",
    "EvaluationRunContractError",
    "EvaluationRunError",
    "RUN_FAILED",
    "RUN_SCHEMA",
    "RUN_STOP",
    "file_identity",
    "prepare_evaluation_run",
    "read_evaluation_run",
]
