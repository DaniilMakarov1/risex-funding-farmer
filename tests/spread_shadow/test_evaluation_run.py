"""Distinguishing process/lifecycle tests for the bounded offline evaluator."""

from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import textwrap
import time

import pytest

from risex_spread_shadow.evaluation_run import (
    DuplicateEvaluationRun,
    prepare_evaluation_run,
    read_evaluation_run,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _source_sha() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=PROJECT_ROOT,
        text=True,
    ).strip()


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", *args],
        cwd=repo,
        text=True,
        stderr=subprocess.STDOUT,
    ).strip()


def _fixture_source(tmp_path: Path, source: str, *, helper: str | None = None):
    repo = tmp_path / "fixture-source"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "fixture@example.invalid")
    _git(repo, "config", "user.name", "Fixture")
    child = _write(repo / "calc.py", source)
    if helper is not None:
        _write(repo / "helper.py", helper)
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "fixture")
    return repo, child, _git(repo, "rev-parse", "HEAD")


def _write(path: Path, source: str) -> Path:
    path.write_text(textwrap.dedent(source), encoding="utf-8")
    return path


def _prepare(tmp_path: Path, child: Path, *, expected_result: str = "results.json"):
    input_path = tmp_path / "input.jsonl"
    input_path.write_text('{"kind":"FIXTURE_INPUT"}\n', encoding="utf-8")
    run_path = tmp_path / "run"
    return prepare_evaluation_run(
        run_path,
        run_id="evaluation-fixture-001",
        owner="builder-test",
        source_root=PROJECT_ROOT,
        source_sha=_source_sha(),
        input_path=input_path,
        command=[sys.executable, str(child)],
        expected_result=expected_result,
        contract={"bounded": True, "lanes": 4, "input_kind": "FIXTURE"},
    )


def _wait_for(path: Path) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {path}")


def _kill_pid(pid: int) -> None:
    def still_running() -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        try:
            state = subprocess.check_output(
                ["ps", "-o", "stat=", "-p", str(pid)],
                text=True,
            ).strip()
        except (OSError, subprocess.SubprocessError):
            return True
        return bool(state) and not state.startswith("Z")

    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if not still_running():
            return
        time.sleep(0.01)
    raise AssertionError(f"process {pid} did not exit")


def test_same_head_source_edit_is_rejected_before_child_execution(tmp_path: Path) -> None:
    repo, child, source_sha = _fixture_source(
        tmp_path,
        """
        from pathlib import Path

        Path("results.json").write_text('{"status":"COMPLETE"}', encoding="utf-8")
        Path("executed").touch()
        """,
    )
    input_path = tmp_path / "input.jsonl"
    input_path.write_text('{"kind":"FIXTURE_INPUT"}\n', encoding="utf-8")
    run = prepare_evaluation_run(
        tmp_path / "run",
        run_id="source-drift-before-execute",
        owner="builder-test",
        source_root=repo,
        source_sha=source_sha,
        input_path=input_path,
        command=[sys.executable, str(child)],
    )
    child.write_text(child.read_text(encoding="utf-8") + "\n# same HEAD edit\n", encoding="utf-8")

    result = run.execute()

    assert result["classification"] == "SOURCE_CHANGED_BEFORE_EXECUTION"
    assert result["authoritative"] is False
    assert result["terminal"]["child_returncode"] is None
    assert result["terminal"]["source"]["head"] == source_sha
    assert result["terminal"]["source"]["clean"] is False
    assert not (run.root / "executed").exists()


def test_source_edit_during_child_is_not_authoritative(tmp_path: Path) -> None:
    repo, child, source_sha = _fixture_source(
        tmp_path,
        """
        from pathlib import Path

        source = Path(__file__)
        source.write_text(source.read_text(encoding="utf-8") + "\n# child edit\n", encoding="utf-8")
        Path("results.json").write_text('{"status":"COMPLETE"}', encoding="utf-8")
        """,
    )
    input_path = tmp_path / "input.jsonl"
    input_path.write_text('{"kind":"FIXTURE_INPUT"}\n', encoding="utf-8")
    run = prepare_evaluation_run(
        tmp_path / "run",
        run_id="source-drift-during-execute",
        owner="builder-test",
        source_root=repo,
        source_sha=source_sha,
        input_path=input_path,
        command=[sys.executable, str(child)],
    )

    result = run.execute()

    assert result["classification"] == "SOURCE_CHANGED_DURING_EXECUTION"
    assert result["authoritative"] is False
    assert result["terminal"]["child_returncode"] == 0
    assert result["terminal"]["source"]["head"] == source_sha
    assert result["terminal"]["source"]["clean"] is False
    assert result["artifacts"]["result"]["present"] is True


def test_historical_completion_survives_source_checkout_drift_and_loss(tmp_path: Path) -> None:
    repo, child, source_sha = _fixture_source(
        tmp_path,
        """
        from pathlib import Path

        Path("results.json").write_text('{"status":"COMPLETE"}', encoding="utf-8")
        """,
    )
    input_path = tmp_path / "input.jsonl"
    input_path.write_text('{"kind":"FIXTURE_INPUT"}\n', encoding="utf-8")
    run = prepare_evaluation_run(
        tmp_path / "run",
        run_id="historical-source-binding",
        owner="builder-test",
        source_root=repo,
        source_sha=source_sha,
        input_path=input_path,
        command=[sys.executable, str(child)],
    )
    completed = run.execute()
    assert completed["classification"] == "COMPLETE"
    assert completed["source"]["historical_matches"] is True

    _write(repo / "README.md", "documentation only\n")
    _git(repo, "add", "README.md")
    _git(repo, "commit", "-q", "-m", "docs-only")
    after_docs = read_evaluation_run(run.root)

    assert after_docs["classification"] == "COMPLETE"
    assert after_docs["authoritative"] is True
    assert after_docs["source"]["historical_matches"] is True
    assert after_docs["source"]["current_matches"] is False
    assert after_docs["source"]["matches"] is True

    missing_root = tmp_path / "source-moved-away"
    repo.rename(missing_root)
    after_loss = read_evaluation_run(run.root)

    assert after_loss["classification"] == "COMPLETE"
    assert after_loss["authoritative"] is True
    assert after_loss["source"]["historical_matches"] is True
    assert after_loss["source"]["current_available"] is False


def test_old_launcher_has_no_durable_terminal_after_abrupt_process_loss(tmp_path: Path) -> None:
    """The historical subprocess.run wrapper loses its only exit record."""

    root = tmp_path / "old-run"
    root.mkdir()
    child = _write(
        root / "child.py",
        """
        import os
        import time
        from pathlib import Path

        Path("child.pid").write_text(str(os.getpid()), encoding="utf-8")
        while True:
            time.sleep(0.05)
        """,
    )
    launcher = _write(
        root / "run.py",
        """
        import subprocess
        from pathlib import Path
        import sys

        out = Path(__file__).parent
        with (out / "run.log").open("x") as log:
            result = subprocess.run(
                [sys.executable, str(out / "child.py")],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        (out / "run.exit").write_text(str(result.returncode) + "\\n")
        """,
    )
    runner = subprocess.Popen([sys.executable, str(launcher)], cwd=root)
    try:
        _wait_for(root / "child.pid")
        child_pid = int((root / "child.pid").read_text(encoding="utf-8"))
        _kill_pid(runner.pid)
        _kill_pid(child_pid)
        runner.wait(timeout=5)
    finally:
        if runner.poll() is None:
            runner.kill()
            runner.wait(timeout=5)

    assert (root / "run.log").exists()
    assert not (root / "run.exit").exists()
    assert not (root / "run.json").exists()


def test_success_persists_terminal_and_verified_result_identity(tmp_path: Path) -> None:
    child = _write(
        tmp_path / "success.py",
        """
        from pathlib import Path

        Path("results.json").write_text('{"status":"COMPLETE","closed":0}\\n', encoding="utf-8")
        print("CHILD_SUCCESS", flush=True)
        """,
    )
    run = _prepare(tmp_path, child)

    result = run.execute()
    reread = read_evaluation_run(run.root)

    assert result == reread
    assert result["classification"] == "COMPLETE"
    assert result["authoritative"] is True
    assert result["terminal"]["kind"] == "RUN_STOP"
    assert result["terminal"]["child_returncode"] == 0
    assert result["source"]["matches"] is True
    assert result["input"]["matches"] is True
    assert result["artifacts"]["result"]["present"] is True
    assert result["artifacts"]["log"]["present"] is True
    assert run.root.stat().st_mode & 0o077 == 0
    with pytest.raises(DuplicateEvaluationRun):
        run.execute()


def test_actual_launch_read_path_binds_cwd_output_and_import(tmp_path: Path) -> None:
    repo, child, source_sha = _fixture_source(
        tmp_path,
        """
        import json
        from pathlib import Path

        from helper import RESULT

        Path("results.json").write_text(
            json.dumps(
                {
                    "status": "COMPLETE",
                    "cwd": str(Path.cwd()),
                    "helper": RESULT,
                }
            ),
            encoding="utf-8",
        )
        """,
        helper='RESULT = "fixture-import"\n',
    )
    input_path = tmp_path / "input.jsonl"
    input_path.write_text('{"kind":"SAVED_INPUT"}\n', encoding="utf-8")
    run = prepare_evaluation_run(
        tmp_path / "run",
        run_id="launch-read-contract",
        owner="builder-test",
        source_root=repo,
        source_sha=source_sha,
        input_path=input_path,
        command=[sys.executable, str(child)],
        cwd=tmp_path / "run",
        contract={"input": str(input_path), "output": "results.json"},
    )

    result = run.execute()
    payload = json.loads((run.root / "results.json").read_text(encoding="utf-8"))

    assert result["classification"] == "COMPLETE"
    assert result["authoritative"] is True
    assert payload["cwd"] == str(run.root)
    assert payload["helper"] == "fixture-import"
    assert result["artifacts"]["result"]["path"] == str(run.root / "results.json")
    assert result["artifacts"]["log"]["path"] == str(run.root / "run.log")
    assert result["terminal"]["result_contract"] == {
        "valid": True,
        "status": "COMPLETE",
    }


@pytest.mark.parametrize(
    ("payload", "expected_classification", "reason"),
    [
        ("", "RESULT_ARTIFACT_INVALID", "EMPTY"),
        ("{", "RESULT_ARTIFACT_INVALID", "MALFORMED_JSON"),
        ('{"status":"INCOMPLETE","processed":3}', "RESULT_ARTIFACT_INCOMPLETE", "EXPLICIT_INCOMPLETE"),
    ],
)
def test_zero_exit_with_noncomplete_result_is_not_authoritative(
    tmp_path: Path,
    payload: str,
    expected_classification: str,
    reason: str,
) -> None:
    child = _write(
        tmp_path / "noncomplete.py",
        f"""
        from pathlib import Path

        Path("results.json").write_text({payload!r}, encoding="utf-8")
        """,
    )
    run = _prepare(tmp_path, child)

    result = run.execute()

    assert result["classification"] == expected_classification
    assert result["authoritative"] is False

    assert result["terminal"]["kind"] == "RUN_FAILED"
    assert result["terminal"]["child_returncode"] == 0
    assert result["terminal"]["result_contract"]["reason"] == reason
    assert result["artifacts"]["result"]["bytes"] == len(payload.encode())


def test_changed_complete_result_is_detected_on_read(tmp_path: Path) -> None:
    child = _write(
        tmp_path / "success.py",
        """
        from pathlib import Path

        Path("results.json").write_text('{"status":"COMPLETE"}', encoding="utf-8")
        """,
    )
    run = _prepare(tmp_path, child)
    assert run.execute()["classification"] == "COMPLETE"

    (run.root / "results.json").write_text(
        '{"status":"COMPLETE","changed":true}', encoding="utf-8"
    )
    result = read_evaluation_run(run.root)

    assert result["classification"] == "INCOMPLETE_RESULT_ARTIFACT_CHANGED"
    assert result["authoritative"] is False


@pytest.mark.parametrize(
    ("source", "expected_classification"),
    [
        (
            """
            from pathlib import Path
            raise SystemExit(7)
            """,
            "CHILD_NONZERO",
        ),
        (
            """
            import os
            import signal
            os.kill(os.getpid(), signal.SIGTERM)
            """,
            "CHILD_SIGNALLED",
        ),
    ],
)
def test_unsuccessful_child_is_terminal_failure_not_success(
    tmp_path: Path,
    source: str,
    expected_classification: str,
) -> None:
    child = _write(tmp_path / "failure.py", source)
    run = _prepare(tmp_path, child)

    result = run.execute()

    assert result["classification"] == expected_classification
    assert result["authoritative"] is False

    assert result["terminal"]["kind"] == "RUN_FAILED"
    assert result["terminal"]["child_returncode"] != 0


def test_zero_child_exit_without_durable_result_fails_closed(tmp_path: Path) -> None:
    child = _write(
        tmp_path / "no-result.py",
        """
        print("NO_RESULT", flush=True)
        """,
    )
    run = _prepare(tmp_path, child)

    result = run.execute()

    assert result["classification"] == "RESULT_ARTIFACT_MISSING"
    assert result["terminal"]["kind"] == "RUN_FAILED"
    assert result["terminal"]["child_returncode"] == 0
    assert result["authoritative"] is False


def test_abrupt_corrected_launcher_is_readable_incomplete_and_not_zero(tmp_path: Path) -> None:
    child = _write(
        tmp_path / "long-child.py",
        """
        import os
        import time
        from pathlib import Path

        Path("child.pid").write_text(str(os.getpid()), encoding="utf-8")
        Path("child.ready").touch()
        while True:
            time.sleep(0.05)
        """,
    )
    driver = _write(
        tmp_path / "driver.py",
        """
        from pathlib import Path
        import sys

        from risex_spread_shadow.evaluation_run import prepare_evaluation_run

        root = Path(sys.argv[1])
        child = Path(sys.argv[2])
        input_path = Path(sys.argv[3])
        source_root = Path(sys.argv[4])
        run = prepare_evaluation_run(
            root,
            run_id="evaluation-abrupt-001",
            owner="builder-test",
            source_root=source_root,
            source_sha=sys.argv[5],
            input_path=input_path,
            command=[sys.executable, str(child)],
            contract={"bounded": True, "lanes": 4},
        )
        run.execute()
        """,
    )
    input_path = tmp_path / "input.jsonl"
    input_path.write_text('{"kind":"FIXTURE_INPUT"}\n', encoding="utf-8")
    root = tmp_path / "abrupt-run"
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        [str(PROJECT_ROOT / "src"), environment.get("PYTHONPATH", "")]
    )
    runner = subprocess.Popen(
        [
            sys.executable,
            str(driver),
            str(root),
            str(child),
            str(input_path),
            str(PROJECT_ROOT),
            _source_sha(),
        ],
        cwd=PROJECT_ROOT,
        env=environment,
    )
    try:
        _wait_for(root / "child.ready")
        child_pid = int((root / "child.pid").read_text(encoding="utf-8"))
        _kill_pid(runner.pid)
        _kill_pid(child_pid)
        runner.wait(timeout=5)
    finally:
        if runner.poll() is None:
            runner.kill()
            runner.wait(timeout=5)

    report = read_evaluation_run(root)

    assert report["classification"] == "INCOMPLETE_NO_TERMINAL"
    assert report["cause"] == "UNKNOWN"
    assert report["authoritative"] is False
    assert report["terminal_present"] is False
    assert report["input"]["matches"] is True
    assert report["source"]["matches"] is True
    assert report["child"]["returncode"] is None
    assert not report["artifacts"]["result"]["present"]
    with pytest.raises(DuplicateEvaluationRun):
        prepare_evaluation_run(
            root,
            run_id="evaluation-abrupt-001-relaunch",
            owner="builder-test",
            source_root=PROJECT_ROOT,
            source_sha=_source_sha(),
            input_path=input_path,
            command=[sys.executable, str(child)],
        )


def test_bound_input_mutation_invalidates_even_a_zero_exit(tmp_path: Path) -> None:
    child = _write(
        tmp_path / "success.py",
        """
        from pathlib import Path

        Path("results.json").write_text('{"status":"COMPLETE"}\\n', encoding="utf-8")
        """,
    )
    run = _prepare(tmp_path, child)
    input_path = tmp_path / "input.jsonl"
    input_path.write_text('{"kind":"MUTATED_INPUT"}\n', encoding="utf-8")

    result = run.execute()

    assert result["classification"] == "INVALID_DURABLE_BINDING"
    assert result["terminal"]["kind"] == "RUN_STOP"
    assert result["terminal"]["child_returncode"] == 0
    assert result["input"]["matches"] is False
    assert result["authoritative"] is False
