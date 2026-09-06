from __future__ import annotations

import gc
import json
from pathlib import Path

import pytest

import risex_spread_shadow.s3_cycle as s3_cycle
from risex_spread_shadow.s3_cycle import (
    CycleEvidenceIntegrityError,
    _CycleCampaignBudget,
)


def _write_run(
    root: Path,
    run_name: str,
    records: list[dict[str, object]],
    *,
    trailing_blank_lines: int = 0,
) -> Path:
    path = root / f"run-{run_name}" / "evidence.jsonl"
    path.parent.mkdir(parents=True)
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for record in records:
            stream.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
            stream.write("\n")
        stream.write("\n" * trailing_blank_lines)
    return path


def _load_budget(root: Path, campaign_id: str) -> _CycleCampaignBudget:
    return _CycleCampaignBudget.load(
        root,
        campaign_id=campaign_id,
        max_records=1_000_000,
        max_bytes=4 * 1024 * 1024 * 1024,
        record_reserve=100_000,
        bytes_reserve=512 * 1024 * 1024,
    )


class _TrackedRecord(dict[str, object]):
    live = 0
    peak = 0

    def __init__(self, value: dict[str, object]) -> None:
        super().__init__(value)
        type(self).live += 1
        type(self).peak = max(type(self).peak, type(self).live)

    def __del__(self) -> None:
        type(self).live -= 1


def test_campaign_budget_streams_large_run_without_retaining_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign_id = "streaming-budget"
    record_count = 12_000
    path = tmp_path / "run-large" / "evidence.jsonl"
    path.parent.mkdir()
    metadata = {
        "metadata": {"campaign_id": campaign_id},
        "kind": "RUN_METADATA",
    }
    payload = "x" * 1_024
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(metadata, separators=(",", ":")) + "\n")
        for index in range(record_count - 1):
            stream.write(
                json.dumps(
                    {"kind": "CYCLE_NOTE", "ordinal": index, "payload": payload},
                    separators=(",", ":"),
                )
                + "\n"
            )

    _TrackedRecord.live = 0
    _TrackedRecord.peak = 0
    source_iter_records = s3_cycle.iter_records

    def tracked_iter_records(source: Path):
        for record in source_iter_records(source):
            yield _TrackedRecord(record)

    monkeypatch.setattr(s3_cycle, "iter_records", tracked_iter_records)
    budget = _load_budget(tmp_path, campaign_id)
    gc.collect()

    assert budget.record_count == record_count
    assert budget.byte_count == path.stat().st_size
    # The former list(iter_records(...)) implementation retains every
    # decoded record until the file has been consumed.  Streaming may retain
    # the first metadata record and the current iterator item, but not the
    # whole adverse run.
    assert _TrackedRecord.peak <= 8
    assert _TrackedRecord.live == 0


def test_campaign_budget_accounts_multiple_matching_runs_and_skips_other_campaigns(
    tmp_path: Path,
) -> None:
    campaign_id = "target-campaign"
    matching_one = [
        {"metadata": {"campaign_id": campaign_id}, "kind": "RUN_METADATA"},
        {"kind": "CYCLE_NOTE", "value": 1},
    ]
    matching_two = [
        {"metadata": {"campaign_id": campaign_id}, "kind": "RUN_METADATA"},
        {"kind": "CYCLE_NOTE", "value": 2},
        {"kind": "RUN_STOP"},
    ]
    unrelated = [
        {"metadata": {"campaign_id": "other-campaign"}, "kind": "RUN_METADATA"},
        {"kind": "CYCLE_NOTE", "value": 99},
        {"kind": "RUN_STOP"},
    ]
    matching_one_path = _write_run(
        tmp_path,
        "001",
        matching_one,
        trailing_blank_lines=2,
    )
    matching_two_path = _write_run(tmp_path, "002", matching_two)
    unrelated_path = _write_run(tmp_path, "003", unrelated)

    budget = _load_budget(tmp_path, campaign_id)

    assert budget.record_count == len(matching_one) + len(matching_two)
    assert budget.byte_count == matching_one_path.stat().st_size + matching_two_path.stat().st_size
    assert budget.byte_count < (
        matching_one_path.stat().st_size
        + matching_two_path.stat().st_size
        + unrelated_path.stat().st_size
    )


def test_campaign_budget_wrong_campaign_is_fully_consumed_but_not_counted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = [
        {"metadata": {"campaign_id": "other-campaign"}, "kind": "RUN_METADATA"},
        *(
            {"kind": "CYCLE_NOTE", "ordinal": index}
            for index in range(32)
        ),
    ]
    path = _write_run(tmp_path, "wrong", records)
    consumed = 0
    source_iter_records = s3_cycle.iter_records

    def counting_iter_records(source: Path):
        nonlocal consumed
        for record in source_iter_records(source):
            consumed += 1
            yield record

    monkeypatch.setattr(s3_cycle, "iter_records", counting_iter_records)
    budget = _load_budget(tmp_path, "target-campaign")

    assert consumed == len(records)
    assert budget.record_count == 0
    assert budget.byte_count == 0
    assert path.stat().st_size > 0


@pytest.mark.parametrize(
    ("contents", "message"),
    (
        ("", "empty run"),
        ("\n\n", "empty run"),
        (
            '{"kind":"RUN_METADATA","metadata":{"campaign_id":"target-campaign"}}\n'
            '{"kind":"BROKEN"',
            "cannot read an existing run",
        ),
        (
            '{"kind":"RUN_METADATA","metadata":[]}\n',
            "malformed metadata",
        ),
    ),
)
def test_campaign_budget_rejects_empty_malformed_and_invalid_runs(
    tmp_path: Path,
    contents: str,
    message: str,
) -> None:
    path = tmp_path / "run-invalid" / "evidence.jsonl"
    path.parent.mkdir()
    path.write_text(contents, encoding="utf-8")

    with pytest.raises(CycleEvidenceIntegrityError, match=message):
        _load_budget(tmp_path, "target-campaign")
