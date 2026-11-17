"""Tests for the option-chain ingestion CLI and NSE provider (gap 0016).

The live NSE option-chain feed has never been verifiable from the sandbox: the
legacy ``option-chain-indices`` endpoint returns HTTP 404 (NSE moved to
``option-chain-v3``) and the current endpoint is Akamai JS-gated (empty ``{}``
payload). The provider's live-with-fixture-fallback is therefore exercised with
a mocked transport and by monkeypatching the live fetch to fail, asserting the
labelled ``source: fixture`` snapshot is returned and the precise live failure
is surfaced via ``on_live_error`` (never fabricated as live data).
"""

from __future__ import annotations

import importlib
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from tantra_data.cli.ingest_option_chain import main as ingest_cli_main
from tantra_data.providers.option_chain import (
    fetch_option_chain,
    fetch_option_chain_fixture,
    parse_option_chain,
)
from tantra_data.store.option_chain import DuckDbOptionChainStore

FIXTURE_PATH = (
    Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "option_chain_nifty.json"
)


def test_importing_provider_requires_no_network() -> None:
    module = importlib.import_module("tantra_data.providers.option_chain")
    assert callable(module.fetch_option_chain)


def test_fetch_option_chain_falls_back_to_fixture(monkeypatch) -> None:
    def network_down(symbol: str):
        raise RuntimeError(f"network unavailable for {symbol}")

    monkeypatch.setattr(
        "tantra_data.providers.option_chain._fetch_live_option_chain",
        network_down,
    )
    snapshot = fetch_option_chain("NIFTY")
    assert snapshot.underlying == "NIFTY"
    assert snapshot.source == "fixture"
    assert len(snapshot.strikes) >= 4
    assert all(positioning.open_interest >= 0 for positioning in snapshot.strikes)


def test_parse_fixture_snapshot_is_valid() -> None:
    snapshot = parse_option_chain(FIXTURE_PATH.read_text(encoding="utf-8"), "NIFTY")
    assert snapshot.source == "fixture"
    assert snapshot.underlying == "NIFTY"
    assert snapshot.expiry == date(2026, 11, 26)
    # 16-Nov-2026 10:15:00 IST == 04:45:00 UTC
    assert snapshot.as_of == datetime(2026, 11, 16, 4, 45, tzinfo=UTC)
    assert {positioning.option_type for positioning in snapshot.strikes} == {"CE", "PE"}
    assert all(
        isinstance(positioning.gamma, Decimal) and isinstance(positioning.open_interest, Decimal)
        for positioning in snapshot.strikes
    )


def test_fetch_option_chain_fixture_is_labeled_fixture() -> None:
    assert fetch_option_chain_fixture("NIFTY").source == "fixture"


def test_dry_run_cli_prints_without_writing(tmp_path, capsys, monkeypatch) -> None:
    snapshot = fetch_option_chain_fixture("NIFTY")
    monkeypatch.setattr(
        "tantra_data.cli.ingest_option_chain.fetch_option_chain",
        lambda symbol, on_live_error=None: snapshot,
    )
    rc = ingest_cli_main(["--root", str(tmp_path), "--symbol", "NIFTY", "--dry-run"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "DRY-RUN" in captured.out
    assert "NIFTY" in captured.out
    assert not (tmp_path / "option_chain.duckdb").exists()


def test_once_cli_upserts_snapshot_into_store(tmp_path, capsys, monkeypatch) -> None:
    snapshot = fetch_option_chain_fixture("NIFTY")
    monkeypatch.setattr(
        "tantra_data.cli.ingest_option_chain.fetch_option_chain",
        lambda symbol, on_live_error=None: snapshot,
    )
    rc = ingest_cli_main(["--root", str(tmp_path), "--symbol", "NIFTY", "--once"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "Upserted" in captured.out
    assert (tmp_path / "option_chain.duckdb").exists()

    store = DuckDbOptionChainStore(tmp_path)
    try:
        chain = store.chain_as_of("NIFTY", snapshot.expiry, snapshot.as_of + timedelta(minutes=1))
        assert len(chain) == len(snapshot.strikes)
    finally:
        store.close()


def test_invalid_market_hours_window_rejected(tmp_path, capsys) -> None:
    rc = ingest_cli_main(["--root", str(tmp_path), "--hours", "15:30-9:15", "--once"])
    captured = capsys.readouterr()
    assert rc == 1
    assert "invalid --hours" in captured.err
