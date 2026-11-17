"""Live-vs-fallback contract tests for the NSE option-chain provider (gap 0016).

Pins the T12.3 follow-on hardening:

* the live path targets the *current* NSE endpoint (``option-chain-v3`` with
  ``type=Indices``), not the removed legacy ``option-chain-indices`` path;
* a live success (mocked transport with a real-shape NSE payload — the
  fixture's shape without the ``source: fixture`` label) parses to a
  ``source: nse`` snapshot and never invokes ``on_live_error``;
* every live failure mode (404 moved/removed, 200 empty JS-gated payload,
  transport error) falls back to the labelled fixture with the precise
  :class:`OptionChainLiveError` surfaced — no data is ever fabricated;
* the ingestion CLI keeps working on the fallback and prints the live-failure
  ``WARNING`` to stderr.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

from tantra_data.cli.ingest_option_chain import main as ingest_cli_main
from tantra_data.providers.option_chain import (
    LIVE_VERIFICATION,
    OPTION_CHAIN_TYPE,
    OPTION_CHAIN_URL,
    OptionChainLiveError,
    fetch_option_chain,
    fetch_option_chain_fixture,
)
from tantra_data.store.option_chain import DuckDbOptionChainStore

FIXTURE_PATH = (
    Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "option_chain_nifty.json"
)

LIVE_HEADERS_EXPECTED = {
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/option-chain",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}


class _FakeResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code


class _FakeSession:
    """Minimal curl_cffi-session stand-in; records the live request shape."""

    def __init__(self, api_text: str, api_status_code: int = 200) -> None:
        self._api_text = api_text
        self._api_status_code = api_status_code
        self.calls: list[tuple[str, dict[str, str] | None, dict[str, str] | None]] = []

    def get(self, url: str, params=None, headers=None, timeout=None) -> _FakeResponse:
        self.calls.append((url, params, headers))
        if url == "https://www.nseindia.com":
            return _FakeResponse("<html>NSE</html>")
        return _FakeResponse(self._api_text, self._api_status_code)


def _live_shaped_payload() -> str:
    """The fixture payload in its live (unlabelled) shape.

    A real NSE payload carries no ``source`` field; ``parse_option_chain``
    defaults to ``source: nse``. Dropping the fixture's ``source``/``info``
    markers gives a real-shape live payload.
    """
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    payload.pop("source", None)
    payload.pop("info", None)
    return json.dumps(payload)


def _strikes(snapshot) -> list[object]:
    return list(snapshot.strikes)


def test_live_path_targets_current_nse_endpoint() -> None:
    assert OPTION_CHAIN_URL == "https://www.nseindia.com/api/option-chain-v3"
    assert OPTION_CHAIN_TYPE == "Indices"


def test_verification_status_is_honest_and_machine_readable() -> None:
    assert LIVE_VERIFICATION.startswith("not-verified-live")
    assert "never fabricated" in LIVE_VERIFICATION


def test_live_success_parses_real_shape_payload_as_nse(monkeypatch) -> None:
    session = _FakeSession(_live_shaped_payload(), 200)
    monkeypatch.setattr("tantra_data.providers.option_chain._requests_session", lambda: session)

    live_errors: list[OptionChainLiveError] = []
    snapshot = fetch_option_chain("NIFTY", on_live_error=live_errors.append)

    assert snapshot.source == "nse"
    assert snapshot.underlying == "NIFTY"
    assert snapshot.expiry == date(2026, 11, 26)
    assert snapshot.as_of == datetime(2026, 11, 16, 4, 45, tzinfo=UTC)
    assert len(snapshot.strikes) == len(_strikes(fetch_option_chain_fixture("NIFTY")))
    assert live_errors == []

    api_call = session.calls[-1]
    assert api_call[0] == OPTION_CHAIN_URL
    assert api_call[1] == {"type": "Indices", "symbol": "NIFTY"}
    assert api_call[2] == LIVE_HEADERS_EXPECTED


def test_live_404_falls_back_to_fixture_and_surfaces_moved_endpoint(monkeypatch) -> None:
    session = _FakeSession("Resource not found", 404)
    monkeypatch.setattr("tantra_data.providers.option_chain._requests_session", lambda: session)

    live_errors: list[OptionChainLiveError] = []
    snapshot = fetch_option_chain("NIFTY", on_live_error=live_errors.append)

    assert snapshot.source == "fixture"
    assert len(snapshot.strikes) == len(_strikes(fetch_option_chain_fixture("NIFTY")))
    assert len(live_errors) == 1
    error = live_errors[0]
    assert isinstance(error, OptionChainLiveError)
    assert error.status_code == 404
    assert error.symbol == "NIFTY"
    assert error.url == OPTION_CHAIN_URL
    assert "HTTP 404" in str(error)
    assert "option-chain-v3" in str(error)


def test_live_empty_payload_falls_back_with_precise_error(monkeypatch) -> None:
    session = _FakeSession("{}", 200)
    monkeypatch.setattr("tantra_data.providers.option_chain._requests_session", lambda: session)

    live_errors: list[OptionChainLiveError] = []
    snapshot = fetch_option_chain("NIFTY", on_live_error=live_errors.append)

    assert snapshot.source == "fixture"
    assert len(live_errors) == 1
    error = live_errors[0]
    assert isinstance(error, OptionChainLiveError)
    assert error.status_code == 200
    assert "unusable payload" in str(error)


def test_transport_error_falls_back_with_none_status(monkeypatch) -> None:
    def broken_session():
        raise OSError("network unreachable")

    monkeypatch.setattr("tantra_data.providers.option_chain._requests_session", broken_session)

    live_errors: list[OptionChainLiveError] = []
    snapshot = fetch_option_chain("NIFTY", on_live_error=live_errors.append)

    assert snapshot.source == "fixture"
    assert len(live_errors) == 1
    assert live_errors[0].status_code is None
    assert live_errors[0].symbol == "NIFTY"


def test_cli_dry_run_surfaces_live_failure_warning_and_keeps_working(
    tmp_path, capsys, monkeypatch
) -> None:
    session = _FakeSession("Resource not found", 404)
    monkeypatch.setattr("tantra_data.providers.option_chain._requests_session", lambda: session)

    rc = ingest_cli_main(["--root", str(tmp_path), "--symbol", "NIFTY", "--dry-run"])
    captured = capsys.readouterr()

    assert rc == 0
    assert "DRY-RUN" in captured.out
    assert "source=fixture" in captured.out
    assert "WARNING option-chain live fetch failed" in captured.err
    assert "HTTP 404" in captured.err
    assert not (tmp_path / "option_chain.duckdb").exists()


def test_cli_once_on_fallback_still_upserts(tmp_path, capsys, monkeypatch) -> None:
    session = _FakeSession("{}", 200)
    monkeypatch.setattr("tantra_data.providers.option_chain._requests_session", lambda: session)

    rc = ingest_cli_main(["--root", str(tmp_path), "--symbol", "NIFTY", "--once"])
    captured = capsys.readouterr()

    assert rc == 0
    assert "Upserted" in captured.out
    assert "WARNING" in captured.err
    assert (tmp_path / "option_chain.duckdb").exists()

    store = DuckDbOptionChainStore(tmp_path)
    try:
        fixture = fetch_option_chain_fixture("NIFTY")
        chain = store.chain_as_of("NIFTY", fixture.expiry, datetime(2026, 11, 17, tzinfo=UTC))
        assert len(chain) == len(fixture.strikes)
    finally:
        store.close()
