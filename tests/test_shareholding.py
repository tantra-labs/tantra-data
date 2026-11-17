"""Shareholding / pledge / insider-disclosure pipeline tests (gap 0024 — T17.5).

Covers the three verified-live governance/disclosure datasets:

* **Shareholding patterns** — BSE LODR Reg 31 iXBRL filings
  (``providers/bse_shareholding.py``), point-in-time by ``filing_date``.
* **Promoter pledge** — NSE ``corporate-pledgedata`` (``providers/nse_pledge.py``).
* **Insider disclosures** — NSE SEBI-PIT ``corporates-pit``
  (``providers/nse_pit.py``).

All three live sources were **verified 2026-11-17 from the sandbox** (see the
``LIVE_VERIFICATION`` statuses / the gap file): BSE returned HTTP 200 JSON +
a 1.27 MB structured iXBRL; NSE pledge returned HTTP 200 with 1,555 company
snapshots; NSE PIT returned HTTP 200 with 20 structured RELIANCE rows. The
tests exercise the providers against the **recorded fixtures** (labelled
``source: fixture``, verbatim from the live payloads), the live-with-fallback
contract, the DuckDB store round-trips / idempotency / point-in-time reads,
the reader ports, the ingestion CLI, and the Parquet bridge exports that Rust
readers consume.

The SAST (SEBI Substantial Acq of shares & Takeovers) Reg 29/31 dataset stays
open-and-documented: no structured free source was verified (NSE sast
endpoints are HTTP 404; the announcements feed is attachment-only text).
"""

from __future__ import annotations

import importlib
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from tantra_data import (
    DuckDbShareholdingStore,
    InsiderDisclosure,
    InsiderDisclosureReader,
    PledgeReader,
    PledgeRecord,
    PythonInsiderDisclosureReader,
    PythonPledgeReader,
    PythonShareholdingReader,
    ShareholdingPattern,
    ShareholdingReader,
)
from tantra_data.cli.ingest_shareholding import main as ingest_cli_main
from tantra_data.providers.bse_shareholding import (
    LIVE_VERIFICATION as BSE_LIVE_VERIFICATION,
)
from tantra_data.providers.bse_shareholding import (
    BseShareholdingLiveError,
    fetch_shareholding_patterns,
    fetch_shareholding_patterns_fixture,
    normalize_category,
    parse_bse_ixbrl,
    parse_shareholding_index,
)
from tantra_data.providers.nse_pit import (
    LIVE_VERIFICATION as PIT_LIVE_VERIFICATION,
)
from tantra_data.providers.nse_pit import (
    NsePitLiveError,
    fetch_insider_disclosures,
    fetch_insider_disclosures_fixture,
    parse_pit_payload,
)
from tantra_data.providers.nse_pledge import (
    LIVE_VERIFICATION as PLEDGE_LIVE_VERIFICATION,
)
from tantra_data.providers.nse_pledge import (
    NsePledgeLiveError,
    fetch_pledge_records,
    fetch_pledge_records_fixture,
    parse_pledge_payload,
)
from tantra_data.service.bridge_exporter import LakehouseBridgeExporter

FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures"

BSE_FIXTURE = FIXTURES / "shareholding_bse_ixbrl.json"
PLEDGE_FIXTURE = FIXTURES / "nse_pledgedata.json"
PIT_FIXTURE = FIXTURES / "nse_pit_reliance.json"


# ---------------------------------------------------------------------------
# Domain
# ---------------------------------------------------------------------------


def test_shareholding_pattern_domain_exact_decimal() -> None:
    pattern = ShareholdingPattern(
        instrument="500325",
        company_name="Reliance Industries Ltd",
        exchange="BSE",
        filing_date=date(2026, 10, 21),
        quarter_end=date(2026, 9, 30),
        category="promoter_promoter_group",
        no_of_shares=6_715_496_096,
        shareholding_pct=Decimal("50.44"),
    )
    assert pattern.shareholding_pct == Decimal("50.44")
    assert isinstance(pattern.shareholding_pct, Decimal)
    assert not isinstance(pattern.shareholding_pct, float)


def test_shareholding_pattern_rejects_bad_fields() -> None:
    kwargs = dict(
        instrument="500325",
        company_name="Reliance Industries Ltd",
        exchange="BSE",
        filing_date=date(2026, 10, 21),
        quarter_end=date(2026, 9, 30),
        category="promoter_promoter_group",
        no_of_shares=0,
        shareholding_pct="50.44",
    )
    try:
        ShareholdingPattern(**{**kwargs, "category": ""})
    except ValueError as exc:
        assert "category" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected ValueError for empty category")

    try:
        ShareholdingPattern(**{**kwargs, "no_of_shares": -1})
    except ValueError as exc:
        assert "no_of_shares" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected ValueError for negative shares")

    try:
        ShareholdingPattern(**{**kwargs, "shareholding_pct": -0.01})
    except ValueError as exc:
        assert "shareholding_pct" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected ValueError for negative pct")


def test_pledge_record_domain() -> None:
    record = PledgeRecord(
        instrument="20 Microns Limited",
        company_name="20 Microns Limited",
        quarter_end=date(2026, 9, 30),
        broadcast_date=date(2026, 11, 17),
        promoter_holding_pct=Decimal("45.04"),
        pledged_shares=1_559_600,
        pledged_pct=Decimal("4.42"),
        promoter_shares=15_893_364,
        total_issued_shares=35_286_502,
    )
    assert record.pledged_pct == Decimal("4.42")
    assert isinstance(record.pledged_pct, Decimal)


def test_pledged_pct_of_promoter_is_exact_decimal() -> None:
    record = PledgeRecord(
        instrument="20 Microns Limited",
        company_name="20 Microns Limited",
        quarter_end=date(2026, 9, 30),
        broadcast_date=date(2026, 11, 17),
        promoter_holding_pct=Decimal("45.04"),
        pledged_shares=1_559_600,
        pledged_pct=Decimal("4.42"),
        promoter_shares=15_893_364,
        total_issued_shares=35_286_502,
    )
    ratio = Decimal(record.pledged_shares) / Decimal(record.promoter_shares) * Decimal(100)
    from tantra_data.domain.shareholding import pledged_pct_of_promoter

    computed = pledged_pct_of_promoter(record)
    assert computed.quantize(Decimal("0.0001")) == ratio.quantize(Decimal("0.0001"))
    assert computed.quantize(Decimal("0.01")) == Decimal("9.81")
    # zero promoter shares -> zero ratio, never a division error
    empty = PledgeRecord(
        instrument="X",
        company_name="X",
        quarter_end=date(2026, 9, 30),
        broadcast_date=date(2026, 11, 17),
        promoter_holding_pct=Decimal("0"),
        pledged_shares=0,
        pledged_pct=Decimal("0"),
        promoter_shares=0,
        total_issued_shares=100,
    )
    assert pledged_pct_of_promoter(empty) == Decimal(0)


def test_insider_disclosure_domain_normalizes_transaction_type() -> None:
    disclosure = InsiderDisclosure(
        instrument="RELIANCE",
        company_name="Reliance Industries Limited",
        disclosure_id=563850,
        trade_date=date(2026, 2, 13),
        intimation_date=date(2026, 2, 16),
        name="BALANADU NARAYAN",
        person_category="Other",
        transaction_type="sell",
        acq_mode="Off Market",
        acquired_shares=2320,
        buy_quantity=0,
        sell_quantity=0,
        value=Decimal("3294168"),
        before_shares=3920,
        before_pct=Decimal("0"),
        after_shares=1600,
        after_pct=Decimal("0"),
    )
    assert disclosure.transaction_type == "SELL"

    pledge = InsiderDisclosure(
        instrument="RELIANCE",
        company_name="Reliance Industries Limited",
        disclosure_id=1,
        trade_date=date(2026, 2, 13),
        intimation_date=date(2026, 2, 16),
        name="X",
        person_category="Promoter Group",
        transaction_type="Pledge Invoke",
        acq_mode="Pledge Creation",
        acquired_shares=100,
        buy_quantity=0,
        sell_quantity=0,
        value=Decimal("0"),
        before_shares=1000,
        before_pct=Decimal("0"),
        after_shares=1100,
        after_pct=Decimal("0"),
    )
    assert pledge.transaction_type == "PLEDGE"


def test_insider_disclosure_rejects_bad_transaction_type() -> None:
    kwargs = dict(
        instrument="RELIANCE",
        company_name="Reliance Industries Limited",
        disclosure_id=1,
        trade_date=date(2026, 2, 13),
        intimation_date=date(2026, 2, 16),
        name="X",
        person_category="Other",
        acq_mode="Off Market",
        acquired_shares=0,
        buy_quantity=0,
        sell_quantity=0,
        value=Decimal("0"),
        before_shares=0,
        before_pct=Decimal("0"),
        after_shares=0,
        after_pct=Decimal("0"),
    )
    try:
        InsiderDisclosure(**{**kwargs, "transaction_type": "Hedge"})
    except ValueError as exc:
        assert "transaction_type" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected ValueError for bad transaction type")


# ---------------------------------------------------------------------------
# BSE shareholding provider
# ---------------------------------------------------------------------------


def test_bse_live_verification_is_verified_and_machine_readable() -> None:
    assert BSE_LIVE_VERIFICATION.startswith("verified-live")
    assert "HTTP 200" in BSE_LIVE_VERIFICATION


def test_normalize_category() -> None:
    assert normalize_category("ShareholdingOfPromoterAndPromoterGroup") == "promoter_promoter_group"
    assert normalize_category("MutualFundsOrUTI") == "mutual_funds"
    assert normalize_category("InstitutionsForeign") == "fii_fpi"
    assert normalize_category("PublicShareholding") == "public"
    # Unknown members snake_case verbatim — never dropped or fabricated.
    assert normalize_category("SomeFutureCategory") == "some_future_category"


def test_parse_shareholding_index() -> None:
    text = json.dumps(
        {
            "Table": [
                {
                    "FLD_ScripCode": 500325,
                    "Company_NAme": "Reliance Industries Ltd",
                    "sQtrName": "September 2026",
                    "EndDate": "2026-09-30T00:00:00",
                    "D": "2026-10-21T15:48:00",
                    "IsXBRL": 1,
                    "XBRLAttachment": "/XBRLFILES/SHPXBRLDataXML/500325_1_SP.html",
                },
                {
                    "FLD_ScripCode": 500325,
                    "Company_NAme": "Reliance Industries Ltd",
                    "sQtrName": "June 2026",
                    "EndDate": "2026-06-30T00:00:00",
                    "D": "2026-07-21T15:40:00",
                    "IsXBRL": 0,
                    "XBRLAttachment": None,
                },
            ]
        }
    )
    filings = parse_shareholding_index(text)
    assert len(filings) == 2
    latest = filings[0]
    assert latest.scripcode == "500325"
    assert latest.company_name == "Reliance Industries Ltd"
    assert latest.filing_date == date(2026, 10, 21)
    assert latest.quarter_end == date(2026, 9, 30)
    assert latest.is_xbrl is True
    assert latest.xbrl_attachment == "/XBRLFILES/SHPXBRLDataXML/500325_1_SP.html"
    assert filings[1].is_xbrl is False
    assert filings[1].xbrl_attachment is None


def test_parse_bse_ixbrl_mini_snippet() -> None:
    """Parse a minimal iXBRL snippet shaped exactly like the live BSE tags."""
    html = (
        "<html><body>"
        "<ix:nonFraction name='in-bse-shp:ShareholdingAsAPercentageOfTotalNumberOfShares' "
        "contextRef='ShareholdingOfPromoterAndPromoterGroup_ContextI' decimals='INF'  "
        "unitRef='pure'>50.44</ix:nonFraction>"
        "<ix:nonFraction name='in-bse-shp:NumberOfFullyPaidUpEquityShares' "
        "contextRef='ShareholdingOfPromoterAndPromoterGroup_ContextI' decimals='INF' "
        "unitRef='shares'>6715496096</ix:nonFraction>"
        "<ix:nonFraction name='in-bse-shp:ShareholdingAsAPercentageOfTotalNumberOfShares' "
        "contextRef='PublicShareholding_ContextI' decimals='INF'  unitRef='pure'>49.56"
        "</ix:nonFraction>"
        "<ix:nonFraction name='in-bse-shp:NumberOfFullyPaidUpEquityShares' "
        "contextRef='PublicShareholding_ContextI' decimals='INF' unitRef='shares'>6597252758"
        "</ix:nonFraction>"
        "</body></html>"
    )
    rows = parse_bse_ixbrl(
        html,
        scripcode="500325",
        company_name="Reliance Industries Ltd",
        filing_date=date(2026, 10, 21),
        quarter_end=date(2026, 9, 30),
    )
    assert len(rows) == 2
    by_category = {row.category: row for row in rows}
    assert by_category["promoter_promoter_group"].shareholding_pct == Decimal("50.44")
    assert by_category["promoter_promoter_group"].no_of_shares == 6_715_496_096
    assert by_category["public"].shareholding_pct == Decimal("49.56")
    assert by_category["public"].no_of_shares == 6_597_252_758
    assert all(isinstance(row.shareholding_pct, Decimal) for row in rows)
    assert all(row.source == "bse" for row in rows)


def test_parse_bse_ixbrl_rejects_empty_payload() -> None:
    try:
        parse_bse_ixbrl(
            "<html><body>no facts</body></html>",
            scripcode="500325",
            company_name="Reliance Industries Ltd",
            filing_date=date(2026, 10, 21),
            quarter_end=date(2026, 9, 30),
        )
    except ValueError as exc:
        assert "structured facts" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected ValueError for fact-less payload")


def test_bse_fixture_is_recorded_and_labeled() -> None:
    payload = json.loads(BSE_FIXTURE.read_text(encoding="utf-8"))
    assert payload["source"] == "fixture"
    assert "Recorded 2026-11-17" in payload["info"]
    rows = fetch_shareholding_patterns_fixture()
    assert len(rows) == len(payload["rows"]) == 36
    # The recorded summary categories include the canonical SEBI buckets.
    categories = {row.category for row in rows}
    assert {"promoter_promoter_group", "public", "fii_fpi", "mutual_funds", "total"} <= categories
    by_category = {row.category: row for row in rows}
    assert by_category["total"].shareholding_pct == Decimal("100.00")
    assert by_category["promoter_promoter_group"].shareholding_pct == Decimal("50.44")
    assert all(row.source == "fixture" for row in rows)
    assert all(isinstance(row.shareholding_pct, Decimal) for row in rows)


def test_fetch_shareholding_falls_back_to_fixture(monkeypatch) -> None:
    def live_down():
        raise RuntimeError("network unavailable")

    monkeypatch.setattr(
        "tantra_data.providers.bse_shareholding._fetch_live_shareholding_patterns", live_down
    )
    rows = fetch_shareholding_patterns("500325")
    assert len(rows) == 36
    assert all(row.source == "fixture" for row in rows)


def test_fetch_shareholding_surfaces_live_error(monkeypatch) -> None:
    errors: list[BseShareholdingLiveError] = []

    def live_down():
        raise OSError("connection refused")

    monkeypatch.setattr(
        "tantra_data.providers.bse_shareholding._fetch_live_shareholding_patterns", live_down
    )
    rows = fetch_shareholding_patterns("500325", on_live_error=errors.append)
    assert len(rows) == 36
    assert len(errors) == 1
    assert errors[0].status_code is None
    assert "bseindia" in errors[0].url


def test_importing_bse_provider_requires_no_network() -> None:
    module = importlib.import_module("tantra_data.providers.bse_shareholding")
    assert callable(module.fetch_shareholding_patterns)


# ---------------------------------------------------------------------------
# NSE pledge provider
# ---------------------------------------------------------------------------


def test_pledge_live_verification_is_verified() -> None:
    assert PLEDGE_LIVE_VERIFICATION.startswith("verified-live")
    assert "1,555" in PLEDGE_LIVE_VERIFICATION


def test_parse_pledge_payload_from_recorded_fixture() -> None:
    records = parse_pledge_payload(PLEDGE_FIXTURE.read_text(encoding="utf-8"))
    assert len(records) == 12
    first = records[0]
    assert first.instrument == "20 Microns Limited"
    assert first.quarter_end == date(2026, 9, 30)
    assert first.broadcast_date == date(2026, 11, 17)
    assert first.promoter_holding_pct == Decimal("45.04")
    assert first.pledged_shares == 1_559_600
    assert first.pledged_pct == Decimal("4.42")
    assert all(isinstance(r.pledged_pct, Decimal) for r in records)
    assert all(r.source == "nse" for r in records)


def test_parse_pledge_payload_rejects_invalid() -> None:
    try:
        parse_pledge_payload("not json")
    except ValueError as exc:
        assert "JSON" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected ValueError for invalid JSON")

    # A row without usable as-of dates (no shp / no broadcastDt) is skipped —
    # never fabricated — so the payload parses to an empty list.
    assert parse_pledge_payload(json.dumps({"data": [{"comName": "X"}]})) == []


def test_fetch_pledge_falls_back_to_fixture(monkeypatch) -> None:
    def live_down():
        raise RuntimeError("offline")

    monkeypatch.setattr("tantra_data.providers.nse_pledge._fetch_live_pledge_records", live_down)
    records = fetch_pledge_records()
    assert len(records) == 12
    assert all(r.source == "fixture" for r in records)


def test_fetch_pledge_surfaces_live_error(monkeypatch) -> None:
    errors: list[NsePledgeLiveError] = []

    def live_down():
        raise OSError("blocked")

    monkeypatch.setattr("tantra_data.providers.nse_pledge._fetch_live_pledge_records", live_down)
    records = fetch_pledge_records(on_live_error=errors.append)
    assert len(records) == 12
    assert len(errors) == 1
    assert errors[0].status_code is None
    assert "pledgedata" in errors[0].url


# ---------------------------------------------------------------------------
# NSE PIT insider provider
# ---------------------------------------------------------------------------


def test_pit_live_verification_is_verified_and_notes_sast_open() -> None:
    assert PIT_LIVE_VERIFICATION.startswith("verified-live")
    assert "SAST" in PIT_LIVE_VERIFICATION


def test_parse_pit_payload_from_recorded_fixture() -> None:
    disclosures = parse_pit_payload(PIT_FIXTURE.read_text(encoding="utf-8"), "RELIANCE")
    assert len(disclosures) == 20
    first = disclosures[0]
    assert first.instrument == "RELIANCE"
    assert first.company_name == "Reliance Industries Limited"
    assert first.name == "BALANADU NARAYAN"
    assert first.transaction_type == "SELL"
    assert first.trade_date == date(2026, 2, 13)
    assert first.intimation_date == date(2026, 2, 16)
    assert first.value == Decimal("3294168")
    assert all(isinstance(d.value, Decimal) for d in disclosures)
    assert all(d.source == "nse" for d in disclosures)


def test_parse_pit_payload_maps_nil_to_zero() -> None:
    disclosures = parse_pit_payload(PIT_FIXTURE.read_text(encoding="utf-8"), "RELIANCE")
    by_id = {d.disclosure_id: d for d in disclosures}
    # Reliance Industrial Investments: before holding was "Nil" (held none).
    assert by_id[475186].before_shares == 0
    assert by_id[475186].after_shares == 240_942_006
    # Nasib Kapoor: after holding was "Nil" (sold out).
    assert by_id[420795].before_shares == 2500
    assert by_id[420795].after_shares == 0


def test_fetch_insider_falls_back_to_fixture_for_recorded_symbol(monkeypatch) -> None:
    def live_down():
        raise RuntimeError("offline")

    monkeypatch.setattr("tantra_data.providers.nse_pit._fetch_live_pit_disclosures", live_down)
    disclosures = fetch_insider_disclosures("RELIANCE")
    assert len(disclosures) == 20
    assert all(d.source == "fixture" for d in disclosures)


def test_fetch_insider_fixture_never_fabricates_other_symbols(monkeypatch) -> None:
    def live_down():
        raise RuntimeError("offline")

    monkeypatch.setattr("tantra_data.providers.nse_pit._fetch_live_pit_disclosures", live_down)
    # The recorded fixture covers RELIANCE only; a different symbol falls back
    # to an empty list, never mislabelled RELIANCE rows.
    assert fetch_insider_disclosures("TATAMOTORS") == []


def test_fetch_insider_surfaces_live_error(monkeypatch) -> None:
    errors: list[NsePitLiveError] = []

    def live_down():
        raise OSError("blocked")

    monkeypatch.setattr("tantra_data.providers.nse_pit._fetch_live_pit_disclosures", live_down)
    disclosures = fetch_insider_disclosures("RELIANCE", on_live_error=errors.append)
    assert len(disclosures) == 20
    assert len(errors) == 1
    assert errors[0].status_code is None
    assert errors[0].symbol == "RELIANCE"


# ---------------------------------------------------------------------------
# DuckDB store
# ---------------------------------------------------------------------------


def test_store_shareholding_round_trip_and_idempotency(tmp_path) -> None:
    store = DuckDbShareholdingStore(tmp_path)
    try:
        rows = fetch_shareholding_patterns_fixture()
        store.upsert_patterns(rows)
        store.upsert_patterns(rows)  # re-upsert -> replace, never duplicate
        assert len(store.all_patterns()) == 36
        latest = store.latest_patterns("500325")
        assert len(latest) == 36
        total = next(row for row in latest if row.category == "total")
        assert total.shareholding_pct == Decimal("100.00")
        assert total.quarter_end == date(2026, 9, 30)
    finally:
        store.close()


def test_store_patterns_as_of_no_lookahead(tmp_path) -> None:
    store = DuckDbShareholdingStore(tmp_path)
    try:
        store.upsert_patterns(fetch_shareholding_patterns_fixture())
        # Before the filing broadcast nothing was known (no look-ahead).
        assert store.patterns_as_of("500325", date(2026, 10, 20)) == []
        assert len(store.patterns_as_of("500325", date(2026, 10, 22))) == 36

        # An older filing stays visible at its own as-of, and the latest
        # filing supersedes it per (quarter, category) after its broadcast.
        older = ShareholdingPattern(
            instrument="500325",
            company_name="Reliance Industries Ltd",
            exchange="BSE",
            filing_date=date(2026, 7, 21),
            quarter_end=date(2026, 6, 30),
            category="promoter_promoter_group",
            no_of_shares=100,
            shareholding_pct=Decimal("45.00"),
        )
        store.upsert_patterns([older])
        at_older = {p.category: p for p in store.patterns_as_of("500325", date(2026, 8, 1))}
        assert at_older["promoter_promoter_group"].quarter_end == date(2026, 6, 30)
        at_latest = {p.category: p for p in store.patterns_as_of("500325", date(2026, 10, 22))}
        assert at_latest["promoter_promoter_group"].quarter_end == date(2026, 9, 30)
    finally:
        store.close()


def test_store_pledge_round_trip_and_idempotency(tmp_path) -> None:
    store = DuckDbShareholdingStore(tmp_path)
    try:
        records = fetch_pledge_records_fixture()
        store.put_pledges(records)
        store.put_pledges(records)  # same quarter -> replace, never duplicate
        assert len(store.all_pledges()) == 12
        latest = store.latest_pledge("20 Microns Limited")
        assert latest is not None
        assert latest.pledged_pct == Decimal("4.42")
        assert latest.quarter_end == date(2026, 9, 30)
        assert store.latest_pledge("No Such Company") is None
    finally:
        store.close()


def test_store_disclosures_round_trip_and_filters(tmp_path) -> None:
    store = DuckDbShareholdingStore(tmp_path)
    try:
        disclosures = fetch_insider_disclosures_fixture("RELIANCE")
        store.upsert_disclosures(disclosures)
        store.upsert_disclosures(disclosures)  # idempotent on disclosure_id
        assert len(store.all_disclosures()) == 20
        assert len(store.disclosures(instrument="RELIANCE")) == 20
        assert len(store.disclosures(instrument="TATAMOTORS")) == 0
        february = store.disclosures(start_date=date(2026, 2, 1), end_date=date(2026, 2, 28))
        assert all(date(2026, 2, 1) <= d.intimation_date <= date(2026, 2, 28) for d in february)
        assert february
    finally:
        store.close()


def test_store_readers_implement_ports(tmp_path) -> None:
    store = DuckDbShareholdingStore(tmp_path)
    try:
        store.upsert_patterns(fetch_shareholding_patterns_fixture())
        store.put_pledges(fetch_pledge_records_fixture())
        store.upsert_disclosures(fetch_insider_disclosures_fixture("RELIANCE"))

        pattern_reader = PythonShareholdingReader(store)
        assert isinstance(pattern_reader, ShareholdingReader)
        assert len(pattern_reader.latest_patterns("500325")) == 36

        pledge_reader = PythonPledgeReader(store)
        assert isinstance(pledge_reader, PledgeReader)
        assert pledge_reader.latest_pledge("20 Microns Limited") is not None

        insider_reader = PythonInsiderDisclosureReader(store)
        assert isinstance(insider_reader, InsiderDisclosureReader)
        assert len(insider_reader.disclosures(instrument="RELIANCE")) == 20
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Ingestion CLI
# ---------------------------------------------------------------------------


def test_ingest_cli_all_datasets_upserts(tmp_path, capsys, monkeypatch) -> None:
    patterns = fetch_shareholding_patterns_fixture()
    pledges = fetch_pledge_records_fixture()
    insiders = fetch_insider_disclosures_fixture("RELIANCE")
    monkeypatch.setattr(
        "tantra_data.cli.ingest_shareholding.fetch_shareholding_patterns",
        lambda scripcode, on_live_error=None: patterns,
    )
    monkeypatch.setattr(
        "tantra_data.cli.ingest_shareholding.fetch_pledge_records",
        lambda on_live_error=None: pledges,
    )
    monkeypatch.setattr(
        "tantra_data.cli.ingest_shareholding.fetch_insider_disclosures",
        lambda symbol, on_live_error=None: insiders,
    )
    rc = ingest_cli_main(["--root", str(tmp_path)])
    captured = capsys.readouterr()
    assert rc == 0
    assert "Ingested 36 shareholding-pattern rows for scripcode 500325" in captured.out
    assert "Ingested 12 promoter-pledge rows" in captured.out
    assert "Ingested 20 insider-disclosure rows for RELIANCE" in captured.out
    assert (tmp_path / "shareholding.duckdb").exists()

    store = DuckDbShareholdingStore(tmp_path)
    try:
        assert len(store.all_patterns()) == 36
        assert len(store.all_pledges()) == 12
        assert len(store.all_disclosures()) == 20
    finally:
        store.close()


def test_ingest_cli_single_dataset_and_warning(tmp_path, capsys, monkeypatch) -> None:
    def failing_pledge(on_live_error=None):
        if on_live_error is not None:
            on_live_error(NsePledgeLiveError("live down", url="https://x", status_code=None))
        return fetch_pledge_records_fixture()

    monkeypatch.setattr("tantra_data.cli.ingest_shareholding.fetch_pledge_records", failing_pledge)
    rc = ingest_cli_main(["--root", str(tmp_path), "--dataset", "pledge"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "Ingested 12 promoter-pledge rows" in captured.out
    assert "WARNING NSE pledge live fetch failed" in captured.err


# ---------------------------------------------------------------------------
# Parquet bridge exports
# ---------------------------------------------------------------------------


def test_export_shareholding_writes_per_filing_date_parquet(tmp_path) -> None:
    store = DuckDbShareholdingStore(tmp_path / "db")
    try:
        store.upsert_patterns(fetch_shareholding_patterns_fixture())
    finally:
        store.close()

    store2 = DuckDbShareholdingStore(tmp_path / "db")
    exporter = LakehouseBridgeExporter(None, None, shareholding=store2)
    try:
        assert exporter.export_shareholding(tmp_path / "catalog") == 1
    finally:
        store2.close()

    files = sorted((tmp_path / "catalog" / "shareholding").glob("*.parquet"))
    assert [file.name for file in files] == ["2026-10-21.parquet"]
    table = pq.read_table(files[0])
    assert table.schema.field("filing_date").type == pa.date32()
    assert table.schema.field("quarter_end").type == pa.date32()
    assert table.schema.field("no_of_shares").type == pa.int64()
    assert table.schema.field("shareholding_pct").type == pa.decimal128(18, 4)
    data = table.to_pydict()
    assert len(data["filing_date"]) == 36
    assert data["shareholding_pct"][0] is not None
    # Percentages survived the projection exactly (Decimal128).
    expected_categories = {"promoter_promoter_group", "public", "fii_fpi", "mutual_funds", "total"}
    assert expected_categories <= set(data["category"])


def test_export_pledge_writes_per_quarter_parquet(tmp_path) -> None:
    store = DuckDbShareholdingStore(tmp_path / "db")
    try:
        store.put_pledges(fetch_pledge_records_fixture())
    finally:
        store.close()

    store2 = DuckDbShareholdingStore(tmp_path / "db")
    exporter = LakehouseBridgeExporter(None, None, pledge=store2)
    try:
        # 12 recorded rows across 2 shareholding quarters (10 at Sep-2026,
        # 2 at Mar-2026).
        assert exporter.export_pledge(tmp_path / "catalog") == 2
    finally:
        store2.close()

    files = sorted((tmp_path / "catalog" / "pledge").glob("*.parquet"))
    assert [file.name for file in files] == ["2026-03-31.parquet", "2026-09-30.parquet"]
    table = pq.read_table(files[1])
    assert table.schema.field("quarter_end").type == pa.date32()
    assert table.schema.field("broadcast_date").type == pa.date32()
    assert table.schema.field("pledged_pct").type == pa.decimal128(18, 4)
    assert table.schema.field("pledged_shares").type == pa.int64()
    data = table.to_pydict()
    assert len(data["instrument"]) == 10
    assert "20 Microns Limited" in data["instrument"]


def test_export_insider_disclosures_writes_per_date_parquet(tmp_path) -> None:
    store = DuckDbShareholdingStore(tmp_path / "db")
    try:
        store.upsert_disclosures(fetch_insider_disclosures_fixture("RELIANCE"))
    finally:
        store.close()

    store2 = DuckDbShareholdingStore(tmp_path / "db")
    exporter = LakehouseBridgeExporter(None, None, insider=store2)
    try:
        # 20 recorded disclosures across 17 distinct intimation dates.
        assert exporter.export_insider_disclosures(tmp_path / "catalog") == 17
    finally:
        store2.close()

    files = sorted((tmp_path / "catalog" / "insider_disclosures").glob("*.parquet"))
    assert files  # per-intimation-date files
    table = pq.read_table(files[0])
    assert table.schema.field("intimation_date").type == pa.date32()
    assert table.schema.field("trade_date").type == pa.date32()
    assert table.schema.field("value").type == pa.decimal128(18, 4)
    assert table.schema.field("before_pct").type == pa.decimal128(18, 4)
    assert table.schema.field("after_pct").type == pa.decimal128(18, 4)
    data = table.to_pydict()
    assert data["instrument"] == ["RELIANCE"] * len(data["instrument"])
    assert all(isinstance(v, int) for v in data["disclosure_id"])


def test_export_shareholding_missing_store_writes_nothing(tmp_path) -> None:
    exporter = LakehouseBridgeExporter(None, None)
    assert exporter.export_shareholding(tmp_path / "catalog") == 0
    assert exporter.export_pledge(tmp_path / "catalog") == 0
    assert exporter.export_insider_disclosures(tmp_path / "catalog") == 0
    assert not (tmp_path / "catalog" / "shareholding").exists()
    assert not (tmp_path / "catalog" / "pledge").exists()
    assert not (tmp_path / "catalog" / "insider_disclosures").exists()
