"""BSE LODR Reg 31 shareholding-pattern provider (gap 0024).

Live source (verified 2026-11-17 from the sandbox)::

    https://api.bseindia.com/BseIndiaAPI/api/Corp_Shareholding_ng/w
        ?scripcode={SCRIP}&flag=0&indtype=

returns a JSON ``Table`` of quarterly shareholding-pattern filings (one row per
quarter, carrying ``EndDate``, the broadcast ``D``, and an ``XBRLAttachment``
path); the latest filing is an **iXBRL** document at
``https://www.bseindia.com{XBRLAttachment}`` with structured SEBI taxonomy
facts (``ix:nonFraction`` tags such as
``in-bse-shp:ShareholdingAsAPercentageOfTotalNumberOfShares`` under
``CategoryOfShareholdersAxis`` contexts). Both endpoints were probed live and
return HTTP 200 with real JSON/HTML payloads.

The API gate is a **header-shape / TLS-fingerprint check**: the repo's
``curl_cffi`` Chrome-impersonating session pattern (as in
``providers/institutional_flow.py``) is required — a bare ``curl`` client is
answered with an Akamai ``403 Access Denied`` shell. ``curl_cffi`` with
Chrome impersonation gets clean JSON.

Live data is never fabricated: the provider uses the repo's live-with-stub-
fallback pattern and falls back to the bundled recorded fixture
``tests/fixtures/shareholding_bse_ixbrl.json`` (labelled ``source: fixture``),
whose rows were recorded verbatim from the live BSE iXBRL filing for Reliance
Industries Ltd (scripcode 500325, quarter ended 2026-09-30, broadcast
2026-10-21). The precise live failure is surfaced through the optional
``on_live_error`` callback as :class:`BseShareholdingLiveError`; the current
status is pinned machine-readably in :data:`LIVE_VERIFICATION`.

Only the SEBI **summary categories** (the ``_ContextI`` contexts of the
``CategoryOfShareholdersAxis``) are captured — promoter & promoter group,
FII/FPI, DII, mutual funds, public, etc. The per-entity sub-rows (each mutual
fund scheme, each shareholder slab) are out of scope for the pattern dataset.

Zero-floats invariant: every percentage is parsed as an exact
``decimal.Decimal``; every share count is an exact ``int`` — no ``float``.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from tantra_data.domain.shareholding import ShareholdingPattern

__all__ = [
    "BSE_HOME_URL",
    "LIVE_VERIFICATION",
    "SHAREHOLDING_INDEX_URL",
    "BseShareholdingLiveError",
    "QuarterlyShareholdingFiling",
    "fetch_shareholding_patterns",
    "fetch_shareholding_patterns_fixture",
    "normalize_category",
    "parse_bse_ixbrl",
    "parse_shareholding_index",
]

BSE_HOME_URL = "https://www.bseindia.com"
SHAREHOLDING_INDEX_URL = f"{BSE_HOME_URL}/BseIndiaAPI/api/Corp_Shareholding_ng/w"
REFERER_URL = BSE_HOME_URL
REQUEST_TIMEOUT_SECONDS = 30
DATE_FORMAT = "%Y-%m-%d"
NSE_DATE_FORMAT = "%d-%b-%Y"
_SUMMARY_CONTEXT_SUFFIX = "_ContextI"

LIVE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": REFERER_URL,
    "Accept": "application/json, text/plain, */*",
}

#: Current live-verification status of the BSE shareholding-pattern feed
#: (checked 2026-11-17 from the sandbox): the index endpoint and the iXBRL
#: filing both returned HTTP 200 with real payloads via the repo's
#: ``curl_cffi`` Chrome-impersonating session. Kept machine-readable so
#: tooling and tests can assert the provider's honest verification state.
LIVE_VERIFICATION = (
    "verified-live (checked 2026-11-17): api.bseindia.com Corp_Shareholding_ng/w "
    "returned HTTP 200 application/json (107 quarterly rows for scripcode=500325) "
    "and the latest iXBRL filing downloaded HTTP 200 text/html (1.27 MB, structured "
    "in-bse-shp taxonomy facts incl. ShareholdingAsAPercentageOfTotalNumberOfShares). "
    "A bare curl client receives an Akamai 403 Access Denied shell; the curl_cffi "
    "Chrome-impersonating session is required."
)

#: Normalizes SEBI ``CategoryOfShareholdersAxis`` member names to the canonical
#: snake_case categories the domain/consumers expect. Unknown members are kept
#: lowercased verbatim (never dropped, never fabricated).
_CATEGORY_ALIASES: dict[str, str] = {
    "ShareholdingOfPromoterAndPromoterGroup": "promoter_promoter_group",
    "PublicShareholding": "public",
    "SharesHeldByNonPromoterNonPublicShareholders": "non_promoter_non_public",
    "ShareholdingPattern": "total",
    "InstitutionsDomestic": "institutions_domestic",
    "MutualFundsOrUTI": "mutual_funds",
    "Banks": "banks",
    "InsuranceCompanies": "insurance",
    "InstitutionsForeign": "fii_fpi",
    "InstitutionsForeignPortfolioInvestorCategoryOne": "fpi_category_one",
    "InstitutionsForeignPortfolioInvestorCategoryTwo": "fpi_category_two",
    "OtherInstitutionsForeign": "other_foreign_institutions",
    "NonInstitutions": "non_institutions",
    "IndividualsOrHinduUndividedFamily": "individuals_huf",
    "CentralGovernmentOrPresidentOfIndia": "central_government",
    "StateGovernmentsOrGovernors": "state_governments",
    "Governments": "governments",
    "OtherFinancialInstitutions": "other_financial_institutions",
    "NBFCsRegisteredWithRBI": "nbfcs",
    "DirectorsAndDirectorsRelatives": "directors_and_relatives",
    "KeyManagerialPersonnel": "key_managerial_personnel",
    "NonResidentIndians": "non_resident_indians",
    "ForeignNationals": "foreign_nationals",
    "ForeignDirectInvestment": "foreign_direct_investment",
    "ProvidentFundsOrPensionFunds": "provident_pension_funds",
    "InvestorEducationAndProtectionFund": "investor_education_protection_fund",
    "CustodianOrDRHolder": "custodian_dr_holders",
    "SovereignWealthFundsDomestic": "sovereign_wealth_funds_domestic",
    "AlternativeInvestmentFunds": "alternative_investment_funds",
    "BodiesCorporate": "bodies_corporate",
    "AssociateCompaniesOrSubsidiaries": "associate_companies_subsidiaries",
    "RelativesOfPromotersOtherThanPromoterGroup": "promoter_relatives_other_group",
    "ShareholdingByCompaniesOrBodiesCorporateWhereCentralOrStateGovernmentIsPromoter": (
        "government_promoted_corporates"
    ),
    "ResidentIndividualShareholdersHoldingNominalShareCapitalInExcessOfRsTwoLakh": (
        "individuals_above_2l"
    ),
    "ResidentIndividualShareholdersHoldingNominalShareCapitalUpToRsTwoLakh": (
        "individuals_up_to_2l"
    ),
    "OtherIndianShareholders": "other_indian_shareholders",
    "OtherNonInstitutions": "other_non_institutions",
    "Indian": "indian",
}

_FIXTURE_PATH = (
    Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "shareholding_bse_ixbrl.json"
)

_FACT_RE = re.compile(
    r"<ix:(nonFraction|nonNumeric)\s+name='([^']+)'\s+contextRef='([^']+)'"
    r"(?:\s+decimals='[^']*')?\s+unitRef='[^']*'[^>]*>(.*?)</ix:\1>",
    re.DOTALL,
)


@dataclass(frozen=True)
class QuarterlyShareholdingFiling:
    """One quarterly shareholding-pattern filing row from the BSE index."""

    scripcode: str
    company_name: str
    quarter_label: str
    filing_date: date
    quarter_end: date
    is_xbrl: bool
    xbrl_attachment: str | None


class BseShareholdingLiveError(RuntimeError):
    """A precise failure from the live BSE shareholding-pattern feed.

    ``status_code`` is the HTTP status when the endpoint responded (``None``
    for transport-level failures such as timeouts); ``url`` identifies the
    request. Callers use this to distinguish a blocked endpoint (TLS
    fingerprint / redirect), a transport failure, or a malformed payload.
    """

    def __init__(
        self,
        message: str,
        *,
        url: str,
        status_code: int | None,
    ) -> None:
        super().__init__(message)
        self.url = url
        self.status_code = status_code


def normalize_category(member: str) -> str:
    """Map a SEBI category member name to the canonical snake_case label."""
    key = member.rsplit(":", 1)[-1]
    if key in _CATEGORY_ALIASES:
        return _CATEGORY_ALIASES[key]
    # Fallback: snake_case the raw member, dropping an axis suffix if any.
    result = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", key).lower()
    return result.replace("__", "_")


def _parse_iso_date(text: str) -> date:
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        try:
            return _parse_nse_date(text)
        except ValueError as exc:
            raise ValueError(f"invalid date {text!r}") from exc


def _parse_nse_date(text: str) -> date:
    from datetime import datetime

    return datetime.strptime(text.strip(), NSE_DATE_FORMAT).date()


def parse_shareholding_index(text: str) -> list[QuarterlyShareholdingFiling]:
    """Parse the BSE shareholding index JSON into filing rows.

    The verified live payload is an object with a ``Table`` array of quarterly
    filing rows shaped like::

        {"FLD_ScripCode": 500325, "Company_NAme": "Reliance Industries Ltd",
         "sQtrName": "September 2026", "EndDate": "2026-09-30T00:00:00",
         "D": "2026-10-21T15:48:00", "IsXBRL": 1,
         "XBRLAttachment": "/XBRLFILES/SHPXBRLDataXML/500325_...._SP.html", ...}

    Returns the rows newest-first (the index is already newest-first).
    """
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError("BSE shareholding index is not valid JSON") from exc
    table = payload.get("Table") if isinstance(payload, dict) else None
    if not isinstance(table, list):
        raise ValueError("BSE shareholding index is missing its Table array")
    return [_parse_index_row(element) for element in table]


def _parse_index_row(element: object) -> QuarterlyShareholdingFiling:
    if not isinstance(element, dict):
        raise ValueError("BSE shareholding index rows must be JSON objects")
    scripcode = element.get("FLD_ScripCode")
    if scripcode is None:
        raise ValueError(f"BSE shareholding index row missing a scripcode: {element!r}")
    filing_date_text = element.get("D")
    quarter_end_text = element.get("EndDate")
    if not isinstance(filing_date_text, str) or not isinstance(quarter_end_text, str):
        raise ValueError(f"BSE shareholding index row missing dates: {element!r}")
    is_xbrl = element.get("IsXBRL") in (1, "1")
    attachment = element.get("XBRLAttachment")
    return QuarterlyShareholdingFiling(
        scripcode=str(scripcode),
        company_name=str(element.get("Company_NAme", "")).strip(),
        quarter_label=str(element.get("sQtrName", "")).strip(),
        filing_date=_parse_iso_date(filing_date_text),
        quarter_end=_parse_iso_date(quarter_end_text),
        is_xbrl=is_xbrl,
        xbrl_attachment=attachment if isinstance(attachment, str) else None,
    )


def parse_bse_ixbrl(
    html: str,
    *,
    scripcode: str,
    company_name: str,
    filing_date: date,
    quarter_end: date,
    exchange: str = "BSE",
) -> list[ShareholdingPattern]:
    """Parse a BSE shareholding-pattern iXBRL filing into pattern rows.

    Extracts the SEBI summary-category facts (``_ContextI`` contexts of the
    ``CategoryOfShareholdersAxis``): the ``ShareholdingAsAPercentageOfTotal-
    NumberOfShares`` percentage and the fully paid-up share count per category.
    Rows whose summary percentage fact is missing are skipped (a category with
    neither the percentage nor the share count is unusable). Raises
    ``ValueError`` when the HTML carries no parseable facts.
    """
    facts: dict[tuple[str, str], str] = {}
    for match in _FACT_RE.finditer(html):
        concept = match.group(2).split(":")[-1]
        facts[(match.group(3), concept)] = match.group(4).strip()
    if not facts:
        raise ValueError("BSE shareholding iXBRL contains no structured facts")

    rows: list[ShareholdingPattern] = []
    for (context, concept), value in facts.items():
        if concept != "ShareholdingAsAPercentageOfTotalNumberOfShares":
            continue
        if not context.endswith(_SUMMARY_CONTEXT_SUFFIX):
            continue
        member = context[: -len(_SUMMARY_CONTEXT_SUFFIX)]
        shares_text = facts.get((context, "NumberOfFullyPaidUpEquityShares")) or facts.get(
            (context, "NumberOfShares")
        )
        try:
            no_of_shares = int(str(shares_text).strip())
            shareholding_pct = Decimal(str(value).strip())
        except (ValueError, InvalidOperation, TypeError) as exc:
            raise ValueError(
                f"BSE shareholding row has invalid numeric fields for {context!r}"
            ) from exc
        rows.append(
            ShareholdingPattern(
                instrument=str(scripcode),
                company_name=company_name,
                exchange=exchange,
                filing_date=filing_date,
                quarter_end=quarter_end,
                category=normalize_category(member),
                no_of_shares=no_of_shares,
                shareholding_pct=shareholding_pct,
                source="bse",
            )
        )
    if not rows:
        raise ValueError("BSE shareholding iXBRL contains no summary-category facts")
    return rows


def read_bse_ixbrl(
    path: str | os.PathLike[str],
    *,
    scripcode: str,
    company_name: str,
    filing_date: date,
    quarter_end: date,
) -> list[ShareholdingPattern]:
    """Read and parse a BSE shareholding iXBRL HTML file from disk."""
    with open(path, encoding="utf-8") as handle:
        return parse_bse_ixbrl(
            handle.read(),
            scripcode=scripcode,
            company_name=company_name,
            filing_date=filing_date,
            quarter_end=quarter_end,
        )


def _requests_session() -> Any:
    """Create a Chrome-impersonating session (lazy ``curl_cffi`` import)."""
    from curl_cffi import requests

    return requests.Session(impersonate="chrome120")


def fetch_shareholding_patterns(
    scripcode: str,
    on_live_error: Callable[[BseShareholdingLiveError], None] | None = None,
) -> list[ShareholdingPattern]:
    """Fetch the latest shareholding-pattern filing for ``scripcode``.

    Live-with-stub-fallback (mirrors ``providers/trading_holidays.py``):
    fetches the quarterly filing index, takes the newest iXBRL filing, and
    parses its structured facts. On any failure (offline, blocked endpoint,
    HTTP error, or a payload without iXBRL facts) it falls back to the bundled
    recorded fixture (``source: fixture``) so the pipeline stays runnable
    offline — the fixture is never presented as live data.

    The precise live failure is passed to ``on_live_error`` (when given) as a
    :class:`BseShareholdingLiveError` carrying the ``url`` and HTTP
    ``status_code`` (``None`` for transport failures).
    """
    try:
        return _fetch_live_shareholding_patterns(scripcode)
    except Exception as exc:
        if not isinstance(exc, BseShareholdingLiveError):
            exc = BseShareholdingLiveError(
                f"BSE shareholding live fetch failed: {exc}",
                url=SHAREHOLDING_INDEX_URL,
                status_code=None,
            )
        if on_live_error is not None:
            on_live_error(exc)
        return fetch_shareholding_patterns_fixture()


def _fetch_live_shareholding_patterns(scripcode: str) -> list[ShareholdingPattern]:
    """Fetch and parse the latest live BSE iXBRL filing (two HTTP calls).

    Every failure mode is wrapped in :class:`BseShareholdingLiveError`: a
    non-200 response, a malformed index, a missing XBRL attachment, and
    transport errors.
    """
    session: Any = _requests_session()
    try:
        session.get(BSE_HOME_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    except Exception as exc:
        raise BseShareholdingLiveError(
            f"BSE home fetch failed: {exc}", url=BSE_HOME_URL, status_code=None
        ) from exc
    status_code: int | None = None
    try:
        index_response = session.get(
            SHAREHOLDING_INDEX_URL,
            params={"scripcode": scripcode, "flag": "0", "indtype": ""},
            headers=LIVE_HEADERS,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        status_code = int(index_response.status_code)
    except Exception as exc:
        raise BseShareholdingLiveError(
            f"BSE shareholding index fetch failed: {exc}",
            url=SHAREHOLDING_INDEX_URL,
            status_code=None,
        ) from exc
    if status_code != 200:
        raise BseShareholdingLiveError(
            f"BSE shareholding index fetch failed with HTTP {status_code} for "
            f"scripcode={scripcode}",
            url=SHAREHOLDING_INDEX_URL,
            status_code=status_code,
        )
    try:
        filings = parse_shareholding_index(index_response.text)
    except ValueError as exc:
        raise BseShareholdingLiveError(
            f"BSE shareholding index returned an unusable payload: {exc}",
            url=SHAREHOLDING_INDEX_URL,
            status_code=status_code,
        ) from exc
    latest = next((filing for filing in filings if filing.is_xbrl and filing.xbrl_attachment), None)
    if latest is None:
        raise BseShareholdingLiveError(
            f"BSE shareholding index returned no iXBRL filing for scripcode={scripcode}",
            url=SHAREHOLDING_INDEX_URL,
            status_code=status_code,
        )
    attachment_url = f"{BSE_HOME_URL}{latest.xbrl_attachment}"
    try:
        filing_response = session.get(
            attachment_url,
            headers=LIVE_HEADERS,
            timeout=REQUEST_TIMEOUT_SECONDS * 2,
        )
        filing_status = int(filing_response.status_code)
    except Exception as exc:
        raise BseShareholdingLiveError(
            f"BSE shareholding iXBRL download failed: {exc}",
            url=attachment_url,
            status_code=None,
        ) from exc
    if filing_status != 200:
        raise BseShareholdingLiveError(
            f"BSE shareholding iXBRL download failed with HTTP {filing_status}",
            url=attachment_url,
            status_code=filing_status,
        )
    try:
        return parse_bse_ixbrl(
            filing_response.text,
            scripcode=scripcode,
            company_name=latest.company_name,
            filing_date=latest.filing_date,
            quarter_end=latest.quarter_end,
        )
    except ValueError as exc:
        raise BseShareholdingLiveError(
            f"BSE shareholding iXBRL returned an unusable payload: {exc}",
            url=attachment_url,
            status_code=filing_status,
        ) from exc


def fetch_shareholding_patterns_fixture() -> list[ShareholdingPattern]:
    """Return the bundled recorded fixture rows (``source: fixture``).

    The fixture must be labelled ``source: fixture``; any payload that is not
    is rejected so a mislabelled fallback can never masquerade as live data.
    """
    payload = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
    if payload.get("source") != "fixture":
        raise ValueError("fixture fallback payload must be labelled source=fixture")
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise ValueError("shareholding fixture is missing its rows array")
    return [
        ShareholdingPattern(
            instrument=str(payload["scripcode"]),
            company_name=str(payload["company_name"]),
            exchange=str(payload.get("exchange", "BSE")),
            filing_date=_parse_iso_date(payload["filing_date"]),
            quarter_end=_parse_iso_date(payload["quarter_end"]),
            category=normalize_category(str(row["category"])),
            no_of_shares=int(row["no_of_shares"]),
            shareholding_pct=Decimal(str(row["shareholding_pct"])),
            source="fixture",
        )
        for row in rows
    ]
