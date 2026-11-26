"""DuckDB-backed mutual-fund screener store with equity holdings lookthrough (gap 0023).

The store lives in a single DuckDB database at ``{root}/mutual_funds.duckdb`` and
owns five tables:

* ``mf_schemes``    — screen-facing scheme records (category / plan / option / AMC);
* ``mf_fund_stats`` — NAV / AUM / TER / portfolio-turnover stats per scheme and date;
* ``mf_holdings``   — ISIN-level portfolio holdings snapshots (``weight_pct`` per isin);
* ``mf_nav``        — per-scheme NAV history (the AMFI ingest rows, screen-facing shape);
* ``mf_returns``    — derived return records (CAGR / expense ratio).

``DuckDbMutualFundsStore`` is the surface the mutual-fund screener
(``tantra_examples.screening.mutual_funds.screen_mutual_funds``) lazy-imports:
it provides scheme / fund-stats / NAV / return reads, the ``mf_holdings.isin``
↔ ``companies.isin_code`` lookthrough (weighted portfolio ROCE, weighted
portfolio P/E, large/mid/small market-cap breakdown) against a separate
``fundamentals.duckdb`` equity store, and portfolio-overlap queries.

Zero-floats invariant: every financial column is an exact ``DECIMAL`` column and
every aggregation runs in exact ``decimal.Decimal`` — never ``float``. Market-cap
buckets are fixed bands (:data:`LARGE_CAP_MIN_CR` / :data:`MID_CAP_MIN_CR`)
because a standalone ``companies`` table cannot rank the full NSE universe the
way SEBI's live top-100 / next-150 / rest classification does.

Connections are opened and closed per operation: the consumer opens its own
``read_only`` connection to the same database file while screening, and DuckDB
forbids two connections to one file with different configurations coexisting.

Missing data is never fabricated: a store with no holdings for a scheme returns
an empty lookthrough (``coverage_pct == 0``), and a data root whose equity
``companies`` table has no ``isin_code`` raises :class:`MutualFundsStoreError`
with the ``[mutual-funds]`` extra / provision-path hint.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import duckdb

__all__ = [
    "LARGE_CAP_MIN_CR",
    "MID_CAP_MIN_CR",
    "DuckDbMutualFundsStore",
    "MutualFundsStoreError",
]

LARGE_CAP_MIN_CR = Decimal("500000")
"""Market cap (₹ Cr) at or above which a holding classifies as large cap."""

MID_CAP_MIN_CR = Decimal("5000")
"""Market cap (₹ Cr) at or above which a holding classifies as mid cap (else small)."""

_QUANT = Decimal("0.0001")
"""Scale the weighted aggregates are quantized to (4dp, like the stored columns)."""

_CREATE_TABLES: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS mf_schemes (
        scheme_code VARCHAR,
        scheme_name VARCHAR,
        category VARCHAR,
        plan VARCHAR,
        option VARCHAR,
        amc_name VARCHAR,
        asof_date DATE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS mf_fund_stats (
        scheme_code VARCHAR,
        asof_date DATE,
        nav DECIMAL(18, 4),
        ter_pct DECIMAL(18, 4),
        aum_cr DECIMAL(18, 4),
        portfolio_turnover_pct DECIMAL(18, 4)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS mf_holdings (
        scheme_code VARCHAR,
        asof_date DATE,
        isin VARCHAR,
        holding_name VARCHAR,
        weight_pct DECIMAL(18, 4)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS mf_nav (
        scheme_code VARCHAR,
        nav_date DATE,
        nav DECIMAL(18, 4),
        repurchase_price DECIMAL(18, 4),
        sale_price DECIMAL(18, 4),
        source VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS mf_returns (
        scheme_code VARCHAR,
        as_of_date DATE,
        cagr_1y DECIMAL(18, 4),
        expense_ratio DECIMAL(18, 4),
        source VARCHAR
    )
    """,
)

_DEFAULT_DB = Path("data") / "mutual_funds.duckdb"

_COMPANY_COLUMNS = (
    "symbol",
    "company_name",
    "isin_code",
    "market_cap_cr",
    "roce_pct",
    "pe_ratio",
    "debt_to_equity",
)

_MISSING_EQUITY_HINT = (
    "the optional '[mutual-funds]' extra (pip install 'tantra-examples[mutual-funds]') "
    "and provision a TANTRA_DATA_DIR data root with fundamentals.duckdb and "
    "mutual_funds.duckdb (e.g. `python -m tantra_examples.provision.seam`) — "
    "rows are never fabricated"
)


class MutualFundsStoreError(RuntimeError):
    """Raised when the mutual-fund / equity tables a query needs are not provisioned."""


def _decimal(value: object, field: str) -> Decimal | None:
    """Coerce a stored value into an exact ``Decimal`` (or ``None``), never a float."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a decimal value, got {value!r}") from exc


class DuckDbMutualFundsStore:
    """DuckDB mutual-fund screener store (schemes, stats, NAV, holdings lookthrough).

    Mirrors the ``store/option_chain.py`` pattern: the schema is created in
    ``__init__`` (and by ``seed_reference_data``), ``put_*`` DELETE-then-INSERTs
    idempotently, and the read helpers return dict rows with exact ``Decimal``
    money/percentage values.
    """

    def __init__(self, db_path: str | Path | None = None) -> None:
        """Open (creating if needed) the DuckDB store at ``db_path``.

        ``db_path`` defaults to ``data/mutual_funds.duckdb`` under the working
        directory (the ingest CLI's default location). The schema tables are
        created when the file is new; no rows are fabricated.
        """
        self._db_path = Path(db_path) if db_path is not None else _DEFAULT_DB
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self.seed_reference_data()

    @property
    def db_path(self) -> Path:
        """Path of the DuckDB database the store reads and writes."""
        return self._db_path

    # ------------------------------------------------------------------
    # Schema / reference data
    # ------------------------------------------------------------------

    def seed_reference_data(self) -> None:
        """Create the store's schema tables (idempotent; never fabricates rows).

        Called by the consumer when no ``mutual_funds.duckdb`` exists yet so the
        store can be read (an empty store screens to zero candidates rather than
        erroring).
        """
        con = duckdb.connect(str(self._db_path))
        try:
            for ddl in _CREATE_TABLES:
                con.execute(ddl)
        finally:
            con.close()

    # ------------------------------------------------------------------
    # Writes (idempotent upserts, exact Decimal)
    # ------------------------------------------------------------------

    def put_scheme(
        self,
        scheme_code: str,
        scheme_name: str,
        category: str | None,
        plan: str | None,
        option: str | None,
        amc_name: str,
        asof_date: date,
    ) -> None:
        """Insert or replace the scheme record for its code and as-of date."""
        con = duckdb.connect(str(self._db_path))
        try:
            con.execute(
                "DELETE FROM mf_schemes WHERE scheme_code = ? AND asof_date = ?",
                [scheme_code, asof_date],
            )
            con.execute(
                "INSERT INTO mf_schemes VALUES (?, ?, ?, ?, ?, ?, ?)",
                [scheme_code, scheme_name, category, plan, option, amc_name, asof_date],
            )
        finally:
            con.close()

    def put_fund_stats(
        self,
        scheme_code: str,
        asof_date: date,
        nav: Decimal | str,
        ter_pct: Decimal | str,
        aum_cr: Decimal | str,
        portfolio_turnover_pct: Decimal | str | None = None,
    ) -> None:
        """Insert or replace the NAV/AUM/TER stats row for its code and date."""
        con = duckdb.connect(str(self._db_path))
        try:
            con.execute(
                "DELETE FROM mf_fund_stats WHERE scheme_code = ? AND asof_date = ?",
                [scheme_code, asof_date],
            )
            con.execute(
                "INSERT INTO mf_fund_stats VALUES (?, ?, ?, ?, ?, ?)",
                [
                    scheme_code,
                    asof_date,
                    _decimal(nav, "nav"),
                    _decimal(ter_pct, "ter_pct"),
                    _decimal(aum_cr, "aum_cr"),
                    _decimal(portfolio_turnover_pct, "portfolio_turnover_pct"),
                ],
            )
        finally:
            con.close()

    def put_holdings(
        self,
        scheme_code: str,
        asof_date: date,
        holdings: list[tuple[str, str | None, Decimal | str]],
    ) -> None:
        """Replace the scheme's whole holdings snapshot for ``asof_date``.

        ``holdings`` is a list of ``(isin, holding_name, weight_pct)`` tuples.
        Re-putting a snapshot never duplicates rows.
        """
        con = duckdb.connect(str(self._db_path))
        try:
            con.execute(
                "DELETE FROM mf_holdings WHERE scheme_code = ? AND asof_date = ?",
                [scheme_code, asof_date],
            )
            for isin, holding_name, weight in holdings:
                con.execute(
                    "INSERT INTO mf_holdings VALUES (?, ?, ?, ?, ?)",
                    [scheme_code, asof_date, isin, holding_name, _decimal(weight, "weight_pct")],
                )
        finally:
            con.close()

    def put_nav(
        self,
        scheme_code: str,
        nav_date: date,
        nav: Decimal | str,
        repurchase_price: Decimal | str | None = None,
        sale_price: Decimal | str | None = None,
        source: str = "amfi",
    ) -> None:
        """Insert or replace one NAV observation for its scheme and date."""
        con = duckdb.connect(str(self._db_path))
        try:
            con.execute(
                "DELETE FROM mf_nav WHERE scheme_code = ? AND nav_date = ?",
                [scheme_code, nav_date],
            )
            con.execute(
                "INSERT INTO mf_nav VALUES (?, ?, ?, ?, ?, ?)",
                [
                    scheme_code,
                    nav_date,
                    _decimal(nav, "nav"),
                    _decimal(repurchase_price, "repurchase_price"),
                    _decimal(sale_price, "sale_price"),
                    source,
                ],
            )
        finally:
            con.close()

    def put_return(
        self,
        scheme_code: str,
        as_of_date: date,
        cagr_1y: Decimal | str | None = None,
        expense_ratio: Decimal | str | None = None,
        source: str = "amfi",
    ) -> None:
        """Insert or replace one derived-return record for its scheme and date."""
        con = duckdb.connect(str(self._db_path))
        try:
            con.execute(
                "DELETE FROM mf_returns WHERE scheme_code = ? AND as_of_date = ?",
                [scheme_code, as_of_date],
            )
            con.execute(
                "INSERT INTO mf_returns VALUES (?, ?, ?, ?, ?)",
                [
                    scheme_code,
                    as_of_date,
                    _decimal(cagr_1y, "cagr_1y"),
                    _decimal(expense_ratio, "expense_ratio"),
                    source,
                ],
            )
        finally:
            con.close()

    # ------------------------------------------------------------------
    # Reads: schemes / stats / NAV / returns
    # ------------------------------------------------------------------

    def get_schemes(
        self,
        category: str = "all",
        plan: str | None = None,
        option: str | None = None,
    ) -> list[dict[str, Any]]:
        """Scheme records, optionally filtered by category / plan / option.

        ``plan`` / ``option`` match the stored AMFI strings
        (``"Direct Plan"`` / ``"Growth Option"``) by case-insensitive substring,
        so the consumer's ``plan="Direct"``, ``option="Growth"`` filters work.
        Each returned dict carries the keys the screener consumes: ``scheme_code``,
        ``scheme_name``, ``category``, ``plan``, ``option_type``, ``amc_name``.
        """
        clauses: list[str] = []
        params: list[object] = []
        for field, value in (("category", category), ("plan", plan), ("option", option)):
            if value and value != "all":
                clauses.append(f"{field} ILIKE ?")
                params.append(f"%{value}%")
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        con = duckdb.connect(str(self._db_path))
        try:
            rows = con.execute(
                "SELECT scheme_code, scheme_name, category, plan, option, amc_name "
                f"FROM mf_schemes{where} ORDER BY scheme_code",
                params,
            ).fetchall()
        finally:
            con.close()
        return [
            {
                "scheme_code": row[0],
                "scheme_name": row[1],
                "category": row[2],
                "plan": row[3],
                "option_type": row[4],
                "amc_name": row[5],
            }
            for row in rows
        ]

    def get_fund_stats(self, scheme_code: str, asof_date: date) -> dict[str, Any] | None:
        """Latest NAV/AUM/TER stats row with ``asof_date <= asof_date``, or ``None``.

        Money/percentage values are exact ``Decimal``.
        """
        con = duckdb.connect(str(self._db_path))
        try:
            row = con.execute(
                "SELECT scheme_code, asof_date, nav, ter_pct, aum_cr, portfolio_turnover_pct "
                "FROM mf_fund_stats WHERE scheme_code = ? AND asof_date <= ? "
                "ORDER BY asof_date DESC LIMIT 1",
                [scheme_code, asof_date],
            ).fetchone()
        finally:
            con.close()
        if row is None:
            return None
        return {
            "scheme_code": row[0],
            "asof_date": row[1],
            "nav": _decimal(row[2], "nav"),
            "ter_pct": _decimal(row[3], "ter_pct"),
            "aum_cr": _decimal(row[4], "aum_cr"),
            "portfolio_turnover_pct": _decimal(row[5], "portfolio_turnover_pct"),
        }

    def get_nav_history(
        self,
        scheme_code: str,
        start: date | None = None,
        end: date | None = None,
    ) -> list[dict[str, Any]]:
        """NAV observations for ``scheme_code``, optionally bounded by date."""
        clauses = ["scheme_code = ?"]
        params: list[object] = [scheme_code]
        if start is not None:
            clauses.append("nav_date >= ?")
            params.append(start)
        if end is not None:
            clauses.append("nav_date <= ?")
            params.append(end)
        con = duckdb.connect(str(self._db_path))
        try:
            rows = con.execute(
                "SELECT scheme_code, nav_date, nav, repurchase_price, sale_price, source "
                f"FROM mf_nav WHERE {' AND '.join(clauses)} ORDER BY nav_date",
                params,
            ).fetchall()
        finally:
            con.close()
        return [
            {
                "scheme_code": row[0],
                "nav_date": row[1],
                "nav": _decimal(row[2], "nav"),
                "repurchase_price": _decimal(row[3], "repurchase_price"),
                "sale_price": _decimal(row[4], "sale_price"),
                "source": row[5],
            }
            for row in rows
        ]

    def get_returns(
        self, scheme_code: str | None = None, as_of: date | None = None
    ) -> list[dict[str, Any]]:
        """Derived-return records, optionally filtered by scheme / as-of date."""
        clauses: list[str] = []
        params: list[object] = []
        if scheme_code is not None:
            clauses.append("scheme_code = ?")
            params.append(scheme_code)
        if as_of is not None:
            clauses.append("as_of_date <= ?")
            params.append(as_of)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        con = duckdb.connect(str(self._db_path))
        try:
            rows = con.execute(
                "SELECT scheme_code, as_of_date, cagr_1y, expense_ratio, source "
                f"FROM mf_returns{where} ORDER BY as_of_date, scheme_code",
                params,
            ).fetchall()
        finally:
            con.close()
        return [
            {
                "scheme_code": row[0],
                "as_of_date": row[1],
                "cagr_1y": _decimal(row[2], "cagr_1y"),
                "expense_ratio": _decimal(row[3], "expense_ratio"),
                "source": row[4],
            }
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Equity lookthrough
    # ------------------------------------------------------------------

    def get_portfolio_fundamentals(
        self,
        scheme_code: str,
        asof_date: date,
        equity_db_path: str | Path | None = None,
        disclosure_lag_days: int = 10,
    ) -> dict[str, Any]:
        """Weighted portfolio ROCE / P/E / market-cap breakdown for ``scheme_code``.

        Cross-references ``mf_holdings.isin`` with ``companies.isin_code`` in the
        equity ``fundamentals.duckdb`` and aggregates exact-``Decimal`` weighted
        portfolio ROCE, weighted portfolio P/E, weighted debt/equity, and the
        large / mid / small market-cap breakdown, plus ``coverage_pct`` (share of
        the portfolio's weight matched to an equity company).

        Holdings are point-in-time: only the latest snapshot disclosed at least
        ``disclosure_lag_days`` before ``asof_date`` is considered (the AMC
        portfolio-disclosure lag buffer — no look-ahead). When no holdings are
        disclosed within the lag window the result carries ``None`` metrics and
        ``coverage_pct == 0`` — values are never fabricated.

        Raises :class:`MutualFundsStoreError` when the equity store is missing or
        its ``companies`` table has no ``isin_code`` column.
        """
        if equity_db_path is None:
            raise MutualFundsStoreError(
                "mutual-funds lookthrough needs the equity fundamentals.duckdb; "
                f"{_MISSING_EQUITY_HINT}"
            )
        snapshot = self._holdings_snapshot(scheme_code, asof_date, disclosure_lag_days)
        if snapshot is None:
            return self._empty_lookthrough(scheme_code)
        holdings = self._load_holdings(scheme_code, snapshot)
        companies = self._load_companies(Path(equity_db_path))
        return self._aggregate_lookthrough(scheme_code, snapshot, holdings, companies)

    def get_portfolio_overlap(
        self,
        scheme_code: str,
        reference_scheme_code: str,
        asof_date: date | None = None,
    ) -> dict[str, Any]:
        """Portfolio overlap between two schemes as of ``asof_date`` (or the latest data).

        ``overlap_pct`` is ``sum(min(weight_a, weight_b))`` across the ISINs both
        schemes hold (the standard fund-overlap metric: 100 when the portfolios
        are identical, 0 when they share nothing), computed from the latest
        holdings snapshot with ``asof_date <= asof_date`` for each scheme.
        """
        weights_a = self._holdings_weights(scheme_code, asof_date)
        weights_b = self._holdings_weights(reference_scheme_code, asof_date)
        common = set(weights_a) & set(weights_b)
        overlap = Decimal(sum(min(weights_a[isin], weights_b[isin]) for isin in common))
        return {
            "scheme_code": scheme_code,
            "reference_scheme_code": reference_scheme_code,
            "asof_date": asof_date,
            "overlap_pct": overlap.quantize(_QUANT),
            "common_isins": len(common),
            "scheme_isins": len(weights_a),
            "reference_isins": len(weights_b),
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _holdings_snapshot(
        self, scheme_code: str, asof_date: date, disclosure_lag_days: int
    ) -> date | None:
        """Latest holdings disclosure date at least ``disclosure_lag_days`` before ``asof_date``."""
        cutoff = asof_date - timedelta(days=disclosure_lag_days)
        con = duckdb.connect(str(self._db_path))
        try:
            row = con.execute(
                "SELECT MAX(asof_date) FROM mf_holdings WHERE scheme_code = ? AND asof_date <= ?",
                [scheme_code, cutoff],
            ).fetchone()
        finally:
            con.close()
        if row is None or row[0] is None:
            return None
        return row[0]

    def _load_holdings(self, scheme_code: str, snapshot: date) -> list[dict[str, Any]]:
        """Holdings of one scheme snapshot, ordered by weight descending."""
        con = duckdb.connect(str(self._db_path))
        try:
            rows = con.execute(
                "SELECT isin, holding_name, weight_pct FROM mf_holdings "
                "WHERE scheme_code = ? AND asof_date = ? ORDER BY weight_pct DESC",
                [scheme_code, snapshot],
            ).fetchall()
        finally:
            con.close()
        return [
            {
                "isin": row[0],
                "holding_name": row[1],
                "weight_pct": _decimal(row[2], "weight_pct"),
            }
            for row in rows
        ]

    def _holdings_weights(self, scheme_code: str, asof_date: date | None) -> dict[str, Decimal]:
        """Latest holdings snapshot's isin → weight map as of ``asof_date`` (or latest)."""
        con = duckdb.connect(str(self._db_path))
        try:
            if asof_date is None:
                snapshot_row = con.execute(
                    "SELECT MAX(asof_date) FROM mf_holdings WHERE scheme_code = ?",
                    [scheme_code],
                ).fetchone()
                cutoff: date | None = snapshot_row[0] if snapshot_row else None
            else:
                cutoff = asof_date
            if cutoff is None:
                return {}
            rows = con.execute(
                "SELECT isin, weight_pct FROM mf_holdings "
                "WHERE scheme_code = ? AND asof_date = "
                "(SELECT MAX(asof_date) FROM mf_holdings "
                "WHERE scheme_code = ? AND asof_date <= ?)",
                [scheme_code, scheme_code, cutoff],
            ).fetchall()
        finally:
            con.close()
        weights: dict[str, Decimal] = {}
        for row in rows:
            isin, raw_weight = row[0], row[1]
            if isin is None:
                continue
            weight = _decimal(raw_weight, "weight_pct")
            if weight is not None:
                weights[str(isin)] = weight
        return weights

    def _load_companies(self, equity_db_path: Path) -> dict[str, dict[str, Any]]:
        """Map ``companies.isin_code`` → company fundamentals rows from the equity store.

        Raises :class:`MutualFundsStoreError` when the equity store is missing its
        ``companies`` table or the table has no ``isin_code`` column.
        """
        con = duckdb.connect(str(equity_db_path))
        try:
            columns = self._table_columns(con, "companies")
        finally:
            con.close()
        if columns is None:
            raise MutualFundsStoreError(
                f"mutual-funds lookthrough needs a 'companies' table in {equity_db_path}; "
                f"{_MISSING_EQUITY_HINT}"
            )
        if "isin_code" not in columns or "symbol" not in columns:
            raise MutualFundsStoreError(
                f"mutual-funds lookthrough needs companies.isin_code in {equity_db_path}; "
                f"{_MISSING_EQUITY_HINT}"
            )
        select_cols = [col for col in _COMPANY_COLUMNS if col in columns]
        con = duckdb.connect(str(equity_db_path))
        try:
            rows = con.execute(f"SELECT {', '.join(select_cols)} FROM companies").fetchall()
        finally:
            con.close()
        companies: dict[str, dict[str, Any]] = {}
        for row in rows:
            company = dict(zip(select_cols, row, strict=True))
            isin = company.get("isin_code")
            if isin:
                companies[str(isin)] = company
        return companies

    def _aggregate_lookthrough(
        self,
        scheme_code: str,
        snapshot: date,
        holdings: list[dict[str, Any]],
        companies: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """Weighted ROCE / P/E / debt-equity and market-cap breakdown (exact Decimal)."""
        total_weight = sum(holding["weight_pct"] for holding in holdings) or Decimal("1")
        matched: list[dict[str, Any]] = []
        for holding in holdings:
            company = companies.get(holding["isin"])
            if company is not None:
                matched.append({**holding, **company})
        covered_weight = sum(holding["weight_pct"] for holding in matched)
        coverage_pct = (Decimal(100) * covered_weight / total_weight).quantize(_QUANT)

        def weighted(field: str) -> Decimal | None:
            pairs: list[tuple[Decimal, Decimal]] = []
            for m in matched:
                metric = _decimal(m.get(field), field)
                if metric is not None:
                    weight: Decimal = m["weight_pct"]
                    pairs.append((weight, metric))
            if not pairs:
                return None
            weight_sum = sum((w for w, _ in pairs), Decimal("0"))
            total_weighted = sum((w * val for w, val in pairs), Decimal("0"))
            return (total_weighted / weight_sum).quantize(_QUANT)

        banded = [m for m in matched if self._cap_band(m.get("market_cap_cr")) is not None]
        band_total = sum(m["weight_pct"] for m in banded) or Decimal("1")
        band_weights: dict[str, Decimal] = {
            "large": Decimal("0"),
            "mid": Decimal("0"),
            "small": Decimal("0"),
        }
        for m in banded:
            band = self._cap_band(m.get("market_cap_cr"))
            if band is not None:
                band_weights[band] += m["weight_pct"]

        return {
            "scheme_code": scheme_code,
            "asof_date": snapshot,
            "portfolio_roce": weighted("roce_pct"),
            "portfolio_pe": weighted("pe_ratio"),
            "portfolio_debt_to_equity": weighted("debt_to_equity"),
            "large_cap_pct": (Decimal(100) * band_weights["large"] / band_total).quantize(_QUANT),
            "mid_cap_pct": (Decimal(100) * band_weights["mid"] / band_total).quantize(_QUANT),
            "small_cap_pct": (Decimal(100) * band_weights["small"] / band_total).quantize(_QUANT),
            "coverage_pct": coverage_pct,
            "holdings": [
                {
                    "symbol": m["symbol"],
                    "company_name": m.get("company_name"),
                    "isin": m["isin"],
                    "holding_name": m.get("holding_name"),
                    "weight_pct": m["weight_pct"],
                    "market_cap_cr": _decimal(m.get("market_cap_cr"), "market_cap_cr"),
                    "roce_pct": _decimal(m.get("roce_pct"), "roce_pct"),
                    "pe_ratio": _decimal(m.get("pe_ratio"), "pe_ratio"),
                    "debt_to_equity": _decimal(m.get("debt_to_equity"), "debt_to_equity"),
                    "cap_band": self._cap_band(m.get("market_cap_cr")),
                }
                for m in matched
            ],
            "matched_isins": len(matched),
            "total_isins": len(holdings),
        }

    def _empty_lookthrough(self, scheme_code: str) -> dict[str, Any]:
        """Honest empty lookthrough when no holdings are disclosed within the lag window."""
        return {
            "scheme_code": scheme_code,
            "asof_date": None,
            "portfolio_roce": None,
            "portfolio_pe": None,
            "portfolio_debt_to_equity": None,
            "large_cap_pct": Decimal("0"),
            "mid_cap_pct": Decimal("0"),
            "small_cap_pct": Decimal("0"),
            "coverage_pct": Decimal("0"),
            "holdings": [],
            "matched_isins": 0,
            "total_isins": 0,
        }

    def _cap_band(self, market_cap_cr: object) -> str | None:
        """Large / mid / small bucket for a market cap (₹ Cr), or ``None`` when unknown."""
        mcap = _decimal(market_cap_cr, "market_cap_cr")
        if mcap is None:
            return None
        if mcap >= LARGE_CAP_MIN_CR:
            return "large"
        if mcap >= MID_CAP_MIN_CR:
            return "mid"
        return "small"

    @staticmethod
    def _table_columns(con: duckdb.DuckDBPyConnection, table: str) -> set[str] | None:
        """Column names of ``table`` in ``con``, or ``None`` when the table is absent."""
        try:
            rows = con.execute(f"DESCRIBE {table}").fetchall()
        except duckdb.CatalogException:
            return None
        return {str(row[0]) for row in rows}
