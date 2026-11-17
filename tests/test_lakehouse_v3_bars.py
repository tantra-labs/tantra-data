"""Offline tests for the bhavcopy archive parser, v3 writers and ingest CLI."""

from __future__ import annotations

import io
import lzma
import zipfile
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tantra_data.adapters.lakehouse_v3 import (
    aliases_path,
    bars_path,
    read_daily_bars,
    write_aliases,
    write_daily_bars,
)
from tantra_data.cli.ingest_bars_v3 import (
    SessionNotPublished,
    download_missing_sessions,
    main,
    session_url,
)
from tantra_data.providers.bhavcopy_archive import (
    BhavcopyArchiveError,
    Dialect,
    isin_index_from_frames,
    merge_sessions,
    parse_archive_bhavcopy,
    read_archive_file,
    resolve_missing_isins,
)

UDIFF_HDR = (
    "TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb,SctySrs,XpryDt,"
    "FininstrmActlXpryDt,StrkPric,OptnTp,FinInstrmNm,OpnPric,HghPric,LwPric,ClsPric,"
    "LastPric,PrvsClsgPric,UndrlygPric,SttlmPric,OpnIntrst,ChngInOpnIntrst,TtlTradgVol,"
    "TtlTrfVal,TtlNbOfTxsExctd,SsnId,NewBrdLotQty,Rmks,Rsvd1,Rsvd2,Rsvd3,Rsvd4\n"
)


def udiff_row(
    day: str, tp: str, isin: str, sym: str, series: str, o: str, h: str, lo: str, c: str, pc: str
) -> str:
    return (
        f"{day},{day},CM,NSE,{tp},1,{isin},{sym},{series},,,,,NAME,{o},{h},{lo},{c},{c},{pc},,{c},,,"
        f"1000,50000.50,12,F1,1,,,,,\n"
    )


UDIFF = (
    UDIFF_HDR
    + udiff_row(
        "2026-10-13",
        "STK",
        "INE001A01036",
        "AAA",
        "EQ",
        "10.50",
        "11.00",
        "10.00",
        "10.75",
        "10.25",
    )
    + udiff_row("2026-10-13", "STK", "INE002A01018", "BBB", "BE", "20", "21", "19", "20.5", "20")
    + udiff_row("2026-10-13", "STK", "INE003A01010", "CCC", "GB", "1", "1", "1", "1", "1")
    + udiff_row("2026-10-13", "IDF", "INE004A01010", "DDD", "EQ", "1", "1", "1", "1", "1")
)

LEGACY = (
    "SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,\n"
    "AAA,EQ,10.5,11,10,10.75,10.7,10.25,500,5375.5,03-JAN-2022,7,INE001A01036,\n"
    "XXX,GS,95.05,95.05,95.05,95.05,95.05,100,10,950.5,03-JAN-2022,1,IN0020010081,\n"
)

SEC = (
    "SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, LAST_PRICE, "
    "CLOSE_PRICE, "
    "AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS, NO_OF_TRADES, DELIV_QTY, DELIV_PER\n"
    "AAA, EQ, 25-Jan-2022, 90.05, 89.95, 97.00, 85.15, 94.30, 94.70, 93.54, 520109, "
    "486.53, 8595, 231284, 44.47\n"
    "BBB, EQ, 25-Jan-2022, 5, 5, 6, 4, 5, 5, 5, 10, -, -, -, -\n"
    "ZZZ, EQ, 25-Jan-2022, 5, 5, 6, 4, 5, 5, 5, 10, 1.0, 3, -, -\n"
    "AAA, BL, 25-Jan-2022, 5, 5, 6, 4, 5, 5, 5, 10, 1.0, 3, -, -\n"
)


def test_udiff_parse() -> None:
    dialect, df = parse_archive_bhavcopy(UDIFF)
    assert dialect is Dialect.UDIFF
    assert df["symbol"].to_list() == ["AAA", "BBB"]
    row = df.row(0, named=True)
    assert row["isin"] == "INE001A01036"
    assert row["trade_date"] == date(2026, 10, 13)
    assert row["open"] == Decimal("10.5000")
    assert row["prev_close"] == Decimal("10.2500")
    assert row["turnover"] == Decimal("50000.5000")
    assert row["trades"] == 12
    assert df.schema["open"] == pl.Decimal(18, 4)


def test_legacy_parse() -> None:
    dialect, df = parse_archive_bhavcopy(LEGACY)
    assert dialect is Dialect.LEGACY
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["trade_date"] == date(2022, 1, 3)
    assert row["turnover"] == Decimal("5375.5000")
    assert row["volume"] == Decimal("500.0000")


def test_sec_parse() -> None:
    dialect, df = parse_archive_bhavcopy(SEC)
    assert dialect is Dialect.SEC_BHAVDATA
    assert df["symbol"].to_list() == ["AAA", "BBB", "ZZZ"]
    row = df.row(0, named=True)
    assert row["isin"] is None
    assert row["trade_date"] == date(2022, 1, 25)
    assert row["close"] == Decimal("94.7000")
    assert row["turnover"] == Decimal("48653000.0000")
    assert df.row(1, named=True)["turnover"] is None
    assert df.row(1, named=True)["trades"] is None


def test_clamp_and_negative_volume() -> None:
    text = UDIFF_HDR + udiff_row(
        "2026-10-13", "STK", "INE001A01036", "AAA", "EQ", "10", "9", "11", "10.5", "10"
    )
    text = text.replace(",1000,50000.50,", ",-5,50000.50,")
    _, df = parse_archive_bhavcopy(text)
    row = df.row(0, named=True)
    assert row["high"] == Decimal("10.5000")
    assert row["low"] == Decimal("10.0000")
    assert row["volume"] == Decimal("0.0000")


def test_unknown_header_and_bad_price() -> None:
    with pytest.raises(BhavcopyArchiveError):
        parse_archive_bhavcopy("A,B,C\n1,2,3\n")
    with pytest.raises(BhavcopyArchiveError):
        parse_archive_bhavcopy(
            UDIFF_HDR + udiff_row("2026-10-13", "STK", "I", "A", "EQ", "x", "1", "1", "1", "1")
        )


def test_read_archive_file_unknown_header_names_file(tmp_path: Path) -> None:
    p = tmp_path / "bhavcopy_20220101.csv.xz"
    p.write_bytes(lzma.compress(b"A,B\n1,2\n"))
    with pytest.raises(BhavcopyArchiveError, match="bhavcopy_20220101"):
        read_archive_file(p)


def test_isin_resolution() -> None:
    _, sec = parse_archive_bhavcopy(SEC)
    index = isin_index_from_frames(
        [
            pl.DataFrame(
                {
                    "symbol": ["AAA", "AAA", "AAA"],
                    "trade_date": [date(2022, 1, 10), date(2022, 1, 20), date(2022, 2, 1)],
                    "isin": ["OLD", "NEAR", "LATER"],
                }
            )
        ]
    )
    out, dropped = resolve_missing_isins(sec, index)
    assert dropped == 2  # BBB, ZZZ have no ISIN anywhere
    assert out["symbol"].to_list() == ["AAA"]
    assert out["isin"].to_list() == ["NEAR"]  # latest on/before 25-Jan wins over later
    early = sec.filter(pl.col("symbol") == "AAA").with_columns(
        pl.lit(date(2022, 1, 1)).alias("trade_date")
    )
    out2, _ = resolve_missing_isins(early, index)
    assert out2["isin"].to_list() == ["OLD"]  # fallback: earliest after


def test_merge_precedence() -> None:
    def frame(isin: str, close: str) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "isin": [isin],
                "symbol": ["A"],
                "series": ["EQ"],
                "trade_date": [date(2022, 1, 3)],
                "open": [Decimal(close)],
                "high": [Decimal(close)],
                "low": [Decimal(close)],
                "close": [Decimal(close)],
                "prev_close": [Decimal(close)],
                "volume": [Decimal("1")],
                "turnover": [None],
                "trades": [None],
            },
            schema=parse_archive_bhavcopy(UDIFF)[1].schema,
        )

    merged = merge_sessions(
        [
            (Dialect.SEC_BHAVDATA, frame("I1", "1")),
            (Dialect.UDIFF, frame("I1", "3")),
            (Dialect.LEGACY, frame("I1", "2")),
        ]
    )
    assert merged.height == 1
    assert merged["source"].to_list() == ["udiff"]
    assert merged["close"].to_list() == [Decimal("3.0000")]


def _bars(rows: list[tuple[str, date, str]]) -> pl.DataFrame:
    schema = parse_archive_bhavcopy(UDIFF)[1].schema
    d = lambda v: [Decimal(r[2]) for r in rows]  # noqa: E731
    return pl.DataFrame(
        {
            "instrument_uid": [r[0] for r in rows],
            "symbol": [r[0] for r in rows],
            "series": ["EQ"] * len(rows),
            "trade_date": [r[1] for r in rows],
            "open": d(0),
            "high": d(0),
            "low": d(0),
            "close": d(0),
            "prev_close": d(0),
            "volume": d(0),
            "turnover": [None] * len(rows),
            "trades": [None] * len(rows),
            "source": ["udiff"] * len(rows),
        },
        schema={
            "instrument_uid": pl.Utf8,
            **{k: v for k, v in schema.items() if k != "isin"},
            "source": pl.Utf8,
        },
    )


def test_writer_layout_schema_idempotent_and_new_wins(tmp_path: Path) -> None:
    first = _bars(
        [
            ("B", date(2022, 1, 3), "1"),
            ("A", date(2022, 1, 4), "1"),
            ("A", date(2022, 1, 3), "1"),
            ("A", date(2023, 1, 2), "5"),
        ]
    )
    assert write_daily_bars(tmp_path, first) == {2022: 3, 2023: 1}
    assert write_daily_bars(tmp_path, first) == {2022: 3, 2023: 1}
    path = (
        tmp_path
        / "market/bars/timeframe=1d/exchange=NSE/asset_class=equity/year=2022/part-0000.parquet"
    )
    assert path == bars_path(tmp_path, 2022)
    assert path.exists()
    assert not list(path.parent.glob("*.tmp"))
    schema = pq.read_schema(path)
    assert schema.field("open").type == pa.decimal128(18, 4)
    assert schema.field("turnover").nullable
    assert schema.field("ts").type == pa.timestamp("ns", tz="UTC")
    assert schema.field("trade_date").type == pa.date32()
    df = read_daily_bars(tmp_path, years=[2022])
    assert df["instrument_uid"].to_list() == ["A", "A", "B"]
    assert df["trade_date"].to_list() == [date(2022, 1, 3), date(2022, 1, 4), date(2022, 1, 3)]
    assert df["ts"][0] == datetime(2022, 1, 3, 3, 45, tzinfo=UTC)
    # new wins on overlap, other rows preserved
    write_daily_bars(tmp_path, _bars([("A", date(2022, 1, 3), "9"), ("C", date(2022, 1, 3), "2")]))
    df = read_daily_bars(tmp_path, years=[2022])
    assert df.height == 4
    a = df.filter((pl.col("instrument_uid") == "A") & (pl.col("trade_date") == date(2022, 1, 3)))
    assert a["close"].to_list() == [Decimal("9.0000")]


def test_aliases_exclusive_and_merge(tmp_path: Path) -> None:
    def al(f: date, t: date) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "symbol": ["A"],
                "series": ["EQ"],
                "instrument_uid": ["I1"],
                "valid_from": [f],
                "valid_to": [t],
            }
        )

    assert write_aliases(tmp_path, al(date(2022, 1, 5), date(2022, 1, 11))) == 1
    assert write_aliases(tmp_path, al(date(2022, 1, 1), date(2022, 1, 8))) == 1
    assert write_aliases(tmp_path, al(date(2022, 3, 1), date(2022, 3, 2))) == 1
    t = pq.read_table(aliases_path(tmp_path))
    assert t.num_rows == 1
    row = t.to_pylist()[0]
    assert row["valid_from"] == date(2022, 1, 1)
    assert row["valid_to"] == date(2022, 3, 2)
    assert row["exchange"] == "NSE"


def _xz(path: Path, text: str) -> None:
    path.write_bytes(lzma.compress(text.encode()))


def test_cli_end_to_end(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    _xz(cache / "bhavcopy_20221231.csv.xz", UDIFF)  # misleading filename date
    _xz(cache / "bhavcopy_20220103.csv.xz", LEGACY)
    _xz(cache / "bhavcopy_20220126.csv.xz", SEC)
    root = tmp_path / "root"
    assert main(["--cache-dir", str(cache), "--root", str(root), "--dry-run"]) == 0
    assert not root.exists()
    assert main(["--cache-dir", str(cache), "--root", str(root)]) == 0
    assert bars_path(root, 2026).exists()
    df = read_daily_bars(root)
    # udiff AAA,BBB; legacy AAA; sec AAA,BBB resolved via index, ZZZ dropped
    assert df.height == 5
    assert set(df["source"].to_list()) == {"udiff", "legacy", "sec_bhavdata"}
    assert aliases_path(root).exists()
    # idempotent
    assert main(["--cache-dir", str(cache), "--root", str(root)]) == 0
    assert read_daily_bars(root).height == 5


def test_session_url_by_date() -> None:
    assert session_url(date(2024, 7, 8)).endswith("BhavCopy_NSE_CM_0_0_0_20240708_F_0000.csv.zip")
    assert session_url(date(2024, 7, 5)).endswith(
        "/historical/EQUITIES/2024/JUL/cm05JUL2024bhav.csv.zip"
    )


def test_download_missing_sessions(tmp_path: Path) -> None:
    requested: list[str] = []
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("x.csv", LEGACY)
    payload = buf.getvalue()

    def fetch(url: str) -> bytes:
        requested.append(url)
        if "20240708" in url:
            return payload
        if "20240709" in url:
            raise SessionNotPublished(url)
        raise OSError("boom")

    # Fri 5 Jul (OSError), Sat/Sun skipped, Mon 8 (ok), Tue 9 (holiday)
    rep = download_missing_sessions(
        tmp_path, date(2024, 7, 5), date(2024, 7, 9), workers=2, fetch=fetch
    )
    assert (rep.downloaded, rep.holidays, len(rep.failures)) == (1, 1, 1)
    assert rep.failures[0][0] == date(2024, 7, 5)
    assert len(requested) == 3
    assert not any("0706" in u or "0707" in u or "06JUL" in u or "07JUL" in u for u in requested)
    cached = tmp_path / "bhavcopy_20240708.csv.xz"
    assert lzma.decompress(cached.read_bytes()).decode() == LEGACY
    # second run skips the cached day
    rep2 = download_missing_sessions(tmp_path, date(2024, 7, 8), date(2024, 7, 8), fetch=fetch)
    assert rep2.skipped_existing == 1 and rep2.downloaded == 0


def test_missing_ohlc_row_is_skipped_not_whole_session() -> None:
    text = (
        UDIFF_HDR
        + udiff_row("2026-10-13", "STK", "I1", "SUSP", "EQ", "-", "-", "-", "-", "10")
        + udiff_row("2026-10-13", "STK", "I2", "LIVE", "EQ", "10", "11", "9", "10.5", "10")
    )
    _, df = parse_archive_bhavcopy(text)
    assert df["symbol"].to_list() == ["LIVE"]


def test_missing_prev_close_and_volume_stay_null_not_fabricated() -> None:
    text = UDIFF_HDR + udiff_row(
        "2026-10-13", "STK", "I1", "NEW", "EQ", "10", "11", "9", "10.5", ""
    )
    text = text.replace(",1000,50000.50,", ",,50000.50,")
    _, df = parse_archive_bhavcopy(text)
    row = df.row(0, named=True)
    assert row["prev_close"] is None
    assert row["volume"] is None


def test_writer_keeps_higher_precedence_source_across_runs(tmp_path: Path) -> None:
    udiff = _bars([("A", date(2022, 12, 30), "3")])
    sec = _bars([("A", date(2022, 12, 30), "1")]).with_columns(
        pl.lit("sec_bhavdata").alias("source")
    )
    write_daily_bars(tmp_path, udiff)
    write_daily_bars(tmp_path, sec)  # later, lower-precedence run must not overwrite
    out = read_daily_bars(tmp_path)
    assert out["source"].to_list() == ["udiff"]
    assert out["close"].to_list() == [Decimal("3.0000")]
    newer = _bars([("A", date(2022, 12, 30), "4")])
    write_daily_bars(tmp_path, newer)  # equal precedence: newest wins
    assert read_daily_bars(tmp_path)["close"].to_list() == [Decimal("4.0000")]
