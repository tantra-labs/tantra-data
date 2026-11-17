"""Point-in-time store adapters for intraday option-chain snapshots (gap 0016)."""

from tantra_data.store.option_chain import DuckDbOptionChainStore

__all__ = ["DuckDbOptionChainStore"]
