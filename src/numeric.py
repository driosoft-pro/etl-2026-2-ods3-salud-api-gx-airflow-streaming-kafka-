"""Numeric conventions found in the source files."""

import pandas as pd


def parse_capacity(values: pd.Series) -> pd.Series:
    """Parse capacity counts, including the source's grouped value ``1,000``.

    Only correctly grouped thousands separators are removed; malformed values
    such as ``1,5`` remain invalid instead of silently becoming fifteen.
    """
    text = values.astype("string").str.strip()
    grouped = text.str.fullmatch(r"[+-]?\d{1,3}(?:,\d{3})+").fillna(False)
    normalized = text.where(~grouped, text.str.replace(",", "", regex=False))
    return pd.to_numeric(normalized, errors="coerce")
