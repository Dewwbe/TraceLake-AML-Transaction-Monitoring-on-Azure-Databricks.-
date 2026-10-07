"""Convert OFAC legacy flat files (SDN.CSV + ALT.CSV) into the canonical sanctions layout.

ASSUMPTION TO VERIFY: the column order below follows OFAC's published data specification for the
legacy delimited files (no header row, "-0-" for nulls). Check it against the specification file
shipped with the download (dat_spec.txt) before relying on it. Unit tests use a fixture written to
this layout, so they prove the parsing code, not the specification.
"""

from __future__ import annotations

import pandas as pd

SDN_COLUMNS = ["ent_num", "sdn_name", "sdn_type", "program", "title", "call_sign", "vess_type",
               "tonnage", "grt", "vess_flag", "vess_owner", "remarks"]
ALT_COLUMNS = ["ent_num", "alt_num", "alt_type", "alt_name", "alt_remarks"]
CANONICAL = ["ent_num", "record_type", "alt_num", "name", "entity_type", "program", "remarks"]


def _read(path: str, columns: list[str]) -> pd.DataFrame:
    df = pd.read_csv(path, header=None, names=columns, dtype=str, keep_default_na=False,
                     na_values=["-0-"], encoding="latin-1")
    return df.where(df.notna(), None)


def ofac_to_canonical(sdn_csv: str, alt_csv: str | None = None) -> pd.DataFrame:
    sdn = _read(sdn_csv, SDN_COLUMNS)
    primary = pd.DataFrame({
        "ent_num": sdn["ent_num"], "record_type": "primary", "alt_num": None,
        "name": sdn["sdn_name"],
        "entity_type": sdn["sdn_type"].fillna("entity"),
        "program": sdn["program"], "remarks": sdn["remarks"],
    })
    frames = [primary]
    if alt_csv:
        alt = _read(alt_csv, ALT_COLUMNS)
        frames.append(pd.DataFrame({
            "ent_num": alt["ent_num"], "record_type": "alias", "alt_num": alt["alt_num"],
            "name": alt["alt_name"], "entity_type": None, "program": None, "remarks": alt["alt_remarks"],
        }))
    return pd.concat(frames, ignore_index=True)[CANONICAL]
