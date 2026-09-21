"""One spelling per street.

The source is not consistent about how it writes a street name. Across the
crash record, College Point Boulevard at Horace Harding Expressway appears
fourteen different ways -- upper case, title case, lower case, "Blvd" for
"Boulevard" -- and every one of them keys as a different intersection. Of
63,132 apparent junctions, 46,645 are real; the other 16,487 are the same
corners written differently.

That is not a cosmetic problem. Splitting a junction's crashes across
three spellings splits its crash count too, which pushes it below the
watchlist's minimum, widens the interval on whatever is left, and lists
the same corner twice when both halves clear the bar.

Nothing of value is lost by collapsing them. Fourteen spellings of one
street is not fourteen facts.

The same vocabulary flattens NYC DOT's street names, which use the same
abbreviations, so the traffic counts can be matched to the junctions.
"""

import re
from typing import Dict

import pandas as pd

# Suffixes and directions, as the sources abbreviate them.
ABBREVIATIONS: Dict[str, str] = {
    "AV": "AVENUE", "AVE": "AVENUE", "AVEN": "AVENUE",
    "ST": "STREET", "STR": "STREET", "STRT": "STREET",
    "PL": "PLACE", "PLC": "PLACE",
    "BLVD": "BOULEVARD", "BL": "BOULEVARD", "BLV": "BOULEVARD",
    "BVD": "BOULEVARD",
    "RD": "ROAD", "DR": "DRIVE", "DRV": "DRIVE",
    "PKWY": "PARKWAY", "PKY": "PARKWAY", "PWY": "PARKWAY",
    "PARKWY": "PARKWAY", "PKWAY": "PARKWAY",
    "EXPY": "EXPRESSWAY", "EXPWY": "EXPRESSWAY", "EXWY": "EXPRESSWAY",
    "EXP": "EXPRESSWAY", "EXPRWY": "EXPRESSWAY",
    "LN": "LANE", "CT": "COURT", "CRT": "COURT",
    "TER": "TERRACE", "TERR": "TERRACE",
    "HWY": "HIGHWAY", "HWAY": "HIGHWAY",
    "BRG": "BRIDGE", "SQ": "SQUARE", "PLZ": "PLAZA", "CIR": "CIRCLE",
    "TPKE": "TURNPIKE", "TPK": "TURNPIKE",
    "CONC": "CONCOURSE", "XING": "CROSSING", "ALY": "ALLEY",
    "N": "NORTH", "S": "SOUTH", "E": "EAST", "W": "WEST",
}

# "165TH STREET" and "165 STREET" are one street.
_ORDINAL = re.compile(r"^(\d+)(ST|ND|RD|TH)$")

# Everything that is not a letter, a digit or a space is separator noise:
# the source writes "BQE", "(BQE)" and "B.Q.E." for the same road.
_NOISE = re.compile(r"[^A-Z0-9 ]")
_SPACES = re.compile(r"\s+")


def canonical(name: object) -> str:
    """Reduce a street name to the one spelling this project uses.

    Upper-cases, strips punctuation, collapses whitespace, expands the
    abbreviations above and drops ordinal suffixes.

    Args:
        name: A street name, or anything else.

    Returns:
        The canonical spelling, or an empty string for a blank or
        non-string input.

    Examples:
        >>> canonical("Belt Pkwy")
        'BELT PARKWAY'
        >>> canonical("E 165th St")
        'EAST 165 STREET'
    """
    if not isinstance(name, str):
        return ""

    words = []
    for word in _SPACES.sub(" ", _NOISE.sub(" ", name.upper())).split():
        ordinal = _ORDINAL.match(word)
        words.append(ordinal.group(1) if ordinal else ABBREVIATIONS.get(word, word))
    return " ".join(words)


def canonical_series(names: pd.Series) -> pd.Series:
    """Canonicalise a column of street names, keeping blanks null.

    Args:
        names: A column of street names.

    Returns:
        The canonical spellings, null where the source said nothing.
    """
    canonicalised = names.map(canonical)
    return canonicalised.where(canonicalised.astype(bool), pd.NA).astype("string")
