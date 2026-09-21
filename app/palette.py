"""One palette, used by every chart on the page.

Before this, three unrelated colour schemes shared a screen: Streamlit's
default blue on the overview charts, an orange I picked for the factor
chart, and a green-yellow-red gradient on the heatmap. The last one was the
real problem -- a rainbow encoding a single continuous quantity, which is
the most common way a density map misleads. Rainbows are not perceptually
ordered, so a reader cannot tell which end is "more" without consulting the
legend, and the yellow band reads as a peak that is not there.

So: one hue per job.

    SERIES        one measure, one colour, across every single-series chart
    SERIES_ALT    the second line only where two share an axis
    DENSITY       a single hue, light to dark, for the heatmap

The two series colours are slots 1 and 2 of a palette validated for
colour-vision deficiency against this page's surface: worst-pair CVD
delta-E 24.7 and normal-vision 33.6, both well clear of the 8 and 15
floors. The density ramp is the same blue stepped light to dark, so it is
monotone in lightness and reads correctly in greyscale and in print.

Keep the theme in .streamlit/config.toml in step with SURFACE and INK here.
"""

from typing import List

# --- surfaces and ink --------------------------------------------------

SURFACE = "#fcfcfb"       # the plane every chart is drawn on
PANEL = "#f0efec"         # cards, popovers, code blocks
INK = "#0b0b0b"           # primary text
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"     # axis labels, captions
GRIDLINE = "#e1e0d9"
BORDER = "#e1e0d9"

# --- series ------------------------------------------------------------

# Every chart showing one measure uses this. Consistency is the point: a
# reader should not have to work out whether a colour change means anything.
SERIES = "#2a78d6"
# Only for the second line on a chart that legitimately shares one axis.
SERIES_ALT = "#eb6834"
LINK = "#256abf"

# Sites flagged on the watchlist map. A second context on its own surface,
# so it takes the alternate hue rather than competing with the density ramp.
FLAGGED = "#eb6834"

# --- sequential density ------------------------------------------------

# One hue, light to dark. The first step is fully transparent so empty
# ground shows the basemap rather than a wash of the lightest colour.
DENSITY_RAMP: List[List[int]] = [
    [205, 226, 251, 0],
    [134, 182, 239, 90],
    [57, 135, 229, 135],
    [28, 92, 171, 180],
    [13, 54, 107, 215],
]


def rgba(hex_colour: str, alpha: int = 255) -> List[int]:
    """Convert `#rrggbb` to the [r, g, b, a] list pydeck wants.

    Args:
        hex_colour: A six-digit hex colour, with or without the leading hash.
        alpha: Opacity from 0 to 255.

    Returns:
        The colour as a four-element list.

    Raises:
        ValueError: If the string is not six hex digits.
    """
    value = hex_colour.lstrip("#")
    if len(value) != 6:
        raise ValueError(f"Expected a six-digit hex colour, got {hex_colour!r}")
    return [int(value[i:i + 2], 16) for i in (0, 2, 4)] + [alpha]
