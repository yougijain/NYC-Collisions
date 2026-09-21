"""Import-level checks on the dashboard's tabs.

Nothing here renders Streamlit, but a syntax error, a bad import or a
renamed helper in a view module would otherwise only surface when somebody
opened the deployed app. These catch it in CI instead.
"""

import importlib
import importlib.util
import inspect
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
HEX_COLOUR = re.compile(r"#[0-9a-fA-F]{6}\b")


@pytest.mark.parametrize("tab", ["finding", "crashes", "watchlist", "method"])
def test_every_tab_exposes_render(tab):
    module = importlib.import_module(f"views.{tab}")
    assert callable(module.render)


def test_the_tab_list_matches_the_modules():
    import views

    for tab in views.TABS:
        assert importlib.util.find_spec(f"views.{tab}") is not None


def test_the_entry_point_wires_up_every_tab():
    """The tabs exist to be used; one defined and never rendered is dead."""
    import dashboard

    source = inspect.getsource(dashboard.main)
    for tab in ("finding", "crashes", "watchlist", "method"):
        assert f"{tab}.render(" in source


def test_the_entry_point_names_all_four_tabs():
    import dashboard

    source = inspect.getsource(dashboard.main)
    for label in ("The finding", "The crashes", "Watchlist", "How it works"):
        assert label in source


def test_no_mapbox_token_means_no_mapbox_basemap(monkeypatch):
    """A fresh clone has no secrets.toml, and the app has to survive that."""
    import dashboard

    monkeypatch.delenv("MAPBOX_API_KEY", raising=False)
    assert dashboard.basemap() is None


def test_the_method_tab_reads_the_generated_report():
    """It describes the model that is actually serving, not a copy of the
    numbers pasted in."""
    from views import method

    metrics = method._metrics()
    assert metrics is not None
    assert method.BASELINE in metrics["metrics"]
    assert method.MODEL in metrics["metrics"]


# --- the exposure section ----------------------------------------------

def test_the_watchlist_reads_the_exposure_it_publishes():
    from views import watchlist

    exposure, summary = watchlist.exposure_data()
    assert not exposure.empty
    assert summary["matched"] == len(exposure)


def test_exposure_boroughs_are_spelled_the_way_the_app_filters_them():
    """The sidebar filters on the dataset's own labels. A watchlist that
    wrote `Unknown` against a filter reading `UNKNOWN` emptied the tab
    without erroring, which is the worst way for this to break."""
    from views import watchlist

    exposure, _ = watchlist.exposure_data()
    sites, _, _ = watchlist.watchlist_data()
    assert set(exposure["borough"]) <= set(sites["borough"])


def test_the_watchlist_still_says_something_with_no_traffic_counts(monkeypatch):
    """A fresh clone, or a build where the join was skipped. The caveat has
    to fall back to the assertion rather than reaching into an empty
    summary."""
    import pandas as pd

    from views import watchlist

    monkeypatch.setattr(watchlist, "exposure_data",
                        lambda: (pd.DataFrame(), {}))

    said = []
    monkeypatch.setattr(watchlist.st, "warning", said.append)
    watchlist._exposure_caveat()
    assert said and "no traffic counts" in said[0]

    # And the section renders nothing at all rather than half a heading.
    def refuse(*args, **kwargs):
        raise AssertionError("drew an empty exposure section")

    for name in ("subheader", "dataframe", "divider", "markdown"):
        monkeypatch.setattr(watchlist.st, name, refuse)
    watchlist._exposure_section(["BRONX"])


# --- one palette, one theme --------------------------------------------

def test_the_theme_and_the_palette_agree():
    """The chart colours were validated against this exact background. If
    the theme drifts from it, that validation no longer describes anything."""
    import tomllib

    import palette

    config = tomllib.loads(
        (ROOT / ".streamlit" / "config.toml").read_text(encoding="utf-8")
    )["theme"]

    assert config["backgroundColor"].lower() == palette.SURFACE
    assert config["secondaryBackgroundColor"].lower() == palette.PANEL
    assert config["textColor"].lower() == palette.INK
    assert config["primaryColor"].lower() == palette.SERIES
    assert config["borderColor"].lower() == palette.BORDER


def test_no_chart_carries_a_colour_of_its_own():
    """Three unrelated palettes used to share a screen. Every colour now
    comes from app/palette.py."""
    stray = []
    for path in sorted((ROOT / "app").rglob("*.py")):
        if path.name == "palette.py":
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if HEX_COLOUR.search(line) and not line.lstrip().startswith("#"):
                stray.append(f"{path.name}:{number}: {line.strip()}")
    assert not stray, "hard-coded colours outside the palette:\n" + "\n".join(stray)


def test_the_density_ramp_only_gets_darker():
    """A sequential ramp has to be monotone in lightness, or a reader
    cannot tell which end means 'more' without the legend. The green to
    yellow to red gradient this replaced was not."""
    import palette

    luminance = [
        0.2126 * r + 0.7152 * g + 0.0722 * b
        for r, g, b, _ in palette.DENSITY_RAMP
    ]
    assert luminance == sorted(luminance, reverse=True)

    alphas = [a for *_, a in palette.DENSITY_RAMP]
    assert alphas == sorted(alphas)
    assert alphas[0] == 0, "the lightest step must let the basemap through"


def test_hex_to_rgba_round_trips():
    import palette

    assert palette.rgba("#2a78d6") == [42, 120, 214, 255]
    assert palette.rgba("2a78d6", 128) == [42, 120, 214, 128]
    with pytest.raises(ValueError):
        palette.rgba("#fff")
