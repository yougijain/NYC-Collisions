"""Import-level checks on the dashboard's tabs.

Nothing here renders Streamlit, but a syntax error, a bad import or a
renamed helper in a view module would otherwise only surface when somebody
opened the deployed app. These catch it in CI instead.
"""

import importlib
import importlib.util
import inspect

import pytest


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
