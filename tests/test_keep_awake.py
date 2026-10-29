"""Tests for the keep-awake visit.

The browser itself runs only in the scheduled workflow. What is tested
here is the judgement it makes from what a page says, since that is what
decides whether a run wakes the app, waits, or goes red.
"""

import keep_awake as K

SLEEP_PAGE = (
    "Zzzz\n\nThis app has gone to sleep due to inactivity. Would you like "
    "to wake it back up?\n\nYes, get this app back up!"
)
BOOTING = "This will take just a sec! Your app is waking up!"
DASHBOARD = "Which New York crashes hurt people\n\nFilters\n\nThe finding"


def test_the_sleep_page_reads_as_asleep():
    assert K.classify([SLEEP_PAGE]) == K.ASLEEP


def test_a_booting_app_is_waited_on_not_woken_twice():
    assert K.classify([BOOTING]) == K.WAITING


def test_the_dashboard_counts_only_once_its_title_has_rendered():
    """Streamlit's shell answers long before the script has run."""
    assert K.classify(["", "RUNNING...\nStop"]) == K.WAITING
    assert K.classify(["", DASHBOARD]) == K.READY


def test_the_dashboard_is_found_inside_its_iframe():
    """streamlit.app draws the app in a frame, not the top-level page."""
    assert K.classify(["streamlit.app shell", DASHBOARD]) == K.READY


def test_a_failed_visit_says_what_to_do():
    line = K.summarise({"ready": False, "woke": True, "seconds": 300}, K.APP_URL)
    assert "did not render" in line
    assert "reboot" in line


def test_a_good_visit_says_whether_it_had_to_wake_the_app():
    asleep = K.summarise({"ready": True, "woke": True, "seconds": 41}, K.APP_URL)
    awake = K.summarise({"ready": True, "woke": False, "seconds": 6}, K.APP_URL)
    assert "woken from sleep" in asleep
    assert "already awake" in awake
