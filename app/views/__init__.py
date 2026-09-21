"""The dashboard's tabs, one module each, every one exposing `render`.

Kept as tabs rather than st.navigation pages on purpose: Streamlit's
navigation lives in the sidebar, which is collapsed behind a hamburger on a
phone, and a phone is what most people open a link like this on. Tabs stay
visible there.

Nothing is re-exported from here. Importing the modules by name keeps
`views.method` meaning the module rather than a function inside it.
"""

TABS = ("finding", "crashes", "watchlist", "method")
