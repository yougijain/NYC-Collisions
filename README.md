# NYC Collisions Mini-Project

**DEPLOYED LIVE (Note: may take a second to load when prompted):** https://nyc-collisions-2020-2025.streamlit.app/

Ingest NYC motor vehicle collision data, clean it, query it with SQL, and serve
an interactive dashboard. The dataset refreshes itself weekly.

## Description

A scheduled GitHub Actions workflow pulls new crashes from the NYC Open Data
(Socrata) API each week, cleans them, merges them into a Parquet dataset and
publishes it as a GitHub Release asset. The Streamlit dashboard queries that
Parquet with DuckDB and renders filters, charts and a heatmap over New York.

Coverage is January 2020 to the present, and grows on its own.

## How it works

```
NYC Open Data (h9gi-nx95)
        |  scripts/fetch_data.py      paginated SoQL, $order=collision_id
        v
   raw records
        |  scripts/clean.py           normalise, type, mask bad coords
        v
   cleaned rows
        |  scripts/build_dataset.py   merge + dedupe on collision_id
        v
  collisions.parquet  --->  GitHub Release asset (tag: data-latest)
        |
        |  app/db.py                  DuckDB view over the Parquet
        v
  sql/*.sql  --->  app/dashboard.py   Streamlit
```

Refreshes are incremental. Each run re-requests a 30-day overlap window
rather than resuming at the newest row held, because NYC back-fills
late-reported crashes and amends published ones. De-duplicating on
`collision_id` makes that overlap idempotent and lets amendments win.

The dashboard resolves its data in order: `$NYC_COLLISIONS_DATA`, a local
build at `data/clean/collisions.parquet`, the published Release asset cached
under `.cache/`, then the committed seed at
`data/clean/collisions_seed.parquet`. A failed download falls back to the
stale cache or the seed, so a bad refresh degrades instead of breaking the
deployed app.

### Why Parquet on a Release rather than a database in the repo

A 2020-present SQLite build runs to roughly 110 MB, past GitHub's 100 MB
file limit, and committing it weekly would add a multi-megabyte binary diff
every run. zstd Parquet stores the same rows at about a fifth the size, and
a Release asset keeps the data out of git history entirely.

## Technical notes

- **Ingest**: Socrata caps responses at 50,000 rows, so larger slices are
  walked with `$limit`/`$offset` under `$order=collision_id`. Offset paging
  without a total order silently drops and repeats rows.
- **Queries**: the date and borough filters are bound as DuckDB named
  parameters and pushed into every query, so aggregates are computed in SQL
  rather than by loading the table into pandas.
- **Heatmap**: capped and sampled by `hash(collision_id)`, which is uniform
  and deterministic, so the map does not shimmer on every rerun.
- **Coordinates**: the source encodes unknown positions as `0.0`; these are
  masked to NULL rather than plotted in the Gulf of Guinea.

## Getting Started

### Dependencies

* Python 3.9 or higher
* `pip` package manager
* Virtual environment tool (e.g. `venv` or `conda`)
* OS: any (tested on Windows 10, macOS, Linux)

### Installing

1. **Clone the repo**
```bash
git clone https://github.com/yougijain/NYC-Collisions
cd NYC-Collisions
```
2. **Create & activate a virtualenv**
```bash
python -m venv .venv
# Windows
.venv\Scripts\activate.bat
# macOS/Linux
source .venv/bin/activate
```
3. **Install Python dependencies**
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Executing program

The repo ships with a seed dataset, so the dashboard runs immediately:

```bash
streamlit run app/dashboard.py
```

To build the full dataset locally instead of using the published one:

```bash
python scripts/build_dataset.py --full     # 2020-present, ~10 minutes
python scripts/build_dataset.py            # refresh only recent crashes
```

Other tasks:

```bash
pytest -q                                  # test suite, no network needed
python scripts/data_quality_report.py      # completeness + validation report
```

Open `notebooks/cleaning.ipynb` or `notebooks/sql_queries.ipynb` to explore
the cleaning steps and validate each SQL script.

### Configuration

| Variable | Where | Purpose |
|---|---|---|
| `SOCRATA_APP_TOKEN` | GitHub repository secret | Lifts Socrata's anonymous rate limit. Optional; the fetch works without it but is throttled harder. |
| `MAPBOX_API_KEY` | `.streamlit/secrets.toml` or env | Dark Mapbox basemap. Optional; OpenStreetMap tiles are used otherwise. |
| `NYC_COLLISIONS_DATA` | env | Point the app at a specific Parquet path or URL. |

### Scheduled refresh

`.github/workflows/refresh-data.yml` runs every Monday at 07:17 UTC, and can
be triggered manually with a full-rebuild toggle. It downloads the current
asset, builds incrementally, runs the test suite against the result, and
publishes only if those tests pass, so a bad upstream day cannot replace a
good dataset. It needs no secrets beyond the automatic `GITHUB_TOKEN`.

## Help

* **"No dataset available"** — run `python scripts/build_dataset.py --full`,
  or set `NYC_COLLISIONS_DATA` to a Parquet path or URL.
* **The dashboard shows old data** — the app caches the Release asset for six
  hours. Delete `.cache/` to force a re-download.
* **Socrata returns 429** — you are being throttled. Set `SOCRATA_APP_TOKEN`;
  tokens are free from the NYC Open Data portal.
* For other issues, open an issue on the GitHub repo.

## Authors

Yougi Jain

## Version History

* 0.1
    * See [contribution history](https://github.com/yougijain/ds-fundamentals-ingest-clean/graphs/contributors)
    * Initial Release

## License

This project is licensed under the MIT License.

## Acknowledgements

#### Data Source

* Motor Vehicle Collisions – Crashes  
* NYC Open Data (CC0 1.0)  
* https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95

#### ReadMe Template

* [awesome-readme](https://github.com/matiassingers/awesome-readme)
* [PurpleBooth](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2)
* [dbader](https://github.com/dbader/readme-template)
* [zenorocha](https://gist.github.com/zenorocha/4526327)
* [fvcproductions](https://gist.github.com/fvcproductions/1bfc2d4aecb01a834b46)
