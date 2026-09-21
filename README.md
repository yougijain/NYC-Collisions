# NYC Collisions: injury risk model and self-refreshing pipeline

**DEPLOYED LIVE (Note: may take a second to load when prompted):** https://nyc-collisions-2020-2025.streamlit.app/

Ingest NYC motor vehicle collision data, clean it, query it with SQL, model
which crashes hurt people, and serve it all through an interactive dashboard.
The dataset refreshes itself weekly.

## Description

A scheduled GitHub Actions workflow pulls new crashes from the NYC Open Data
(Socrata) API each week, cleans them, merges them into a Parquet dataset and
publishes it as a GitHub Release asset. The Streamlit dashboard queries that
Parquet with DuckDB and renders filters, charts and a heatmap over New York.

Coverage is January 2020 to the present, and grows on its own. <!-- generated:dataset-headline -->
The current build holds **637,256 crashes** through **2026-06-11**, of which 244,916 (38.4%) injured or killed someone: 326,869 people injured and 1,701 killed. Borough is known for 94.6% of them, against 69.3% as the source ships it.
<!-- /generated:dataset-headline -->

Those figures are generated, not typed. `scripts/dataset_facts.py` measures
whatever build is current and writes [`docs/dataset_facts.md`](docs/dataset_facts.md)
and `docs/dataset_facts.json`; the weekly refresh reruns it and commits the
result. Quote that file rather than measuring your own, and a stale number
shows up as a diff instead of surviving in a README.

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
        |                                   |
        |  app/db.py    DuckDB view         |  models/injury_risk.py
        |                                   v
        |                            scripts/train_injury_risk.py
        |                              reports/injury_risk/
        |                                   |
        |                                   |  models/watchlist.py
        |                                   v
        |                            scripts/build_watchlist.py
        |                              injury_watchlist.csv
        v                                   |
  sql/*.sql  ------>  app/dashboard.py  <---+
                          Streamlit
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

## The model

**Given a crash, what is the probability it injured someone.** Not a forecast:
every feature is something written down after the fact, so the model scores a
crash that has already happened and asks whether that combination of
circumstances usually hurts people.

Measured on 121,970 crashes from 2025 onward, which neither the model nor its
calibration saw:

<!-- generated:model-metrics -->
| Predictor | ROC-AUC | Brier | Log loss | Mean predicted | Brier skill |
|---|---|---|---|---|---|
| Borough × hour base rate | 0.5425 | 0.2478 | 0.6891 | 0.371 | — |
| Gradient boosting (uncalibrated) | 0.7946 | 0.1862 | 0.5504 | 0.365 | +24.9% |
| **Gradient boosting** | **0.7946** | **0.1811** | **0.5373** | **0.423** | **+26.9%** |

Observed injury rate on the test fold: 0.432.
<!-- /generated:model-metrics -->

The baseline is there because it is what anyone can produce in one SQL query.
That it barely clears a coin flip is itself the finding: *when* and *which
borough* say almost nothing about whether a crash hurt someone. What was hit
does —
shuffling the second vehicle costs more test AUC than borough, hour, weekday,
month and vehicle count combined.

Design decisions, in short:

- **Split by time, in three folds.** Train on 2020–2023, calibrate on 2024,
  test on 2025 onward. A random split would leak: the share of reported
  crashes that injured someone climbed from 0.296 in 2020 to 0.440 in 2024,
  mostly because fewer property-damage-only crashes are being reported.
- **Calibrate the level.** That drift leaves an uncalibrated model predicting
  0.366 for a period that ran at 0.432. A single log-odds shift fitted on 2024
  closes it, improving Brier while leaving ROC-AUC untouched to four places
  (see the table above). Platt and isotonic were tried and bought 0.0001.
- **Brier over ROC-AUC.** The watchlist subtracts predicted rates from
  observed ones, so the probabilities have to be right and not merely ordered
  right.
- **No location features.** No coordinates, no street names. The watchlist
  measures how far a site sits from what its crash mix predicts; let the model
  see the site and that gap goes to zero. Tests enforce it.

Full write-up, including what it does not prove, in
[`docs/model_card.md`](docs/model_card.md). The figures above describe one
build; the generated results, including the calibration curve and permutation
importances, are regenerated weekly in
[`reports/injury_risk/results.md`](reports/injury_risk/results.md) and are
what to quote.

```bash
pip install -r requirements-ml.txt
python scripts/train_injury_risk.py
```

## The watchlist

A model that only produces an AUC is a model nobody uses. The dashboard's
second tab turns it into a list of intersections, ranked by one claim:

> at this intersection, more crashes injured someone than the crashes
> themselves account for

<!-- generated:watchlist-headline -->
**544 intersections** with at least 25 crashes between 2022-01-01 and 2026-06-11 qualify, over 413,780 scored crashes. The worst sits **31.7 points** above what its crash mix predicts; the top decile sits 14.2 points above. 57 clear z = 1.96, against roughly 14 expected from chance across that many sites — so the head of the list is signal and the tail is a screening queue, not a verdict.
<!-- /generated:watchlist-headline -->

<!-- generated:watchlist-examples -->
| Intersection | Borough | Crashes | Injured | Expected | Excess |
|---|---|---|---|---|---|
| Avenue U @ Gerritsen Avenue | Brooklyn | 26 | 88.5% | 54.7% | +31.7 pts |
| Church Avenue @ Flatbush Avenue | Brooklyn | 31 | 83.9% | 53.9% | +27.9 pts |
| Atlantic Avenue @ Crescent Street | Brooklyn | 30 | 73.3% | 43.7% | +27.6 pts |
| Cross Bronx Expressway @ Randall Avenue | Bronx | 28 | 71.4% | 42.4% | +27.0 pts |
| East 165 Street @ Grand Concourse | Bronx | 27 | 81.5% | 54.4% | +25.0 pts |
<!-- /generated:watchlist-examples -->

Three things make that number mean something:

- **Sites are keyed direction-free.** "A and B" and "B and A" are one
  junction.
  <!-- generated:watchlist-scope -->
  _pending_
  <!-- /generated:watchlist-scope -->
- **Every score is out of sample.** Scoring a crash with a model that trained
  on it shrinks its residual, and the residual is the entire product. Each
  year is scored by a model trained on everything before the previous year
  and calibrated on the previous one.
- **The ranking is by a lower bound, not the estimate.** A site with 25
  crashes and a 30-point excess is a weaker finding than one with 400 crashes
  and 15, and the ordering says so.

The same scoring produces the factor table, which is the part a non-technical
reader repeats back.

<!-- generated:factor-examples -->
Crashes where the officer wrote *Pedestrian/Bicyclist/Other Pedestrian Error/Confusion* injure someone 83.3% of the time, against 13.0% for *Oversized Vehicle*. Predicted and observed track within a couple of points across all 31 factors, which is the calibration check worth trusting most.
<!-- /generated:factor-examples -->

```bash
python scripts/build_watchlist.py   # rewrites the three committed CSVs
```

### What it does not prove

**There is no exposure denominator.** This dataset has crashes but no traffic
counts, so a junction with many crashes may simply be a junction with many
vehicles. Every rate here is per *crash*, never per vehicle passing through,
and nothing on the list is a claim that an intersection is dangerous in the
ordinary sense. NYC DOT publishes automated traffic volume counts on the same
open data portal; joining them is what would turn crashes-per-crash into
crashes-per-million-vehicles, which is the number a traffic engineer actually
wants.

The model card lists the rest: the features are an officer's judgement
recorded after the fact, a quarter of contributing factors say "Unspecified",
reported crashes are not all crashes and the gap between them moves, and
nothing here is causal.

## Technical notes

- **Ingest**: Socrata caps responses at 50,000 rows, so larger slices are
  walked with `$limit`/`$offset` under `$order=collision_id`. Offset paging
  without a total order silently drops and repeats rows.
- **Street columns**: the JSON API returns `cross_street_name` and
  `off_street_name` under each other's names, so a build taken from it files
  house numbers as cross streets and carries no identifiable intersections at
  all. Matching the 27,164 `collision_id`s present in both the API and the CSV
  export pins it down: the export's CROSS STREET NAME equals the API's
  `off_street_name` for 99.996% of them, and its OFF STREET NAME equals the
  API's `cross_street_name` for 100%. `scripts/clean.py` detects the payload
  shape and restores the documented meaning, then collapses the fixed-width
  padding the source pads street names with, which otherwise stops two
  records at one intersection from grouping together.
- **Dataset versioning**: `DATASET_VERSION` in `scripts/clean.py` stamps the
  cleaning semantics into each Parquet build. An incremental run that finds a
  dataset stamped older than the code refuses to merge into it and rebuilds
  from scratch, so a change like the street-column fix reaches every row
  rather than only the ones fetched after it shipped.
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
* `requirements.txt` runs the dashboard and the pipeline; `requirements-ml.txt`
  adds scikit-learn and matplotlib for training. The deployed app needs only
  the first.
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
python scripts/dataset_facts.py            # headline figures for this build
python scripts/build_seed.py               # re-cut the committed fallback
python scripts/train_injury_risk.py        # retrain, re-measure, redraw
python scripts/build_watchlist.py          # rescore and rank intersections
```

`pytest` runs against the committed seed unless `NYC_COLLISIONS_DATA` points
it elsewhere, so a code change is never graded on whatever the city published
that morning. The refresh workflow sets that variable to the build it is about
to publish and reuses the same suite as the gate.

Open `notebooks/cleaning.ipynb` or `notebooks/sql_queries.ipynb` to explore
the cleaning steps and validate each SQL script.

### Configuration

| Variable | Where | Purpose |
|---|---|---|
| `SOCRATA_APP_TOKEN` | GitHub repository secret | Lifts Socrata's anonymous rate limit. Optional; the fetch works without it but is throttled harder. |
| `MAPBOX_API_KEY` | `.streamlit/secrets.toml` or env | Dark Mapbox basemap. Optional; OpenStreetMap tiles are used otherwise. |
| `NYC_COLLISIONS_DATA` | env | Point the app, the tests or the trainer at a specific Parquet path or URL. |

### Scheduled refresh

`.github/workflows/refresh-data.yml` runs every Monday at 07:17 UTC, and can
be triggered manually with a full-rebuild toggle. It downloads the current
asset, builds incrementally, runs the test suite against the result, and
publishes only if those tests pass, so a bad upstream day cannot replace a
good dataset. It then regenerates the figures above, retrains the model and rescores the
watchlist, and commits all of it. It needs no secrets beyond the automatic
`GITHUB_TOKEN`.

The committed seed is not regenerated weekly; a megabyte of binary churn every
Monday is not worth it. Re-cut it with `python scripts/build_seed.py` when the
schema changes or the fallback drifts far enough from the live data to matter.

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
