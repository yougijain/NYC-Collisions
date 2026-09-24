# NYC Collisions: which intersections hurt people more than they should

**[Open the live dashboard →](https://nyc-collisions-current.streamlit.app/)**
 (first visit may take a moment to wake the app)

<!-- generated:finding-headline -->
**1,093 intersections** out of **637,256 crashes** since 2020 injure people more often than their own crashes account for. The worst of them runs **28 percentage points** above what its mix of crashes predicts.
<!-- /generated:finding-headline -->

![The dashboard's landing page: headline figures, the worst intersections, and
the caveat that this is not a danger ranking](docs/img/dashboard.png)

## What that means, in plain terms

New York publishes a record of every reported car crash. Count those records
by location and you mostly learn where the traffic is. The busiest junctions
have the most crashes. That is not news.

The harder question is where crashes *go badly*.

Two sedans bumping at walking pace and a van hitting a cyclist are each one
row in the data. They are not the same event. Every crash here gets a score
for how likely it was to hurt somebody, based only on what was involved: the
vehicles, the cause the officer wrote down, the type of road, the hour. Never
on where it happened. Each intersection is then held against its own crashes.
When a junction injures people more often than its own mix of crashes
predicts, something about the place is worth a look.

Think of it as a screening tool rather than a verdict. Tens of thousands of
junctions narrow to a list short enough for one person to work through, and
each entry comes with what is unusual about it.

### What it deliberately does not claim

This is not a ranking of the most dangerous intersections. Rather than put
that in a disclaimer, the project went and measured it. NYC DOT publishes
automated traffic counts; joining them on produces a per-vehicle rate for the
sites they reach. Ranking by that rate gives a substantially different order.

<!-- generated:exposure-lede -->
Over the 425 sites a counter reaches, the two orderings agree at a rank correlation of only **0.27**.
<!-- /generated:exposure-lede -->

A site near the top of this list has crashes that injure people more often
than their circumstances account for. That is a different claim from being
the place you are most likely to get hurt. [The full measurement is further
down](#what-it-does-not-prove).

## Description

A scheduled GitHub Actions workflow pulls new crashes from the NYC Open Data
(Socrata) API each week, cleans them, merges them into a Parquet dataset and
publishes it as a GitHub Release asset. The Streamlit dashboard queries that
Parquet with DuckDB and renders filters, charts and a heatmap over New York.

Coverage is January 2020 to the present, and grows on its own. <!-- generated:dataset-headline -->
The current build holds **637,256 crashes** through **2026-06-11**. Of those, 244,916 (38.4%) injured or killed somebody: 326,869 people injured, 1,701 killed. Borough is known for 94.6% of them, against 69.3% as the source ships it.
<!-- /generated:dataset-headline -->

Those figures are generated, not typed. `scripts/dataset_facts.py` measures
whatever build is current and writes [`docs/dataset_facts.md`](docs/dataset_facts.md)
and `docs/dataset_facts.json`; the weekly refresh reruns it and commits the
result. Quote that file rather than measuring your own, and a stale number
shows up as a diff instead of surviving in a README.

## How it works

```
NYC Open Data (h9gi-nx95)                NYC Open Data (7ym2-wayt)
        |  scripts/fetch_data.py                  |  scripts/fetch_traffic_volume.py
        |    paginated SoQL,                      |    both through scripts/socrata.py
        |    $order=collision_id                  v
        v                                  traffic_volume.parquet
   raw records                                    |   (Release asset)
        |  scripts/clean.py                       |
        |    normalise, type, mask bad coords     |
        v                                         |
   cleaned rows                                   |
        |  scripts/build_dataset.py               |
        |    merge + dedupe on collision_id       |
        v                                         |
  collisions.parquet  --->  Release asset         |
        |                        |                |
        |  app/db.py             |  models/injury_risk.py
        |    DuckDB view         v                |
        |                 scripts/train_injury_risk.py
        |                   reports/injury_risk/   |
        |                        |                |
        |                        |  models/watchlist.py
        |                        v                |
        |                 scripts/build_watchlist.py
        |                   injury_watchlist.csv   |
        |                        |                |
        |                        |  models/exposure.py
        |                        v                v
        |                 scripts/build_exposure.py
        |                   injury_exposure.csv
        v                        |
  sql/*.sql  ---->  app/dashboard.py  <-----------+
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
| Gradient boosting (uncalibrated) | 0.7947 | 0.1861 | 0.5504 | 0.365 | +24.9% |
| **Gradient boosting** | **0.7947** | **0.1811** | **0.5374** | **0.423** | **+26.9%** |

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
**1,093 intersections** with at least 25 crashes between 2022-01-01 and 2026-06-11 qualify, over 413,780 scored crashes. The worst sits **27.6 points** above what its crash mix predicts; the top decile sits 13.7 points above. 102 clear z = 1.96, against roughly 27 expected from chance across that many sites. The head of the list is signal. The tail is a screening queue.
<!-- /generated:watchlist-headline -->

<!-- generated:watchlist-examples -->
| Intersection | Borough | Crashes | Injured | Expected | Excess |
|---|---|---|---|---|---|
| 2 Avenue @ East 40 Street | Manhattan | 31 | 87.1% | 57.5% | +27.6 pts |
| East 161 Street @ Melrose Avenue | Bronx | 53 | 77.4% | 53.3% | +22.1 pts |
| Cross Bronx Expressway @ Randall Avenue | Bronx | 28 | 71.4% | 42.4% | +27.0 pts |
| East 149 Street @ Park Avenue | Bronx | 31 | 64.5% | 37.7% | +24.8 pts |
| Avenue D @ Kings Highway | Brooklyn | 38 | 73.7% | 47.7% | +24.0 pts |
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

The same scoring produces the factor table, the part a non-technical
reader repeats back.

<!-- generated:factor-examples -->
Crashes where the officer wrote *Lost Consciousness* injure someone 83.4% of the time, against 13.0% for *Oversized Vehicle*. Predicted and observed track within a couple of points across all 31 factors. That agreement is the calibration check worth trusting most.
<!-- /generated:factor-examples -->

```bash
python scripts/build_watchlist.py   # rewrites the three committed CSVs
```

### What it does not prove

**This is not a ranking of dangerous intersections, and that is now measured
rather than asserted.** Every rate on the list is per *crash*: of the crashes
at this junction, how many hurt somebody. The question a traffic engineer asks
is per *vehicle*, and answering it needs to know how many vehicles pass
through, which the collision data does not say.

NYC DOT publishes automated traffic volume counts on the same open data
portal. `models/exposure.py` joins them on, so the gap between the two
questions can be put in numbers instead of in a disclaimer.

<!-- generated:exposure-coverage -->
NYC DOT's automated traffic counts reach **425 of the 1,075 located sites** (39.5%): a recorder within 150m whose location text names one of the junction's own streets. Those sites see a median 14,314 vehicles a day past the counter, and a median 0.824 crashes that hurt someone per million vehicles.

Ranking them by that rate rather than by crash mix gives a substantially different order — the two agree at a Spearman correlation of **0.27**. That is the distance between the two questions, in a number.

It is also why the watchlist is not re-ranked by it. A recorder sits on one segment rather than across a junction, and 338 of the matched counters cover a single direction, roughly half the traffic on a two-way street; sort by crashes per vehicle and the head of the list is whichever junction has the most under-measured traffic. Counts are a median 10 years old, the oldest from 2007. So every row carries a grade for how much weight it can take — 46 high (the counter names both streets and covers both directions), 190 medium, 189 low — and the ranking stays with the crash-mix residual, which covers every site rather than a third of them.
<!-- /generated:exposure-coverage -->

Read the list as what it is: sites whose crashes injure people more often than
their circumstances account for. Read it as a screening question rather than
a verdict, and not as the site you are most likely to be hurt at.

What would close the gap is a count at every approach to a junction rather
than on one segment of one street, taken in the window the crashes are drawn
from. DOT's counts are deployments, not a network: a recorder goes out for a
week or two and moves on.

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
python scripts/fetch_traffic_volume.py     # pull DOT counts (7ym2-wayt)
python scripts/build_exposure.py           # join counts, rebuild the rates
python scripts/sync_docs.py                # rewrite the figures in the prose
```

`build_exposure.py` reads `data/clean/traffic_volume.parquet` if it is there
and the published Release asset otherwise, so it works without running the
fetch first.

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

* **"No dataset available"**: run `python scripts/build_dataset.py --full`,
  or set `NYC_COLLISIONS_DATA` to a Parquet path or URL.
* **The dashboard shows old data**: the app caches the Release asset for six
  hours. Delete `.cache/` to force a re-download.
* **Socrata returns 429**: you are being throttled. Set `SOCRATA_APP_TOKEN`;
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
