# Model card: NYC crash injury risk

## What it does

Given a crash that has already been reported, estimates the probability that
someone was injured or killed in it.

The inputs are what a responding officer writes down: the vehicles involved,
the contributing factors, the road type, the borough, the time. So this is a
scoring model, not a forecast — it cannot tell you where the next crash will
be, and nothing in it claims to.

The reason to want that probability is the watchlist. Comparing a site's
observed injury rate against what its crash mix predicts separates "this
intersection sees a lot of bicycle and moped collisions" from "this
intersection hurts people more often than its crash mix accounts for".
Only the second is a finding.

## Intended use

- Ranking intersections for engineering review, as a first pass before
  someone looks at the site.
- Describing which circumstances carry injury risk, in language a
  non-technical reader can repeat.

## Not intended for

- **Predicting future crashes.** Every feature is recorded after the fact.
- **Anything about an individual.** The model sees no one's identity, and a
  prediction about a crash is not a judgement about a driver.
- **Enforcement targeting.** Reported crashes are not a census of crashes,
  and the reporting gaps are not evenly spread (see Limitations).
- **Comparing sites on absolute danger.** There is no exposure denominator,
  so a busy intersection and a dangerous one look alike.

## Data

NYC Open Data, *Motor Vehicle Collisions — Crashes* (`h9gi-nx95`), cleaned by
`scripts/clean.py`. Current build: 637,256 crashes, 2020-01-01 to 2026-06-11.
Live figures in [`dataset_facts.md`](dataset_facts.md).

**Target.** `number_of_persons_injured > 0 or number_of_persons_killed > 0`.
Fatal crashes are pooled with injury crashes rather than modelled separately:
at 1,701 deaths in 637,256 crashes, a fatality model would be fitting noise.

**Features.** Eleven, all nameable, from `models/features.py`:

| Feature | What it is |
|---|---|
| `borough` | One of five, or Unknown where the source left it blank |
| `hour`, `weekday`, `month` | When |
| `vehicle_1`, `vehicle_2` | The two vehicles, mapped from 1,385 source spellings onto 14 named classes |
| `vehicle_count` | How many vehicles the report lists |
| `factor_1`, `factor_2` | Contributing factors, with categories under 0.1% folded into Other |
| `road_class` | Expressway, parkway, bridge, boulevard, avenue, street, road — read out of the street name |
| `at_intersection` | Whether the report names a cross street |

**Deliberately excluded.** The casualty counts, which are the target. And
coordinates, street names and anything else identifying *where* a crash
happened — if the model can see the site, it learns the site, and the gap the
watchlist measures collapses to zero. Two tests enforce this: perturbing the
casualty columns or the location columns must leave every feature unchanged.

## How it was built

**Three time-ordered folds.** No random splitting: reporting practice drifts
hard enough that a random split would badly flatter the model.

| Fold | Period | Crashes | Injury rate |
|---|---|---|---|
| Train | 2020-01-01 to 2023-12-31 | 423,970 | 0.358 |
| Calibration | 2024-01-01 to 2024-12-31 | 91,316 | 0.441 |
| Test | 2025-01-01 to 2026-06-11 | 121,970 | 0.432 |

**Estimator.** `HistGradientBoostingClassifier` with native categorical
support, so nothing is one-hot expanded and "Sedan" stays "Sedan" in
everything the model reports about itself. Learning rate 0.06, 31 leaves,
minimum 100 samples per leaf, L2 1.0; early stopping picks the round count
and settles near 700. The curve is flat well before that — 200 rounds gives
ROC-AUC 0.7929 against 0.7943 at 716 — so this is not a tuned number.

**Calibration.** A single additive shift in log-odds, fitted on 2024. A full
Platt scaling returns a slope of 0.967 and lands within 0.0001 Brier of it,
and isotonic the same, so the extra parameters buy nothing. The drift here is
a level change, and one number that can be read out loud — "the city ran 0.34
log-odds hotter than the model was trained for" — is the right shape for it.

## Results

Measured on the test fold, which neither the model nor its calibration saw.
Regenerate with `python scripts/train_injury_risk.py`; full output in
[`../reports/injury_risk/results.md`](../reports/injury_risk/results.md).

| Predictor | ROC-AUC | Brier | Log loss | Mean predicted | Brier skill |
|---|---|---|---|---|---|
| Borough × hour base rate | 0.5491 | 0.2475 | 0.6886 | 0.369 | — |
| Gradient boosting (uncalibrated) | 0.7943 | 0.1862 | 0.5507 | 0.366 | +24.8% |
| **Gradient boosting** | **0.7943** | **0.1812** | **0.5378** | **0.423** | **+26.8%** |

Observed injury rate on the test fold: 0.432.

**The baseline is the point of comparison.** Borough and hour are one SQL
query, and they carry almost no signal: ROC-AUC 0.549, barely better than a
coin flip. That is worth knowing on its own — *when* and *which borough* tell
you very little about whether a crash hurt someone. What the vehicles were
tells you a great deal.

**Brier is the number that matters** here, because the watchlist subtracts
predicted rates from observed ones and so depends on the probabilities being
right, not just ordered right. Calibration takes Brier from 0.1862 to 0.1812
while leaving ROC-AUC untouched to four places — a level shift is monotone,
so it cannot reorder anything. That is the entire difference between the two
model rows.

**What the model is using**, by test ROC-AUC lost when each feature is
shuffled:

| Feature | AUC drop |
|---|---|
| `vehicle_2` | 0.0975 |
| `factor_2` | 0.0955 |
| `factor_1` | 0.0734 |
| `vehicle_1` | 0.0522 |
| `road_class` | 0.0296 |
| `borough` | 0.0056 |
| `hour` | 0.0051 |
| `vehicle_count` | 0.0051 |
| `month` | 0.0024 |
| `at_intersection` | 0.0011 |
| `weekday` | 0.0005 |

What was hit and why dominates; when and where barely register. The injury
rate runs from 0.135 when the first vehicle is an ambulance to 0.891 when it
is an e-bike, and no amount of knowing the hour gets you that.

## Limitations

**No exposure denominator.** This is the big one, and it is not fixable with
this dataset. There are crashes here but no traffic counts, so a site with
many crashes may simply be a site with many vehicles. Every rate here is per
*crash*, never per vehicle passing through. NYC DOT publishes automated
traffic volume counts; joining them would turn "crashes per crash" into
"crashes per million vehicles", which is the number a traffic engineer
actually wants. Until then, the watchlist ranks sites by residual injury risk
given their crash mix, and that is all it ranks them by.

**Reported crashes are not all crashes, and the gap moves.** The share of
reported crashes that injured someone climbed from 0.296 in 2020 to 0.440 in
2024 and has held near 0.43 since. New York's roads did not get 50% more
dangerous in four years. The likelier story is that fewer
property-damage-only crashes are being reported, which mechanically raises
the share that involved injury — and the share of crashes logged to a street
address rather than an intersection rose from 0.261 to 0.323 over the same
period, which is roughly what that looks like. The three-fold split and the
level shift handle this operationally, but it means the model's absolute
probabilities describe *reported* crashes and will drift again.

**The features are recorded after the crash, by a person.** "Driver
Inattention/Distraction" is an officer's judgement, and 25% of crashes get
"Unspecified". The model partly learns how reports get written, not only
what happened. A site whose crashes are documented more carefully will score
differently from one whose are not.

**Missing location is informative, and that is a problem.** Crashes with no
street name have an injury rate of 0.249 against 0.36–0.47 for named roads —
because they are disproportionately the minor property-damage reports. The
model uses this through `road_class = Unknown`, which is legitimate for
scoring a crash but means the Unknown category is carrying a reporting
artifact rather than anything about roads.

**Borough is missing for 31% of crashes.** Kept as its own `Unknown`
category rather than imputed, so any borough-level reading is over the 69%
that carry one.

**Nothing here is causal.** "Unsafe speed raises predicted injury risk" is a
statement about what co-occurs in crash reports. It is not an estimate of
what would happen if speeds fell.

## The watchlist built on it

`models/watchlist.py` turns per-crash scores into ranked intersections. Three
decisions there are worth repeating here, because they are what make the
ranking mean anything.

**Sites are scored out of sample, rolling forward.** Scoring a crash with a
model that trained on it shrinks its residual, and the residual is the whole
product. Each year is scored by a model trained on everything before the
previous year and calibrated on the previous one — five fits for 2022 through
2026. 2020 and 2021 go unscored, which costs little, since they sit in a
different reporting regime anyway.

**The excess is centred.** A site's excess is its observed injury rate minus
its mean predicted rate, with the city-wide mean subtracted so the whole
scored population nets to zero. That turns it into a comparison between sites
rather than a claim about absolute risk, and it absorbs whatever level bias
the model has left.

**The ranking is a lower bound.** A site with 25 crashes and a 30-point excess
is a weaker finding than one with 400 crashes and 15 points, so sites are
ordered by the conservative end of a 95% interval rather than by the estimate,
on top of a hard minimum of 25 crashes. `excess_z` is published alongside so a
reader can see the arithmetic: 55 of 544 sites clear z = 1.96, against roughly
14 expected by chance at that many comparisons. The head of the list is
signal; the tail is a screening queue, not a verdict.

Current build: 544 sites over 413,780 scored crashes, 2022-01-01 to
2026-06-11. Regenerate with `python scripts/build_watchlist.py`.

## Reproducing

```bash
pip install -r requirements-ml.txt
python scripts/build_dataset.py --full     # or set NYC_COLLISIONS_DATA
python scripts/train_injury_risk.py
python scripts/build_watchlist.py
```

Writes `reports/injury_risk/{metrics.json,results.md,calibration.png}`,
`models/artifacts/injury_risk.joblib`, and the three committed watchlist files
under `data/clean/`. Seeded throughout; the same dataset gives the same
numbers. Each generated file records the dataset version and build timestamp
it came from, so a figure quoted anywhere can be traced to one build.

The weekly refresh reruns all of it against the newly published dataset and
commits the result, so the numbers in this card go stale as a diff rather than
in silence.
