# Model card: NYC crash injury risk

## What it does

Given a crash that has already been reported, estimates the probability that
someone was injured or killed in it.

The inputs are what a responding officer writes down: the vehicles involved,
the contributing factors, the road type, the borough, the time. So this is a
scoring model rather than a forecast. It cannot tell you where the next crash will
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
- **Comparing sites on absolute danger.** The exposure denominator covers a
  third of the watchlist, so for most sites a busy intersection and a
  dangerous one still look alike (see Limitations).

## Data

NYC Open Data, *Motor Vehicle Collisions — Crashes* (`h9gi-nx95`), cleaned by
`scripts/clean.py`.

<!-- generated:dataset-headline -->
The current build holds **637,256 crashes** through **2026-06-11**. Of those, 244,916 (38.4%) injured or killed somebody: 326,869 people injured, 1,701 killed. Borough is known for 94.6% of them, against 69.3% as the source ships it.
<!-- /generated:dataset-headline -->

Live figures in [`dataset_facts.md`](dataset_facts.md).

**Target.** `number_of_persons_injured > 0 or number_of_persons_killed > 0`.
Fatal crashes are pooled with injury crashes rather than modelled separately:
at well under three deaths per thousand crashes, a fatality model would be
fitting noise.

**Features.** Eleven, all nameable, from `models/features.py`:

| Feature | What it is |
|---|---|
| `borough` | One of five, or Unknown where the source left it blank |
| `hour`, `weekday`, `month` | When |
| `vehicle_1`, `vehicle_2` | The two vehicles, mapped from 1,385 source spellings onto 14 named classes |
| `vehicle_count` | How many vehicles the report lists |
| `factor_1`, `factor_2` | Contributing factors, with categories under 0.1% folded into Other |
| `road_class` | Expressway, parkway, bridge, boulevard, avenue, street, road, read out of the street name |
| `at_intersection` | Whether the report names a cross street |

**Deliberately excluded.** The casualty counts, which are the target. And
coordinates, street names and anything else identifying *where* a crash
happened. If the model can see the site, it learns the site, and the gap the
watchlist measures collapses to zero. Two tests enforce this: perturbing the
casualty columns or the location columns must leave every feature unchanged.

## How it was built

**Three time-ordered folds.** No random splitting: reporting practice drifts
hard enough that a random split would badly flatter the model.

<!-- generated:model-folds -->
| Fold | Period | Crashes | Injury rate |
|---|---|---|---|
| Train | 2020-01-01 to 2023-12-31 | 423,970 | 0.358 |
| Calibration | 2024-01-01 to 2024-12-31 | 91,316 | 0.441 |
| Test | 2025-01-01 to 2026-06-11 | 121,970 | 0.432 |
<!-- /generated:model-folds -->

**Estimator.** `HistGradientBoostingClassifier` with native categorical
support, so nothing is one-hot expanded and "Sedan" stays "Sedan" in
everything the model reports about itself. Learning rate 0.06, 31 leaves,
minimum 100 samples per leaf, L2 1.0; early stopping picks the round count
and settles near 700. The curve is flat well before that; a third of those
rounds gets within 0.002 ROC-AUC, so this is not a tuned number.

**Calibration.** A single additive shift in log-odds, fitted on 2024. A full
Platt scaling returns a slope of 0.967 and lands within 0.0001 Brier of it,
and isotonic the same, so the extra parameters buy nothing. The drift here is
a level change. One number that can be read out loud, "the city ran 0.34
log-odds hotter than the model was trained for", is the right shape for it.

## Results

Measured on the test fold, which neither the model nor its calibration saw.
Regenerate with `python scripts/train_injury_risk.py`; full output in
[`../reports/injury_risk/results.md`](../reports/injury_risk/results.md).

<!-- generated:model-metrics -->
| Predictor | ROC-AUC | Brier | Log loss | Mean predicted | Brier skill |
|---|---|---|---|---|---|
| Borough × hour base rate | 0.5425 | 0.2478 | 0.6891 | 0.371 | — |
| Gradient boosting (uncalibrated) | 0.7947 | 0.1861 | 0.5504 | 0.365 | +24.9% |
| **Gradient boosting** | **0.7947** | **0.1811** | **0.5374** | **0.423** | **+26.9%** |

Observed injury rate on the test fold: 0.432.
<!-- /generated:model-metrics -->

**The baseline is the point of comparison.** Borough and hour are one SQL
query, and they carry almost no signal, barely beating a coin flip. That is
worth knowing on its own: *when* and *which borough* tell
you very little about whether a crash hurt someone. What the vehicles were
tells you a great deal.

**Brier is the number that matters** here, because the watchlist subtracts
predicted rates from observed ones and so depends on the probabilities being
right rather than merely ordered right. Calibration improves Brier measurably
while leaving ROC-AUC untouched to four places. A level shift is monotone,
so it cannot reorder anything. Hence the entire difference between the two
model rows.

**What the model is using**, by test ROC-AUC lost when each feature is
shuffled:

<!-- generated:feature-importance -->
| Feature | AUC drop |
|---|---|
| `factor_2` | 0.0981 |
| `vehicle_2` | 0.0918 |
| `factor_1` | 0.0733 |
| `vehicle_1` | 0.0524 |
| `road_class` | 0.0296 |
| `borough` | 0.0054 |
| `vehicle_count` | 0.0052 |
| `hour` | 0.0050 |
| `month` | 0.0023 |
| `at_intersection` | 0.0013 |
| `weekday` | 0.0003 |
<!-- /generated:feature-importance -->

What was hit and why dominates; when and where barely register. The injury
rate runs from 0.135 when the first vehicle is an ambulance to 0.891 when it
is an e-bike, and no amount of knowing the hour gets you that.

## Limitations

**The exposure denominator is partial.** This is the big one. Every rate the
model produces is per *crash*: of the crashes here, how many hurt somebody,
because the collision data says nothing about how many vehicles passed
through. A site with many crashes may simply be a site with many vehicles.

NYC DOT's automated traffic volume counts (`7ym2-wayt`) supply the missing
denominator for part of the city, and `models/exposure.py` joins them on.
That converts this limitation from an assertion into a measurement:

<!-- generated:exposure-coverage -->
NYC DOT's automated traffic counts reach **425 of the 1,075 located sites** (39.5%): a recorder within 150m whose location text names one of the junction's own streets. Those sites see a median 14,314 vehicles a day past the counter, and a median 0.824 crashes that hurt someone per million vehicles.

Ranking them by that rate rather than by crash mix gives a substantially different order — the two agree at a Spearman correlation of **0.27**. That is the distance between the two questions, in a number.

It is also why the watchlist is not re-ranked by it. A recorder sits on one segment rather than across a junction, and 338 of the matched counters cover a single direction, roughly half the traffic on a two-way street; sort by crashes per vehicle and the head of the list is whichever junction has the most under-measured traffic. Counts are a median 10 years old, the oldest from 2007. So every row carries a grade for how much weight it can take — 46 high (the counter names both streets and covers both directions), 190 medium, 189 low — and the ranking stays with the crash-mix residual, which covers every site rather than a third of them.
<!-- /generated:exposure-coverage -->

Three things the join still does not measure, each carried on the row rather
than buried here. A count is **one approach, not the junction**: traffic
through an intersection is the sum over every arm, and a recorder sits on one
segment, so a junction fed by four busy roads with a counter on the quietest
will look worse than it is. **Most segments are counted in one direction**,
so for a two-way street the figure is roughly half the traffic. And **the
count is from one year, applied across the window**, which assumes the street
did not change.

So the watchlist still ranks sites by residual injury risk given their crash
mix, and that is all it ranks them by. The per-vehicle rate is published
beside it as a second lens, graded, for the sites that have one.

**Reported crashes are not all crashes, and the gap moves.** The share of
reported crashes that injured someone climbed from 0.296 in 2020 to 0.440 in
2024 and has held near 0.43 since. New York's roads did not get 50% more
dangerous in four years. The likelier story is that fewer
property-damage-only crashes are being reported, which mechanically raises
the share that involved injury. The share of crashes logged to a street
address rather than an intersection rose from 0.261 to 0.323 over the same
period, roughly what that looks like. The three-fold split and the
level shift handle this operationally, but it means the model's absolute
probabilities describe *reported* crashes and will drift again.

**The features are recorded after the crash, by a person.** "Driver
Inattention/Distraction" is an officer's judgement, and 25% of crashes get
"Unspecified". The model partly learns how reports get written, not only
what happened. A site whose crashes are documented more carefully will score
differently from one whose are not.

**Missing location is informative, and that is a problem.** Crashes with no
street name have an injury rate of 0.249 against 0.36–0.47 for named roads.
because they are disproportionately the minor property-damage reports. The
model uses this through `road_class = Unknown`, legitimate for
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
previous year and calibrated on the previous one: five fits for 2022 through
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
reader can see the arithmetic. The head of the list is signal; the tail is a
screening queue, not a verdict.

<!-- generated:watchlist-scope -->
Keying sites direction-free merges 65,544 apparent intersections into 40,695 real ones. The current build lists 1,093 of them, over 413,780 scored crashes from 2022-01-01 to 2026-06-11, with 102 above z = 1.96 against roughly 27 expected by chance.
<!-- /generated:watchlist-scope -->

Regenerate with `python scripts/build_watchlist.py`.

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
