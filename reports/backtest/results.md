# Does the watchlist find sites that stay bad?

Fit on **2022–2023**, checked against **2024–2026**. 247 sites clear 20 scored crashes in both windows.

## Headline

Early excess predicts late excess at a Spearman correlation of **0.2599** (permutation null -0.0012 ± 0.0615, p = 0.0005).

Sorting by raw injury rate instead, with no model, predicts the same target at **0.2556**.

## Every ordering, against both targets

| Predictor | Target | Spearman |
|---|---|---|
| model excess | late excess | 0.260 |
| model excess, lower 95% | late excess | 0.256 |
| observed injury rate | late excess | 0.256 |
| crash count | late excess | 0.051 |
| model excess | late injury rate | 0.265 |
| model excess, lower 95% | late injury rate | 0.242 |
| observed injury rate | late injury rate | 0.469 |
| crash count | late injury rate | -0.054 |

## By decile of the early ranking

| Decile | Sites | Early excess | Late excess | Late injury rate |
|---|---|---|---|---|
| 1 | 25 | -16.7 pts | -3.3 pts | 43.9% |
| 2 | 25 | -10.1 pts | -2.8 pts | 44.5% |
| 3 | 24 | -6.3 pts | +0.2 pts | 48.8% |
| 4 | 25 | -3.5 pts | +2.0 pts | 49.3% |
| 5 | 25 | -1.1 pts | +2.1 pts | 50.7% |
| 6 | 24 | +1.5 pts | +3.3 pts | 51.8% |
| 7 | 25 | +4.6 pts | +0.4 pts | 48.4% |
| 8 | 24 | +7.9 pts | +2.6 pts | 52.7% |
| 9 | 25 | +11.5 pts | +5.4 pts | 54.5% |
| 10 | 25 | +17.6 pts | +4.8 pts | 54.8% |

## Does the model adjustment change the ranking at all?

Early excess and raw injury rate order the sites at a Spearman of **0.7887**, so the two predictors above are related but not the same list. Predicted rates run 20.9% to 64.2% across sites (sd 0.081, against 0.133 for observed rates), so the adjustment is substantial rather than cosmetic.

## Sensitivity

Neither the split year nor the crash threshold was tuned. 2024 is the headline because it was chosen before the first run, not because of how it came out.

| Split | Min crashes | Sites | Model excess | Injury rate | p |
|---|---|---|---|---|---|
| 2023 | 15 | 139 | 0.220 | 0.229 | 0.00599 |
| 2023 | 20 | 58 | 0.195 | 0.216 | 0.08583 |
| 2023 | 25 | 27 | _too few to rank_ | | |
| 2024 | 15 | 466 | 0.279 | 0.297 | 0.002 |
| 2024 | 20 | 247 | 0.260 | 0.256 | 0.002 |
| 2024 | 25 | 133 | 0.275 | 0.319 | 0.002 |
| 2025 | 15 | 293 | 0.282 | 0.220 | 0.002 |
| 2025 | 20 | 127 | 0.303 | 0.208 | 0.002 |
| 2025 | 25 | 63 | 0.293 | 0.212 | 0.01198 |

## What a visit to the top of the list would have found

- Of the top 50 by the early ranking, **64%** were still above expectation in the later window, against a base rate of 57% (1.12x lift).
- Their mean late excess was +4.0 points, against +0.9 for the rest.

## Regression to the mean

The top 50 sites averaged +13.8 points of excess in the early window and +4.0 in the later one, retaining 29%. Some shrinkage is arithmetic rather than failure: ranking on a noisy estimate selects sites whose noise ran high. The permutation null above is what separates the two.
