# Does the watchlist find sites that stay bad?

Fit on **2022–2023**, checked against **2024–2026**. 247 sites clear 20 scored crashes in both windows.

## Headline

Early excess predicts late excess at a Spearman correlation of **0.2574** (permutation null -0.0007 ± 0.0644, p = 0.0005).

Sorting by raw injury rate instead, with no model, predicts the same target at **0.2616**.

## Every ordering, against both targets

| Predictor | Target | Spearman |
|---|---|---|
| model excess | late excess | 0.257 |
| model excess, lower 95% | late excess | 0.257 |
| observed injury rate | late excess | 0.262 |
| crash count | late excess | 0.051 |
| model excess | late injury rate | 0.262 |
| model excess, lower 95% | late injury rate | 0.241 |
| observed injury rate | late injury rate | 0.469 |
| crash count | late injury rate | -0.054 |

## By decile of the early ranking

| Decile | Sites | Early excess | Late excess | Late injury rate |
|---|---|---|---|---|
| 1 | 25 | -16.9 pts | -3.4 pts | 44.1% |
| 2 | 25 | -10.1 pts | -2.4 pts | 44.4% |
| 3 | 24 | -6.2 pts | +0.4 pts | 49.3% |
| 4 | 25 | -3.5 pts | +1.3 pts | 48.4% |
| 5 | 25 | -1.0 pts | +2.7 pts | 51.8% |
| 6 | 24 | +1.4 pts | +2.4 pts | 50.9% |
| 7 | 25 | +4.7 pts | -0.4 pts | 48.5% |
| 8 | 24 | +8.0 pts | +3.2 pts | 52.0% |
| 9 | 25 | +11.5 pts | +6.3 pts | 55.3% |
| 10 | 25 | +17.6 pts | +4.7 pts | 54.6% |

## Does the model adjustment change the ranking at all?

Early excess and raw injury rate order the sites at a Spearman of **0.7909**, so the two predictors above are related but not the same list. Predicted rates run 20.8% to 63.9% across sites (sd 0.081, against 0.133 for observed rates), so the adjustment is substantial rather than cosmetic.

## Sensitivity

Neither the split year nor the crash threshold was tuned. 2024 is the headline because it was chosen before the first run, not because of how it came out.

| Split | Min crashes | Sites | Model excess | Injury rate | p |
|---|---|---|---|---|---|
| 2023 | 15 | 139 | 0.220 | 0.232 | 0.00599 |
| 2023 | 20 | 58 | 0.184 | 0.211 | 0.10379 |
| 2023 | 25 | 27 | _too few to rank_ | | |
| 2024 | 15 | 466 | 0.275 | 0.299 | 0.002 |
| 2024 | 20 | 247 | 0.257 | 0.262 | 0.002 |
| 2024 | 25 | 133 | 0.274 | 0.326 | 0.00599 |
| 2025 | 15 | 293 | 0.277 | 0.220 | 0.002 |
| 2025 | 20 | 127 | 0.296 | 0.206 | 0.002 |
| 2025 | 25 | 63 | 0.298 | 0.217 | 0.00998 |

## What a visit to the top of the list would have found

- Of the top 50 by the early ranking, **64%** were still above expectation in the later window, against a base rate of 57% (1.13x lift).
- Their mean late excess was +4.0 points, against +0.8 for the rest.

## Regression to the mean

The top 50 sites averaged +13.9 points of excess in the early window and +4.0 in the later one, retaining 29%. Some shrinkage is arithmetic rather than failure: ranking on a noisy estimate selects sites whose noise ran high. The permutation null above is what separates the two.
