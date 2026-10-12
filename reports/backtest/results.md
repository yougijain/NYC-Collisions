# Does the watchlist find sites that stay bad?

Fit on **2022–2023**, checked against **2024–2026**. 247 sites clear 20 scored crashes in both windows.

## Headline

Early excess predicts late excess at a Spearman correlation of **0.2625** (permutation null -0.0013 ± 0.0633, p = 0.0005).

Sorting by raw injury rate instead, with no model, predicts the same target at **0.2574**.

## Every ordering, against both targets

| Predictor | Target | Spearman |
|---|---|---|
| model excess | late excess | 0.263 |
| model excess, lower 95% | late excess | 0.259 |
| observed injury rate | late excess | 0.257 |
| crash count | late excess | 0.050 |
| model excess | late injury rate | 0.264 |
| model excess, lower 95% | late injury rate | 0.241 |
| observed injury rate | late injury rate | 0.469 |
| crash count | late injury rate | -0.054 |

## By decile of the early ranking

| Decile | Sites | Early excess | Late excess | Late injury rate |
|---|---|---|---|---|
| 1 | 25 | -16.7 pts | -3.4 pts | 43.8% |
| 2 | 25 | -10.0 pts | -2.5 pts | 44.8% |
| 3 | 24 | -6.3 pts | +0.6 pts | 49.3% |
| 4 | 25 | -3.5 pts | +1.6 pts | 48.8% |
| 5 | 25 | -1.0 pts | +2.3 pts | 50.2% |
| 6 | 24 | +1.4 pts | +2.7 pts | 52.5% |
| 7 | 25 | +4.6 pts | -0.1 pts | 48.5% |
| 8 | 24 | +7.9 pts | +2.9 pts | 52.3% |
| 9 | 25 | +11.6 pts | +6.0 pts | 54.8% |
| 10 | 25 | +17.6 pts | +5.0 pts | 54.6% |

## Does the model adjustment change the ranking at all?

Early excess and raw injury rate order the sites at a Spearman of **0.7888**, so the two predictors above are related but not the same list. Predicted rates run 20.5% to 64.2% across sites (sd 0.081, against 0.133 for observed rates), so the adjustment is substantial rather than cosmetic.

## Sensitivity

Neither the split year nor the crash threshold was tuned. 2024 is the headline because it was chosen before the first run, not because of how it came out.

| Split | Min crashes | Sites | Model excess | Injury rate | p |
|---|---|---|---|---|---|
| 2023 | 15 | 139 | 0.229 | 0.237 | 0.00399 |
| 2023 | 20 | 58 | 0.201 | 0.224 | 0.07784 |
| 2023 | 25 | 27 | _too few to rank_ | | |
| 2024 | 15 | 466 | 0.275 | 0.297 | 0.002 |
| 2024 | 20 | 247 | 0.263 | 0.257 | 0.002 |
| 2024 | 25 | 133 | 0.284 | 0.326 | 0.00399 |
| 2025 | 15 | 293 | 0.281 | 0.220 | 0.002 |
| 2025 | 20 | 127 | 0.310 | 0.217 | 0.002 |
| 2025 | 25 | 63 | 0.310 | 0.224 | 0.00998 |

## What a visit to the top of the list would have found

- Of the top 50 by the early ranking, **64%** were still above expectation in the later window, against a base rate of 56% (1.14x lift).
- Their mean late excess was +4.1 points, against +0.9 for the rest.

## Regression to the mean

The top 50 sites averaged +13.9 points of excess in the early window and +4.1 in the later one, retaining 29%. Some shrinkage is arithmetic rather than failure: ranking on a noisy estimate selects sites whose noise ran high. The permutation null above is what separates the two.
