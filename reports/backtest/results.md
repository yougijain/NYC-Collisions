# Does the watchlist find sites that stay bad?

Fit on **2022–2023**, checked against **2024–2026**. 247 sites clear 20 scored crashes in both windows.

## Headline

Early excess predicts late excess at a Spearman correlation of **0.2557** (permutation null -0.0006 ± 0.0637, p = 0.0015).

Sorting by raw injury rate instead, with no model, predicts the same target at **0.2575**.

## Every ordering, against both targets

| Predictor | Target | Spearman |
|---|---|---|
| model excess | late excess | 0.256 |
| model excess, lower 95% | late excess | 0.251 |
| observed injury rate | late excess | 0.258 |
| crash count | late excess | 0.051 |
| model excess | late injury rate | 0.266 |
| model excess, lower 95% | late injury rate | 0.240 |
| observed injury rate | late injury rate | 0.469 |
| crash count | late injury rate | -0.054 |

## By decile of the early ranking

| Decile | Sites | Early excess | Late excess | Late injury rate |
|---|---|---|---|---|
| 1 | 25 | -16.8 pts | -3.4 pts | 44.1% |
| 2 | 25 | -10.1 pts | -2.5 pts | 44.4% |
| 3 | 24 | -6.3 pts | -0.2 pts | 48.1% |
| 4 | 25 | -3.5 pts | +1.7 pts | 48.8% |
| 5 | 25 | -1.0 pts | +3.3 pts | 51.7% |
| 6 | 24 | +1.6 pts | +2.3 pts | 52.2% |
| 7 | 25 | +4.7 pts | +0.1 pts | 48.0% |
| 8 | 24 | +8.0 pts | +3.4 pts | 53.4% |
| 9 | 25 | +11.5 pts | +5.3 pts | 54.3% |
| 10 | 25 | +17.6 pts | +5.0 pts | 54.6% |

## Does the model adjustment change the ranking at all?

Early excess and raw injury rate order the sites at a Spearman of **0.792**, so the two predictors above are related but not the same list. Predicted rates run 20.6% to 64.4% across sites (sd 0.081, against 0.133 for observed rates), so the adjustment is substantial rather than cosmetic.

## Sensitivity

Neither the split year nor the crash threshold was tuned. 2024 is the headline because it was chosen before the first run, not because of how it came out.

| Split | Min crashes | Sites | Model excess | Injury rate | p |
|---|---|---|---|---|---|
| 2023 | 15 | 139 | 0.223 | 0.232 | 0.00399 |
| 2023 | 20 | 58 | 0.194 | 0.213 | 0.09381 |
| 2023 | 25 | 27 | _too few to rank_ | | |
| 2024 | 15 | 466 | 0.280 | 0.301 | 0.002 |
| 2024 | 20 | 247 | 0.256 | 0.258 | 0.002 |
| 2024 | 25 | 133 | 0.278 | 0.323 | 0.002 |
| 2025 | 15 | 293 | 0.283 | 0.222 | 0.002 |
| 2025 | 20 | 127 | 0.301 | 0.210 | 0.002 |
| 2025 | 25 | 63 | 0.296 | 0.221 | 0.01198 |

## What a visit to the top of the list would have found

- Of the top 50 by the early ranking, **66%** were still above expectation in the later window, against a base rate of 58% (1.14x lift).
- Their mean late excess was +4.0 points, against +0.9 for the rest.

## Regression to the mean

The top 50 sites averaged +13.9 points of excess in the early window and +4.0 in the later one, retaining 29%. Some shrinkage is arithmetic rather than failure: ranking on a noisy estimate selects sites whose noise ran high. The permutation null above is what separates the two.
