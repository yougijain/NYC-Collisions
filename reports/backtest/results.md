# Does the watchlist find sites that stay bad?

Fit on **2022–2023**, checked against **2024–2026**. 273 sites clear 20 scored crashes in both windows.

## Headline

Early excess predicts late excess at a Spearman correlation of **0.257** (permutation null 0.0012 ± 0.0607, p = 0.001).

Sorting by raw injury rate instead, with no model, predicts the same target at **0.2636**.

## Every ordering, against both targets

| Predictor | Target | Spearman |
|---|---|---|
| model excess | late excess | 0.257 |
| model excess, pooled | late excess | 0.253 |
| model excess, lower 95% | late excess | 0.249 |
| observed injury rate | late excess | 0.264 |
| observed injury rate, pooled | late excess | 0.260 |
| crash count | late excess | -0.001 |
| model excess | late injury rate | 0.244 |
| model excess, pooled | late injury rate | 0.236 |
| model excess, lower 95% | late injury rate | 0.217 |
| observed injury rate | late injury rate | 0.454 |
| observed injury rate, pooled | late injury rate | 0.449 |
| crash count | late injury rate | -0.092 |

## By decile of the early ranking

| Decile | Sites | Early excess | Late excess | Late injury rate |
|---|---|---|---|---|
| 1 | 28 | -16.2 pts | -3.4 pts | 44.4% |
| 2 | 27 | -9.8 pts | +0.5 pts | 48.4% |
| 3 | 27 | -6.2 pts | +2.2 pts | 51.9% |
| 4 | 27 | -3.4 pts | +1.4 pts | 47.3% |
| 5 | 28 | -0.8 pts | +3.1 pts | 53.1% |
| 6 | 27 | +1.7 pts | +3.8 pts | 54.3% |
| 7 | 27 | +4.8 pts | +2.0 pts | 51.0% |
| 8 | 27 | +8.0 pts | +2.2 pts | 50.9% |
| 9 | 27 | +11.8 pts | +5.6 pts | 54.7% |
| 10 | 28 | +18.6 pts | +6.6 pts | 57.0% |

## Does the model adjustment change the ranking at all?

Early excess and raw injury rate order the sites at a Spearman of **0.7888**, so the two predictors above are related but not the same list. Predicted rates run 20.0% to 64.3% across sites (sd 0.079, against 0.131 for observed rates), so the adjustment is substantial rather than cosmetic.

## Sensitivity

Neither the split year nor the crash threshold was tuned. 2024 is the headline because it was chosen before the first run, not because of how it came out.

| Split | Min crashes | Sites | Model excess | Injury rate | Model, pooled | Rate, pooled | p |
|---|---|---|---|---|---|---|---|
| 2023 | 15 | 139 | 0.182 | 0.208 | 0.172 | 0.185 | 0.00998 |
| 2023 | 20 | 58 | 0.139 | 0.146 | 0.138 | 0.129 | 0.15369 |
| 2023 | 25 | 27 | _too few to rank_ | | | | |
| 2024 | 15 | 524 | 0.270 | 0.287 | 0.270 | 0.286 | 0.002 |
| 2024 | 20 | 273 | 0.257 | 0.264 | 0.253 | 0.260 | 0.002 |
| 2024 | 25 | 149 | 0.283 | 0.289 | 0.268 | 0.273 | 0.002 |
| 2025 | 15 | 490 | 0.246 | 0.243 | 0.238 | 0.235 | 0.002 |
| 2025 | 20 | 246 | 0.300 | 0.241 | 0.289 | 0.234 | 0.002 |
| 2025 | 25 | 124 | 0.317 | 0.281 | 0.302 | 0.265 | 0.00399 |

## Partial pooling

Sites in the early window truly differ by about 6.0 points of excess (between-site sd), against a median sampling sd of 8.4 points for one site's own estimate. So a typical site keeps 34% of its deviation from the mean, ranging from 25% to 67%.

- Pooled early excess predicts late excess at **0.2529** (p = 0.0005), against **0.2602** for raw injury rates pooled the same way.
- Across 8 split-and-threshold combinations, pooling improves the model's ordering in 1 and the pooled model beats the pooled naive ordering in 4.
- The top 50 by pooled excess: 72% still above expectation against a 58% base rate.
- Their pooled early excess was +6.1 points and their late excess +5.6, retaining 92%. A calibrated pooled estimate would retain about all of it; well under that means it was not pulled hard enough.

## Noise floor

Everything above rescored under 5 model seeds. Only the early-stopping holdout changes between them, so any difference smaller than this spread is not a difference.

| Predictor | Mean | sd | Range |
|---|---|---|---|
| model excess | 0.261 | 0.007 | 0.252 to 0.269 |
| model excess, pooled | 0.258 | 0.007 | 0.248 to 0.266 |
| model excess, lower 95% | 0.252 | 0.005 | 0.244 to 0.257 |
| observed injury rate | 0.264 | 0.006 | 0.255 to 0.273 |
| observed injury rate, pooled | 0.261 | 0.006 | 0.251 to 0.269 |
| crash count | 0.000 | 0.003 | -0.004 to 0.003 |

| Comparison | Mean difference | sd | Wins |
|---|---|---|---|
| pooling, model | -0.0032 | 0.0008 | 0 of 5 |
| model vs naive, pooled | -0.0029 | 0.0040 | 1 of 5 |
| model vs naive, raw | -0.0031 | 0.0037 | 1 of 5 |

## What a visit to the top of the list would have found

- Of the top 50 by the early ranking, **74%** were still above expectation in the later window, against a base rate of 58% (1.29x lift).
- Their mean late excess was +5.3 points, against +1.8 for the rest.

## Regression to the mean

The top 50 sites averaged +15.0 points of excess in the early window and +5.3 in the later one, retaining 35%. Some shrinkage is arithmetic rather than failure: ranking on a noisy estimate selects sites whose noise ran high. The permutation null above is what separates the two.
