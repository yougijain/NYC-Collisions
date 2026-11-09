# Does the watchlist find sites that stay bad?

Fit on **2022–2023**, checked against **2024–2026**. 271 sites clear 20 scored crashes in both windows.

## Headline

Early excess predicts late excess at a Spearman correlation of **0.2563** (permutation null 0.0006 ± 0.0611, p = 0.0005).

Sorting by raw injury rate instead, with no model, predicts the same target at **0.254**.

## Every ordering, against both targets

| Predictor | Target | Spearman |
|---|---|---|
| model excess | late excess | 0.256 |
| model excess, pooled | late excess | 0.255 |
| model excess, lower 95% | late excess | 0.248 |
| observed injury rate | late excess | 0.254 |
| observed injury rate, pooled | late excess | 0.250 |
| crash count | late excess | 0.008 |
| model excess | late injury rate | 0.249 |
| model excess, pooled | late injury rate | 0.244 |
| model excess, lower 95% | late injury rate | 0.220 |
| observed injury rate | late injury rate | 0.450 |
| observed injury rate, pooled | late injury rate | 0.445 |
| crash count | late injury rate | -0.086 |

## By decile of the early ranking

| Decile | Sites | Early excess | Late excess | Late injury rate |
|---|---|---|---|---|
| 1 | 28 | -16.2 pts | -3.2 pts | 44.4% |
| 2 | 27 | -9.8 pts | -0.0 pts | 48.2% |
| 3 | 27 | -6.1 pts | +1.9 pts | 50.4% |
| 4 | 27 | -3.3 pts | +1.2 pts | 47.4% |
| 5 | 27 | -0.7 pts | +3.1 pts | 52.8% |
| 6 | 27 | +1.7 pts | +4.2 pts | 54.9% |
| 7 | 27 | +4.8 pts | +2.2 pts | 49.9% |
| 8 | 27 | +8.0 pts | +2.5 pts | 52.9% |
| 9 | 27 | +11.8 pts | +5.2 pts | 53.6% |
| 10 | 27 | +18.3 pts | +6.6 pts | 56.9% |

## Does the model adjustment change the ranking at all?

Early excess and raw injury rate order the sites at a Spearman of **0.7923**, so the two predictors above are related but not the same list. Predicted rates run 20.6% to 64.3% across sites (sd 0.079, against 0.130 for observed rates), so the adjustment is substantial rather than cosmetic.

## Sensitivity

Neither the split year nor the crash threshold was tuned. 2024 is the headline because it was chosen before the first run, not because of how it came out.

| Split | Min crashes | Sites | Model excess | Injury rate | Model, pooled | Rate, pooled | p |
|---|---|---|---|---|---|---|---|
| 2023 | 15 | 139 | 0.204 | 0.230 | 0.194 | 0.205 | 0.00599 |
| 2023 | 20 | 58 | 0.166 | 0.159 | 0.161 | 0.141 | 0.11776 |
| 2023 | 25 | 27 | _too few to rank_ | | | | |
| 2024 | 15 | 523 | 0.274 | 0.294 | 0.273 | 0.293 | 0.002 |
| 2024 | 20 | 271 | 0.256 | 0.254 | 0.255 | 0.250 | 0.002 |
| 2024 | 25 | 149 | 0.272 | 0.273 | 0.263 | 0.258 | 0.002 |
| 2025 | 15 | 471 | 0.267 | 0.251 | 0.257 | 0.242 | 0.002 |
| 2025 | 20 | 237 | 0.330 | 0.235 | 0.319 | 0.227 | 0.002 |
| 2025 | 25 | 118 | 0.322 | 0.259 | 0.306 | 0.242 | 0.002 |

## Partial pooling

Sites in the early window truly differ by about 6.0 points of excess (between-site sd), against a median sampling sd of 8.4 points for one site's own estimate. So a typical site keeps 34% of its deviation from the mean, ranging from 25% to 67%.

- Pooled early excess predicts late excess at **0.2551** (p = 0.0005), against **0.2502** for raw injury rates pooled the same way.
- Across 8 split-and-threshold combinations, pooling improves the model's ordering in 0 and the pooled model beats the pooled naive ordering in 6.
- The top 50 by pooled excess: 70% still above expectation against a 57% base rate.
- Their pooled early excess was +6.0 points and their late excess +5.8, retaining 96%. A calibrated pooled estimate would retain about all of it; well under that means it was not pulled hard enough.

## Noise floor

Everything above rescored under 5 model seeds. Only the early-stopping holdout changes between them, so any difference smaller than this spread is not a difference.

| Predictor | Mean | sd | Range |
|---|---|---|---|
| model excess | 0.261 | 0.004 | 0.256 to 0.267 |
| model excess, pooled | 0.258 | 0.005 | 0.253 to 0.266 |
| model excess, lower 95% | 0.251 | 0.005 | 0.245 to 0.257 |
| observed injury rate | 0.257 | 0.007 | 0.250 to 0.269 |
| observed injury rate, pooled | 0.254 | 0.007 | 0.247 to 0.265 |
| crash count | 0.003 | 0.006 | -0.007 to 0.008 |

| Comparison | Mean difference | sd | Wins |
|---|---|---|---|
| pooling, model | -0.0025 | 0.0015 | 0 of 5 |
| model vs naive, pooled | +0.0047 | 0.0060 | 4 of 5 |
| model vs naive, raw | +0.0036 | 0.0060 | 4 of 5 |

## What a visit to the top of the list would have found

- Of the top 50 by the early ranking, **70%** were still above expectation in the later window, against a base rate of 57% (1.23x lift).
- Their mean late excess was +5.2 points, against +1.7 for the rest.

## Regression to the mean

The top 50 sites averaged +14.7 points of excess in the early window and +5.2 in the later one, retaining 35%. Some shrinkage is arithmetic rather than failure: ranking on a noisy estimate selects sites whose noise ran high. The permutation null above is what separates the two.
