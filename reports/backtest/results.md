# Does the watchlist find sites that stay bad?

Fit on **2022–2023**, checked against **2024–2026**. 271 sites clear 20 scored crashes in both windows.

## Headline

Early excess predicts late excess at a Spearman correlation of **0.2528** (permutation null 0.0018 ± 0.061, p = 0.0005).

Sorting by raw injury rate instead, with no model, predicts the same target at **0.253**.

## Every ordering, against both targets

| Predictor | Target | Spearman |
|---|---|---|
| model excess | late excess | 0.253 |
| model excess, pooled | late excess | 0.251 |
| model excess, lower 95% | late excess | 0.247 |
| observed injury rate | late excess | 0.253 |
| observed injury rate, pooled | late excess | 0.250 |
| crash count | late excess | 0.012 |
| model excess | late injury rate | 0.238 |
| model excess, pooled | late injury rate | 0.232 |
| model excess, lower 95% | late injury rate | 0.214 |
| observed injury rate | late injury rate | 0.445 |
| observed injury rate, pooled | late injury rate | 0.441 |
| crash count | late injury rate | -0.081 |

## By decile of the early ranking

| Decile | Sites | Early excess | Late excess | Late injury rate |
|---|---|---|---|---|
| 1 | 28 | -16.2 pts | -3.0 pts | 44.5% |
| 2 | 27 | -9.8 pts | -0.8 pts | 47.6% |
| 3 | 27 | -6.2 pts | +2.7 pts | 51.0% |
| 4 | 27 | -3.2 pts | +2.1 pts | 49.3% |
| 5 | 27 | -0.8 pts | +2.7 pts | 51.5% |
| 6 | 27 | +1.8 pts | +2.9 pts | 53.4% |
| 7 | 27 | +4.7 pts | +3.4 pts | 51.2% |
| 8 | 27 | +7.9 pts | +2.0 pts | 52.1% |
| 9 | 27 | +11.6 pts | +6.2 pts | 54.1% |
| 10 | 27 | +18.1 pts | +6.3 pts | 56.9% |

## Does the model adjustment change the ranking at all?

Early excess and raw injury rate order the sites at a Spearman of **0.7873**, so the two predictors above are related but not the same list. Predicted rates run 20.9% to 64.4% across sites (sd 0.080, against 0.130 for observed rates), so the adjustment is substantial rather than cosmetic.

## Sensitivity

Neither the split year nor the crash threshold was tuned. 2024 is the headline because it was chosen before the first run, not because of how it came out.

| Split | Min crashes | Sites | Model excess | Injury rate | Model, pooled | Rate, pooled | p |
|---|---|---|---|---|---|---|---|
| 2023 | 15 | 139 | 0.194 | 0.226 | 0.182 | 0.202 | 0.01996 |
| 2023 | 20 | 58 | 0.154 | 0.161 | 0.149 | 0.146 | 0.12774 |
| 2023 | 25 | 27 | _too few to rank_ | | | | |
| 2024 | 15 | 523 | 0.266 | 0.292 | 0.265 | 0.291 | 0.002 |
| 2024 | 20 | 271 | 0.253 | 0.253 | 0.251 | 0.250 | 0.002 |
| 2024 | 25 | 149 | 0.267 | 0.271 | 0.254 | 0.256 | 0.002 |
| 2025 | 15 | 469 | 0.264 | 0.251 | 0.254 | 0.242 | 0.002 |
| 2025 | 20 | 229 | 0.327 | 0.234 | 0.314 | 0.224 | 0.002 |
| 2025 | 25 | 118 | 0.307 | 0.242 | 0.291 | 0.225 | 0.002 |

## Partial pooling

Sites in the early window truly differ by about 6.0 points of excess (between-site sd), against a median sampling sd of 8.4 points for one site's own estimate. So a typical site keeps 34% of its deviation from the mean, ranging from 25% to 67%.

- Pooled early excess predicts late excess at **0.2507** (p = 0.0005), against **0.25** for raw injury rates pooled the same way.
- Across 8 split-and-threshold combinations, pooling improves the model's ordering in 0 and the pooled model beats the pooled naive ordering in 5.
- The top 50 by pooled excess: 72% still above expectation against a 59% base rate.
- Their pooled early excess was +5.9 points and their late excess +5.5, retaining 92%. A calibrated pooled estimate would retain about all of it; well under that means it was not pulled hard enough.

## Noise floor

Everything above rescored under 5 model seeds. Only the early-stopping holdout changes between them, so any difference smaller than this spread is not a difference.

| Predictor | Mean | sd | Range |
|---|---|---|---|
| model excess | 0.259 | 0.005 | 0.253 to 0.263 |
| model excess, pooled | 0.256 | 0.005 | 0.251 to 0.261 |
| model excess, lower 95% | 0.252 | 0.005 | 0.246 to 0.258 |
| observed injury rate | 0.253 | 0.003 | 0.249 to 0.257 |
| observed injury rate, pooled | 0.250 | 0.003 | 0.246 to 0.254 |
| crash count | 0.009 | 0.006 | 0.002 to 0.017 |

| Comparison | Mean difference | sd | Wins |
|---|---|---|---|
| pooling, model | -0.0033 | 0.0019 | 0 of 5 |
| model vs naive, pooled | +0.0054 | 0.0031 | 5 of 5 |
| model vs naive, raw | +0.0056 | 0.0038 | 4 of 5 |

## What a visit to the top of the list would have found

- Of the top 50 by the early ranking, **74%** were still above expectation in the later window, against a base rate of 59% (1.26x lift).
- Their mean late excess was +5.5 points, against +1.8 for the rest.

## Regression to the mean

The top 50 sites averaged +14.6 points of excess in the early window and +5.5 in the later one, retaining 38%. Some shrinkage is arithmetic rather than failure: ranking on a noisy estimate selects sites whose noise ran high. The permutation null above is what separates the two.
