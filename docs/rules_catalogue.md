# Detection rules catalogue

All thresholds live in `config/rules.yml` and are **illustrative assumptions, not regulatory values**. Calibrate against labelled data before drawing conclusions. Scores follow `min(1, 0.5 * observed / threshold)`. Hits are de-duplicated to one per rule, account and day.

| Rule | Typology | Logic | Default parameters |
|---|---|---|---|
| R001 Structuring | Structuring | Sender has at least `min_count` transactions between `lower_ratio x threshold` and `threshold` within `window_days` | threshold 50,000 AED; lower ratio 0.8; 3 txns; 3 days |
| R002 Rapid pass-through | Rapid movement | Rolling inflow at least `min_amount` and outflow at least `out_in_ratio` of inflow within `window_days` | 50,000 AED; 0.9; 2 days |
| R003 High-risk jurisdiction | Country exposure | Cross-border transaction of at least `min_amount` where either country is in a listed tier **at event time** (SCD2 lookup) | 10,000 AED; tier HIGH |
| R004 Fan-out / fan-in | Fan-out and fan-in | Distinct counterparties within `window_days` at least `min_counterparties` (sender or receiver side) | 8; 1 day |
| R005 Round amounts | Round-amount transfers | At least `min_count` round-unit multiples of at least `min_amount` by one sender within `window_days` | unit 1,000; 10,000 AED; 3; 7 days |
| S001 Sanctions screening | Sanctions | Name similarity of customer vs latest list snapshot: REVIEW from 80, STRONG from 92 | `config/screening.yml` |

## Known limitations

* R002 will also flag legitimate pass-through accounts (payment processors, treasury accounts); no entity-type segmentation exists.
* R003 volume depends entirely on the country tier table; the demo table marks two countries HIGH for illustration only.
* Screening uses names only. No date of birth, nationality or identifier corroboration, no transliteration across scripts, no handling of alias strength.
* Evaluation is account level against originator accounts. Typology labels are per transaction in the source and are not used for rule tuning.

## Metrics on the bundled synthetic sample (local run)

Overall (any rule): precision 0.57, recall 0.81 against originator accounts. Per rule: R001 and R002 precision 1.0 (recall 0.19), R004 and R005 precision 1.0 (recall 0.14), R003 precision 0.35 (recall 0.33). Screening: 18 of 18 seeded names caught, 12 additional customers flagged. These figures describe synthetic data generated to contain these patterns and say nothing about real-world performance.
