# Reference data

* `fx_rates_illustrative.csv` – ILLUSTRATIVE placeholder rates (currency -> AED). Replace with a
  sourced, dated rate table before drawing any conclusions from amounts. Only USD reflects the
  well-known AED peg; every other figure is a rough placeholder.
* Country risk: `config/country_risk_template.csv` is a header-only template. Populate it from the
  FATF lists you decide to use (verify the current lists), with country names matching the
  transaction source (e.g. SAML-D location names).
