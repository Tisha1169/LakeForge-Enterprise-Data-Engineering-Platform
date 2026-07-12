# great_expectations/

Data quality validation suites, run between Bronze and Silver (and optionally
Silver and Gold). Built out in Phase 14.

Will contain:
- Expectation suites per table (null checks, uniqueness, referential
  integrity, schema drift detection, value-range/format checks).
- Checkpoint configs wiring suites to specific pipeline runs.
- Generated HTML data-docs reports (gitignored — regenerated per run, not
  committed).

A failed expectation suite quarantines the batch rather than silently letting
bad data flow into Silver/Gold.
