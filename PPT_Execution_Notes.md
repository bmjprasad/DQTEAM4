## Auto DQ and ML Anomaly Detection - Execution Notes (for PPT)

### Objective
- Automatically generate DQ rules from metadata and profile stats
- Detect anomalies without predefined rules by learning data patterns

### Architecture (High-level)
- Ingest table from Unity Catalog: `catalog.schema.table`
- Notebook 1: Auto DQ Rule Generator
  - Profiles columns (nulls, distincts, quantiles, patterns)
  - Generates candidate rules: NOT NULL, UNIQUE, RANGE, LENGTH, DOMAIN, PATTERN
  - Writes to Delta table: `<catalog>.dq.dq_rule_generator`
- Notebook 2: ML-based Anomaly Detection
  - Column-level: numeric (quantile tails), string length, rare categories
  - Row-level: aggregate per-column anomaly signals to score rows
  - Relationship-level: numeric residual anomalies; rare categorical pairs
  - Writes to Delta table: `<catalog>.dq.dq_anomalies`

### Prereqs
- Databricks cluster (DBR 12+), Unity Catalog enabled
- Permissions to read target table and write to `<catalog>.dq` schema

### Parameters (Widgets)
- Auto DQ: `catalog`, `schema`, `table`, `sampleFraction`, `minConfidence`, `coverageThreshold`
- ML Anomaly: `catalog`, `schema`, `table`, `idColumns`, `sampleFraction`, `mode`

### Run Steps
1. Open notebook: AutoDQ_Rule_Generator
   - Set widgets and run all cells
   - Output: rules preview, and persisted Delta table `dq_rule_generator`
2. Open notebook: AutoDQ_ML_Anomaly_Detection
   - Set widgets (provide `idColumns` if available) and run all
   - Output: anomalies by type and top anomalies

### Rule Types (Heuristics)
- NOT NULL: coverage ≥ `coverageThreshold`
- UNIQUE (approx): approx distinct/non-null ≥ 0.999 and coverage ≥ threshold
- RANGE (numeric/date): BETWEEN q01 and q99
- LENGTH RANGE (string): length BETWEEN len_q01 and len_q99
- DOMAIN: top values covering ≥ `coverageThreshold` (≤20 values)
- PATTERN: best regex with coverage ≥ `coverageThreshold`

### Anomaly Types
- Column: numeric tail values, string length outliers, rare categories
- Row: sum of signals across columns; top 1% by score
- Relationship: high-corr numeric residuals; rare categorical pairs

### Tables
- Rules: `<catalog>.dq.dq_rule_generator`
  - Columns: catalog, schema, table, column, rule_type, rule_expression, confidence, coverage, support, row_count, profile_json, provenance, created_at, rule_id
- Anomalies: `<catalog>.dq.dq_anomalies`
  - Columns: catalog, schema, table, anomaly_type, columns, row_id, anomaly_score, details, created_at

### Ops & Governance
- Partitioned Delta tables; 30-day log retention
- Overwrite rules per table by DELETE and append
- Use scheduled jobs to refresh rules/anomalies

### Limitations & Next Steps
- Approximations used (quantiles, approx distinct)
- Consider Isolation Forest/Autoencoder for richer row scoring
- Add cross-table referential integrity checks