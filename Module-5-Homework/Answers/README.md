# Module 5 Homework: Data Platforms (Bruin)

## Question 1:
**Answer:** `.bruin.yml and pipeline.yml (assets can be anywhere)`
**Explanation:** A Bruin project strictly requires `.bruin.yml` at the root to define environments/connections, and `pipeline.yml` to define pipeline configurations. The actual asset scripts (SQL, Python) don't strictly require an `assets/` folder, as Bruin recursively parses files looking for `@bruin` block annotations.

## Question 2: 
**Answer:** `time_interval - incremental based on a time column`
**Explanation:** The `time_interval` strategy is designed specifically for time-bound data processing. It automatically deletes the existing records within the specified `start_date` and `end_date` boundary before inserting the new results, guaranteeing idempotency and preventing duplicate rows.

## Question 3: 
**Answer:** `bruin run --var 'taxi_types=["yellow"]'`
**Explanation:** Pipeline variables are overridden at runtime using the `--var` flag. Since the schema for `taxi_types` explicitly defines it as an `array` of strings, the override value must be passed as a valid JSON array format.

## Question 4: 
**Answer:** `bruin run ingestion/trips.py --downstream`
**Explanation:** Bruin CLI executes based on file paths. The `--downstream` flag tells the orchestrator to run the specific asset (`trips.py`) and sequentially trigger all other assets that depend on it in the lineage graph.

## Question 5: 
**Answer:** `name: not_null`
**Explanation:** Bruin integrates data quality testing directly into the asset definition. To enforce a constraint that prevents empty values in critical columns like `pickup_datetime`, the built-in `not_null` check is applied at the column level. This acts as an automated assertion during pipeline execution.

## Question 6: 
**Answer:** `bruin lineage`
**Explanation:** Understanding the Directed Acyclic Graph (DAG) is essential for data reliability. The `bruin lineage` command parses the `depends` block in each asset and visualizes the dependency tree, showing the exact execution order from ingestion to final reporting.

## Question 7: 
**Answer:** `--full-refresh`
**Explanation:** For idempotency and clean state initialization, the `--full-refresh` flag is critical. When running on a new DuckDB database or when a complete rebuild is needed, this flag overrides incremental logic (like `append` or `time_interval`), forcing the orchestrator to truncate and recreate the tables from scratch.