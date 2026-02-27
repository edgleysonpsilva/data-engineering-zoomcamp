# Module 5 Homework: Data Platforms (Bruin)

## Question 1: Bruin Pipeline Structure
**Answer:** `.bruin.yml and pipeline.yml (assets can be anywhere)`
**Explanation:** A Bruin project strictly requires `.bruin.yml` at the root to define environments/connections, and `pipeline.yml` to define pipeline configurations. The actual asset scripts (SQL, Python) don't strictly require an `assets/` folder, as Bruin recursively parses files looking for `@bruin` block annotations.

## Question 2: Materialization Strategies
**Answer:** `time_interval - incremental based on a time column`
**Explanation:** The `time_interval` strategy is designed specifically for time-bound data processing. It automatically deletes the existing records within the specified `start_date` and `end_date` boundary before inserting the new results, guaranteeing idempotency and preventing duplicate rows.

## Question 3: Pipeline Variables
**Answer:** `bruin run --var 'taxi_types=["yellow"]'`
**Explanation:** Pipeline variables are overridden at runtime using the `--var` flag. Since the schema for `taxi_types` explicitly defines it as an `array` of strings, the override value must be passed as a valid JSON array format.

## Question 4: Running with Dependencies
**Answer:** `bruin run ingestion/trips.py --downstream`
**Explanation:** Bruin CLI executes based on file paths. The `--downstream` flag tells the orchestrator to run the specific asset (`trips.py`) and sequentially trigger all other assets that depend on it in the lineage graph.