# Module 5 Homework: Data Platforms with Bruin

In this homework, we'll use Bruin to build a complete data pipeline, from ingestion to reporting.

## Setup

Install Bruin CLI:
```bash
curl -LsSf https://getbruin.com/install/cli | sh
```

Initialize the zoomcamp template:
```bash
bruin init zoomcamp my-pipeline
```

- Configure your `.bruin.yml` with a DuckDB connection
- Follow the tutorial in the main module README

After completing the setup, you should have a working NYC taxi data pipeline.

---

## Question 1. Bruin Pipeline Structure

In a Bruin project, what are the required files/directories?

**Answer:** `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`

---

## Question 2. Materialization Strategies

You're building a pipeline that processes NYC taxi data organized by month based on `pickup_datetime`. Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?

**Answer:** `time_interval` — incremental based on a time column

---

## Question 3. Pipeline Variables

You have the following variable defined in `pipeline.yml`:

```yaml
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

How do you override this when running the pipeline to only process yellow taxis?

**Answer:**
```bash
bruin run --var 'taxi_types=["yellow"]'
```

---

## Question 4. Running with Dependencies

You've modified the `ingestion/trips.py` asset and want to run it plus all downstream assets. Which command should you use?

**Answer:**
```bash
bruin run --select ingestion.trips+
```

---

## Question 5. Quality Checks

You want to ensure the `pickup_datetime` column in your trips table never has NULL values. Which quality check should you add to your asset definition?

**Answer:** `name: not_null`

---

## Question 6. Lineage and Dependencies

After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?

**Answer:**
```bash
bruin lineage
```

---

## Question 7. First-Time Run

You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?

**Answer:** `--full-refresh`

---

## References

- [Bruin Documentation](https://bruin-data.github.io/bruin/)
- [Bruin Pipeline Structure](https://bruin-data.github.io/bruin/getting-started/concepts.html)
- [Bruin Materialization Strategies](https://bruin-data.github.io/bruin/assets/materialization.html)
- [Bruin Pipeline Variables](https://bruin-data.github.io/bruin/pipelines/variables.html)
- [Bruin CLI Reference](https://bruin-data.github.io/bruin/commands/run.html)
- [Bruin Quality Checks](https://bruin-data.github.io/bruin/assets/quality-checks.html)
- [Bruin Lineage Command](https://bruin-data.github.io/bruin/commands/lineage.html)