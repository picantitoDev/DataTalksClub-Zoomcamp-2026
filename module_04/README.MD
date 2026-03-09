# Module 4 Homework: Analytics Engineering with dbt

This homework uses the dbt project in `04-analytics-engineering/taxi_rides_ny/` to transform NYC taxi data and answer analytical questions by querying the built models.

---

## Setup

1. Set up your dbt project following the setup guide
2. Load the following data into your warehouse *(use static tables from the DTC GitHub repo — not the official TLC tables, as some values may change over time)*:
   - Green and Yellow taxi data for 2019–2020
   - FHV trip data for 2019
3. Run `dbt build --target prod` to create all models and run tests

> **Note:** By default, dbt uses the `dev` target. You must use `--target prod` to build models in the production dataset, which is required for the queries below.

After a successful build, you should have models like `fct_trips`, `dim_zones`, and `fct_monthly_zone_revenue` in your warehouse.

---

## Question 1 — dbt Lineage and Execution

Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql  (depends on stg_green_tripdata & stg_yellow_tripdata)
```

**Question:** If you run `dbt run --select int_trips_unioned`, what models will be built?

**Run output:**

```
$ uv run dbt run --select int_trips_unioned.sql
19:37:12  Running with dbt=1.11.6
19:37:13  Registered adapter: duckdb=1.10.1
19:37:13  Found 10 models, 2 seeds, 19 data tests, 3 sources, 620 macros
19:37:13  Concurrency: 1 threads (target='dev')
19:37:13  1 of 1 START sql view model dev.int_trips_unioned .............................. [RUN]
19:37:13  1 of 1 OK created sql view model dev.int_trips_unioned ......................... [OK in 0.08s]
19:37:13  Finished running 1 view model in 0.17s.
19:37:13  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1
```

**Answer:** `int_trips_unioned` only

---

## Question 2 — dbt Tests

The following generic test is configured in `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

**Run output:**

```
$ uv run dbt test --select fct_trips --target prod
19:40:22  Running with dbt=1.11.5
19:40:23  Registered adapter: duckdb=1.10.0
19:40:25  Found 8 models, 2 seeds, 34 data tests, 2 sources, 618 macros
19:40:25  Concurrency: 1 threads (target='prod')

19:40:25   1 of 10 FAIL  accepted_values_fct_trips_payment_type__False__1__2__3__4__5
19:40:26   2 of 10 PASS  accepted_values_fct_trips_service_type__Green__Yellow
19:40:27   3 of 10 PASS  not_null_fct_trips_pickup_datetime
19:40:27   4 of 10 PASS  not_null_fct_trips_service_type
19:40:27   5 of 10 PASS  not_null_fct_trips_total_amount
19:40:28   6 of 10 PASS  not_null_fct_trips_trip_id
19:40:28   7 of 10 PASS  not_null_fct_trips_vendor_id
19:40:29   8 of 10 PASS  relationships_fct_trips_dropoff_location_id__location_id__ref_dim_zones_
19:40:30   9 of 10 PASS  relationships_fct_trips_pickup_location_id__location_id__ref_dim_zones_
19:40:57  10 of 10 PASS  unique_fct_trips_trip_id

19:40:57  Finished running 10 data tests in 32.46s.
19:40:58  Done. PASS=9 WARN=0 ERROR=1 SKIP=0 NO-OP=0 TOTAL=10
```

**Failure detail:**
```
Failure in test accepted_values_fct_trips_payment_type__False__1__2__3__4__5
  Got 1 result, configured to fail if != 0
  compiled code at target/compiled/taxi_rides_ny/models/marts/schema.yml/...
```

---

## Question 3 — Record Count in `fct_monthly_zone_revenue`

After running the dbt project, query the `fct_monthly_zone_revenue` model.

**Answer:** `12,184` records

---

## Question 4 — Best Performing Zone for Green Taxis (2020)

Using the `fct_monthly_zone_revenue` table, find the pickup zone with the highest total revenue (`revenue_monthly_total_amount`) for Green taxi trips in 2020.

**Answer:** East Harlem North

---

## Question 5 — Green Taxi Trip Counts (October 2019)

Using the `fct_monthly_zone_revenue` table, find the total number of trips (`total_monthly_trips`) for Green taxis in October 2019.

*(See query result screenshot)*

---

## Question 6 — Staging Model for FHV Data

### Task

Create a staging model for For-Hire Vehicle (FHV) trip data for 2019 with the following requirements:

- Filter out records where `dispatching_base_num IS NULL`
- Rename fields to match project naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

### Staging Model (`stg_fhv_tripdata.sql`)

```sql
with non_null as (

    select *
    from {{ source('raw', 'fhv_tripdata') }}
    where dispatching_base_num is not null

),

filtered as (

    select 
        cast(dispatching_base_num as string)    as dispatching_base_num,
        cast(pulocationid as int)               as pickup_location_id,
        cast(dolocationid as int)               as dropoff_location_id,
        cast(pickup_datetime as timestamp)      as pickup_datetime,
        cast(dropoff_datetime as timestamp)     as dropoff_datetime,
        cast(sr_flag as int64)                  as shared_ride_flag
    from non_null   

)

select *
from filtered
```

### Run output

```
$ uv run dbt run --select stg_fhv_tripdata --target prod
20:45:51  Running with dbt=1.11.6
20:45:51  Registered adapter: duckdb=1.10.1
20:45:52  Found 10 models, 2 seeds, 20 data tests, 3 sources, 620 macros
20:45:52  Concurrency: 1 threads (target='prod')
20:45:52  1 of 1 START sql view model prod.stg_fhv_tripdata .............................. [RUN]
20:45:52  1 of 1 OK created sql view model prod.stg_fhv_tripdata ......................... [OK in 0.07s]
20:45:52  Finished running 1 view model in 0.16s.
20:45:52  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1
```

**Answer:** `43,244,693` records in `stg_fhv_tripdata`