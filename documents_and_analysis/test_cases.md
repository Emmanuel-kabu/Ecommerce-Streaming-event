#  E-commerce Streaming Analytics Platform - Test Cases

## Overview

This document outlines test cases for validating the functionality, performance, and reliability of the E-commerce Streaming Analytics Platform.

### Result Recording Conventions
- **Status** values: `✅ Passed` / `❌ Failed` / `⏳ Pending` / `⚠️ Blocked`
- **Actual Results** should include:
  - Record counts (generated/processed/stored)
  - Key log excerpts (include log file path + timestamps)
  - Evidence queries (copy/paste SQL and output)
  - Environment (local vs Docker, Spark master URL, DB host)

---

## Test Environment Setup

### Prerequisites
- PostgreSQL running and reachable
- Spark runtime available (pyspark installed + Java) OR Docker Compose stack running
- Test scripts available: `tests/setup_test_environment.py`, `tests/generate_test_data.py`, `tests/test_stream_to_postgres.py`

### Test Data Preparation
```bash
# Create test environment
python tests/setup_test_environment.py

# Generate controlled test data
python tests/generate_test_data.py --count 1000 --batch-size 100 --categories all --out-dir data/incoming

# Integration smoke test: stream to PostgreSQL
python tests/test_stream_to_postgres.py --count 50 --timeout-seconds 90
```

### Common Verification Queries (PostgreSQL)
```sql
SELECT COUNT(*) FROM ecommerce_events;

SELECT event_id, event_type, category, price, event_timestamp
FROM ecommerce_events
ORDER BY created_at DESC
LIMIT 20;

SELECT
  SUM(CASE WHEN event_id IS NULL THEN 1 ELSE 0 END) AS null_event_id,
  SUM(CASE WHEN event_type IS NULL THEN 1 ELSE 0 END) AS null_event_type,
  SUM(CASE WHEN event_timestamp IS NULL THEN 1 ELSE 0 END) AS null_event_ts
FROM ecommerce_events;
```

---

## 1. Data Generation Component Tests

### Test Case DG-001: Basic Data Generation
**Objective:** Verify the generator creates valid CSV files.

**Test Steps:**
1. Run:
   ```bash
   python tests/generate_test_data.py --count 60 --batch-size 10 --out-dir data/incoming --seed 42
   ```
2. Check `data/incoming/` for generated files.

**Expected Results:**
- At least 6 CSV files created
- Each file contains exactly 10 records + header
- All required columns present in each file
- Valid ISO timestamps in `event_timestamp`
- No duplicate `event_id` values within a single file

**Evidence / Verification (PowerShell):**
```powershell
$files = Get-ChildItem data\incoming -Filter *.csv
$files.Count

$f = ($files | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
(Import-Csv $f | Measure-Object).Count

# Header
Get-Content $f -TotalCount 1

# Duplicate event_ids in a file (expect 0)
(Import-Csv $f | Group-Object event_id | Where-Object { $_.Count -gt 1 }).Count
```

**Actual Results:** _TBD (during testing)_
**Status:** ⏳ Pending
**Notes:** _TBD (optional)_

---

### Test Case DG-002: Data Schema Validation
**Objective:** Ensure generated data adheres to expected schema and constraints.

**Test Steps:**
1. Generate a deterministic dataset:
   ```bash
   python tests/generate_test_data.py --count 100 --batch-size 100 --out-dir data/incoming --seed 42
   ```
2. Validate column presence and basic formats.

**Expected Results:**
- `event_id`: UUID-like format
- `event_type`: one of `[view, purchase, add_to_cart, add_to_wishlist, order]`
- `product_id`: positive integer
- `price`: decimal (2 dp)
- `customer_email`: contains `@` and `.`

**Evidence / Verification (PowerShell spot checks):**
```powershell
$f = (Get-ChildItem data\incoming -Filter *.csv | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName

$allowed = @('view','purchase','add_to_cart','add_to_wishlist','order')

(Import-Csv $f | Where-Object { $_.event_id -match '^[0-9a-fA-F-]{36}$' }).Count
(Import-Csv $f | Where-Object { $allowed -contains $_.event_type }).Count
```

**Actual Results:** _TBD (during testing)_
**Status:** ⏳ Pending
**Notes:** _TBD (optional)_

---

### Test Case DG-003: Data Volume and Performance
**Objective:** Validate the generator can produce high volumes consistently.

**Test Steps:**
1. Run a high-volume generation (adjust to your machine):
   ```bash
   python tests/generate_test_data.py --count 300000 --batch-size 1000 --out-dir data/incoming --seed 42
   ```
2. Verify record totals across output files.

**Expected Results:**
- ~300,000 records generated
- Files remain well-formed CSVs

**Evidence / Verification (PowerShell):**
```powershell
$files = Get-ChildItem data\incoming -Filter *.csv
$total = 0
foreach ($f in $files) { $total += (Import-Csv $f.FullName | Measure-Object).Count }
$total
```

**Actual Results:** _TBD (during testing)_
**Status:** ⏳ Pending
**Notes:** _TBD (optional)_

---

## 2. Spark Streaming Processing Tests

### Test Case SP-001: Basic Stream Processing
**Objective:** Verify Spark processes CSV files and inserts rows into PostgreSQL.

**Prerequisites:**
- PostgreSQL running and reachable
- `POSTGRES_PASSWORD` set in the environment

**Test Steps:**
1. Run the integration smoke test:
   ```bash
   python tests/test_stream_to_postgres.py --count 50 --timeout-seconds 90
   ```
2. Inspect Spark logs.
3. Verify rows exist in the created test table.

**Expected Results:**
- Spark starts successfully and writes at least 1 row within the timeout
- Logs show successful batch writes
- PostgreSQL has rows in the `ecommerce_events_test_<run_id>` table created by the test

**Evidence / Verification:**
- Spark log file: `logs/spark_streaming_YYYYMMDD.log`
- PostgreSQL:
  ```sql
  SELECT COUNT(*) FROM ecommerce_events_test_<run_id>;
  SELECT event_type, COUNT(*) FROM ecommerce_events_test_<run_id> GROUP BY event_type;
  ```

**Actual Results:** _TBD (during testing)_
**Status:** ⏳ Pending
**Notes:** _TBD (optional)_

---

### Test Case SP-002: Data Validation and Cleansing
**Objective:** Test Spark's data validation capabilities

**Test Steps:**
1. Create CSV with invalid data:
   - Invalid email formats
   - Null required fields
   - Invalid timestamp formats
   - Negative prices
2. Process through Spark streaming
3. Check PostgreSQL row counts and Spark logs

**Expected Results:**
- Invalid records are filtered out by Spark validation rules
- Valid records are written successfully
- Spark logs show validation/batch warnings when relevant (e.g., missing columns / duplicates)
- No invalid data persisted to the target table (validated by DB queries)

**Evidence / Verification:**
- Prepare an isolated input directory and table (do not mix with your main pipeline):
   - Set env vars before running Spark:
      - `INPUT_DATA_DIR=data/incoming_validation_test`
      - `CHECKPOINT_DIR=data/checkpoints_validation_test`
      - `DB_TABLE=ecommerce_events_validation_test`
- Create a CSV containing both valid and invalid rows (copy a generated file and edit a few rows):
   - Invalid examples that should be filtered:
      - `price = -10`
      - `customer_email = not-an-email`
      - `event_timestamp = not-a-timestamp`
      - missing `event_id`
- Run the Spark job and then validate the DB only contains valid rows:
   ```sql
   -- Should be 0 if invalid emails were filtered
   SELECT COUNT(*) FROM ecommerce_events_validation_test
   WHERE customer_email NOT LIKE '%@%.%';

   -- Should be 0 if negative/zero prices were filtered
   SELECT COUNT(*) FROM ecommerce_events_validation_test
   WHERE price <= 0;

   -- Should be 0 if timestamps were required
   SELECT COUNT(*) FROM ecommerce_events_validation_test
   WHERE event_timestamp IS NULL;
   ```

**Actual Results:** _TBD (during testing)_
**Status:** ⏳ Pending

---

### Test Case SP-003: High Volume Processing
**Objective:** Test Spark performance under load

**Test Steps:**
1. Generate 1000 CSV files (100MB total)
2. Start Spark streaming with monitoring
3. Process all files and measure performance

**Expected Results:**
- All files processed within 10 minutes
- Throughput > 10,000 records/second
- Memory usage stable throughout test
- Zero data loss or corruption
- Database maintains responsiveness

**Evidence / Verification:**
- Prefer measuring throughput from Spark logs:
   - Look for: `Successfully wrote <n> records in <t>s (<r> records/sec)`
   - Also watch stream progress logs: `Input: <x> rows/sec, Processed: <y> rows/sec`
- Validate DB ingestion count matches generated valid record count:
   ```sql
   SELECT COUNT(*) FROM ecommerce_events_test_<run_id>;
   ```

**Actual Results:** _TBD (during testing)_
**Status:** ⏳ Pending

---

## 3. Database Storage Tests

### Test Case DB-001: Data Insertion and Retrieval
**Objective:** Verify correct data storage in PostgreSQL

**Test Steps:**
1. Insert test batch of 1000 records
2. Query database with various filters
3. Verify data integrity and performance

**Expected Results:**
- All 1000 records inserted successfully
- Query response time < 100ms for indexed columns
- Correct data types preserved
- All indexes functioning properly
- No constraint violations

**Evidence / Verification:**
- Insert via the pipeline (preferred) or via `tests/test_stream_to_postgres.py` and then:
   ```sql
   SELECT COUNT(*) FROM ecommerce_events;

   -- Data type sanity checks
   SELECT
      COUNT(*) FILTER (WHERE price IS NULL) AS null_price,
      COUNT(*) FILTER (WHERE event_timestamp IS NULL) AS null_event_timestamp
   FROM ecommerce_events;

   -- Index usage (example)
   EXPLAIN ANALYZE
   SELECT * FROM ecommerce_events WHERE event_type = 'purchase' ORDER BY event_timestamp DESC LIMIT 100;
   ```

**Actual Results:** _TBD (during testing)_
**Status:** ⏳ Pending

---

### Test Case DB-002: Concurrent Access
**Objective:** Test database under concurrent read/write load

**Test Steps:**
1. Start data ingestion (write load)
2. Execute multiple concurrent analytical queries
3. Monitor database performance

**Expected Results:**
- No lock conflicts or deadlocks
- Write operations complete successfully
- Read queries maintain acceptable response times
- Database connection pool stable
- No data corruption under concurrent access

**Evidence / Verification:**
- While ingestion is running, execute read queries in parallel from another session.
- Check for lock waits/deadlocks (optional):
   ```sql
   SELECT now(), wait_event_type, wait_event, COUNT(*)
   FROM pg_stat_activity
   WHERE datname = current_database()
   GROUP BY 1,2,3
   ORDER BY COUNT(*) DESC;
   ```

**Actual Results:** _TBD (during testing)_
**Status:** ⏳ Pending

---

### Test Case DB-003: Materialized View Refresh
**Objective:** Verify analytical views update correctly

**Test Steps:**
1. Insert batch of new records
2. Refresh materialized view
3. Verify view contains updated aggregations

**Expected Results:**
- Materialized view refresh completes in < 30 seconds
- Aggregated metrics accurate
- No missing data in summary view
- Concurrent refresh operation handling

**Verification Steps / Queries (example):**
```sql
-- If you used the provided sql_setup/postgres_setup.sql
SELECT refresh_ecommerce_analytics();

-- Confirm the view has rows for the last few hours
SELECT *
FROM ecommerce_analytics_summary
ORDER BY hour_bucket DESC
LIMIT 50;
```

**Actual Results:** _TBD (during testing)_
**Status:** ⏳ Pending

---

## 4. End-to-End Integration Tests

### Test Case E2E-001: Complete Data Pipeline
**Objective:** Test entire pipeline from generation to storage

**Test Steps:**
1. Start all services (generator, Spark, PostgreSQL)
2. Run for 10 minutes with default configuration
3. Monitor each component
4. Verify data completeness

**Expected Results:**
- Continuous data flow through entire pipeline
- Data generated → processed → stored without loss
- All services remain stable
- End-to-end latency < 10 seconds
- Data quality metrics within acceptable ranges

**Evidence / Verification:**
- Confirm new CSVs appear in `data/incoming/` while the generator runs.
- Confirm Spark logs show periodic batches and successful writes.
- Confirm DB row count increases over time:
   ```sql
   SELECT COUNT(*) FROM ecommerce_events;
   SELECT MAX(created_at) FROM ecommerce_events;
   ```

**Actual Results:** _TBD (during testing)_
**Status:** ⏳ Pending

---

### Test Case E2E-002: Failure Recovery
**Objective:** Test system recovery from component failures

**Test Steps:**
1. Start normal operations
2. Simulate failures:
   - Stop Spark streaming for 2 minutes
   - Restart PostgreSQL
   - Fill disk space temporarily
3. Verify recovery behavior

**Expected Results:**
- System recovers automatically when services restart
- No data loss during outages
- Backpressure handling prevents memory issues
- Error logging captures failure details
- Operations resume normally after recovery

**Evidence / Verification:**
- Capture timestamps for when you stop/start each component.
- Confirm Spark resumes processing after restart by checking:
   - Log file: `logs/spark_streaming_YYYYMMDD.log`
   - Messages indicating query start and subsequent successful batch writes.
- Validate DB row counts resume increasing after recovery:
   ```sql
   SELECT COUNT(*) FROM ecommerce_events;
   ```

**Actual Results:** _TBD (during testing)_
**Status:** ⏳ Pending

---

## 5. Performance Benchmark Tests

### Test Case PERF-001: Throughput Baseline
**Objective:** Establish performance baseline

**Test Configuration:**
- Data generation: 1000 records/second
- Spark: 2 workers, 4GB RAM each
- PostgreSQL: Default configuration

**Measurements:**
1. Data generation rate
2. Spark processing throughput
3. Database insertion rate
4. End-to-end latency
5. Resource utilization

**Expected Performance Targets:**
- Generation Rate: 1000 events/second
- Processing Rate: > 5000 events/second
- Database Rate: > 2000 inserts/second
- End-to-End Latency: < 5 seconds (95th percentile)
- CPU Usage: < 70% average
- Memory Usage: < 80% of allocated

**Evidence / Verification:**
- Capture Spark throughput from logs:
   - `Successfully wrote <n> records in <t>s (<r> records/sec)`
   - Stream progress: `Input: <x> rows/sec, Processed: <y> rows/sec`
- Compute DB insert rate by sampling counts over time:
   ```sql
   -- Run twice ~30s apart and compute delta/time
   SELECT now() AS ts, COUNT(*) AS rows FROM ecommerce_events;
   ```

**Actual Results:** _TBD (during testing)_
**Status:** ⏳ Pending

---

### Test Case PERF-002: Scale Testing
**Objective:** Test scalability limits

**Test Steps:**
1. Gradually increase data generation rate
2. Monitor system performance at each level
3. Identify bottlenecks and breaking points

**Test Levels:**
- Level 1: 500 events/second
- Level 2: 1000 events/second
- Level 3: 2000 events/second
- Level 4: 5000 events/second
- Level 5: 10000 events/second

**Expected Results:**
- System maintains stability up to Level 4
- Graceful degradation at higher loads
- Clear bottleneck identification
- No data loss under stress

**Evidence / Verification:**
- For each level, record:
   - Spark `processedRowsPerSecond`
   - DB insert rate (count delta / time)
   - Peak CPU/RAM
   - Any Spark retries or failed batches

   **Actual Results:** _TBD (during testing)_
   **Status:** ⏳ Pending

---

## 6. Data Quality Tests

### Test Case DQ-001: Duplicate Detection
**Objective:** Verify system handles duplicate events

**Test Steps:**
1. Generate dataset with known duplicates (same event_id)
2. Process through pipeline
3. Verify duplicate handling

**Expected Results:**
- Duplicates are detected and logged (Spark logs a warning with duplicate counts)
- Database prevents duplicates because `event_id` is a primary key in the Spark-created table
- A batch containing duplicate `event_id` values may fail to write (constraint violation) and be retried; failures should be visible in Spark logs

**Evidence / Verification:**
- Create a small CSV where 2+ rows share the same `event_id`.
- Run ingestion into an isolated table and verify:
   - Spark logs contain `Found <n> duplicate event_ids` for the batch.
   - PostgreSQL contains at most one row for that `event_id`:
      ```sql
      SELECT event_id, COUNT(*)
      FROM ecommerce_events_validation_test
      GROUP BY event_id
      HAVING COUNT(*) > 1;
      ```

   **Actual Results:** _TBD (during testing)_
**Status:** ⏳ Pending

---

### Test Case DQ-002: Data Completeness
**Objective:** Ensure no data loss during processing

**Test Steps:**
1. Generate exactly 10000 test records
2. Process through complete pipeline
3. Count records at each stage

**Expected Results:**
- Generated: 10000 records
- Spark Processed: 10000 records
- Database Stored: 10000 valid records
- Zero data loss percentage

**Evidence / Verification:**
- Generate deterministic valid input:
   ```bash
   python tests/generate_test_data.py --count 10000 --batch-size 500 --out-dir data/incoming --seed 42
   ```
- Ingest into an isolated table and verify DB count equals 10000:
   ```sql
   SELECT COUNT(*) FROM ecommerce_events_test_<run_id>;
   ```
- If DB count is lower, check Spark logs for filtered rows (validation filters) or failed batches (JDBC retries exhausted).

**Actual Results:** _TBD (during testing)_
**Status:** ⏳ Pending

---

## Test Execution Summary

### Test Results Overview
| Component | Total Tests | Passed | Failed | Pending |
|-----------|-------------|---------|---------|---------|
| Data Generation | 3 | 0 | 0 | 3 |
| Spark Processing | 3 | 0 | 0 | 3 |
| Database | 3 | 0 | 0 | 3 |
| End-to-End | 2 | 0 | 0 | 2 |
| Performance | 2 | 0 | 0 | 2 |
| Data Quality | 2 | 0 | 0 | 2 |
| **Total** | **15** | **0** | **0** | **15** |

### Critical Issues Found
Use this section to track blockers and defects found during execution.

| Issue ID | Severity | Component | Summary | Repro Steps | Current Status | Owner | ETA |
|---------:|----------|-----------|---------|------------|----------------|-------|-----|
| CI-001 | (Critical/High/Medium/Low) | (DG/SP/DB/E2E/PERF/DQ) | (short) | (steps) | (Open/Investigating/Fixed/Deferred) | (name) | (date) |

### Performance Metrics Summary
Capture baseline numbers here so future changes can be compared apples-to-apples.

| Metric | Target | Observed | Unit | Notes |
|--------|--------|----------|------|------|
| Generation rate | 1000 |  | events/sec | 
| Spark throughput | > 5000 |  | events/sec | 
| DB insert rate | > 2000 |  | rows/sec | 
| End-to-end latency (p95) | < 5 |  | sec | 
| CPU avg | < 70 |  | % | 
| Memory avg | < 80 |  | % | 

### Recommendations
Record concrete follow-ups based on failures, bottlenecks, or risk.

- **Reliability:** Ensure Spark checkpoints are on persistent storage (especially when running in Docker) and that `CHECKPOINT_DIR` is not shared across unrelated runs.
- **Data correctness:** Add/verify idempotency for `event_id` (unique constraint or de-dupe logic) to prevent duplicates on retries.
- **Observability:** Standardize log locations and include run IDs in logs; persist logs as artifacts for troubleshooting.
- **Performance:** Validate that PostgreSQL indexes match your query patterns; consider batching/JDBC tuning if inserts become a bottleneck.
- **Schema alignment:** Keep PostgreSQL schema/table definitions consistent between `sql_setup/postgres_setup.sql` and the Spark job’s auto-created table.

---

## Test Automation Scripts

### Running All Tests
```bash
# 1) Create/Reset the test environment (directories + any local prerequisites)
python tests/setup_test_environment.py

# 2) Generate controlled CSV test data
python tests/generate_test_data.py --count 1000 --batch-size 100 --categories all --out-dir data/incoming

# 3) Integration smoke test: Spark stream -> PostgreSQL (creates an isolated table per run)
# Required env vars: POSTGRES_PASSWORD (and optionally POSTGRES_HOST/PORT/DB/USER)
python tests/test_stream_to_postgres.py --count 50 --timeout-seconds 90
```

### Manual Test Verification
```bash
# If you have psql available, this is the quickest way to sanity-check ingestion
# (update connection parameters as needed)
psql -h localhost -U postgres -d ecommerce_analytics -c "SELECT COUNT(*) FROM ecommerce_events;"
```

This test plan provides comprehensive coverage of all system components and should be executed before any production deployment or major releases.