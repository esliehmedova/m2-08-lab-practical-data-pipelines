## Task 1: Pipeline Architecture Diagram (~25 minutes) 

### 1.1 — Draw the end-to-end architecture 

┌─────────────────────┐         ┌─────────────────────────┐
│   DATA SOURCE 1     │         │     DATA SOURCE 2        │
│  Online Retail.xlsx │         │  Live Transaction Stream │
│  (historical batch) │         │  (row-by-row events)     │
└────────┬────────────┘         └────────────┬─────────────┘
         │  read once / backfill              │  continuous / simulated
         │  Excel → Parquet                   │  JSON records
         └─────────────────┬──────────────────┘
                           ▼
         ┌─────────────────────────────────────┐
         │           INGESTION LAYER            │
         │                                      │
         │  • Attach metadata to every record:  │
         │    _source: batch | stream           │
         │    _ingested_at: timestamp           │
         │  • Unified schema enforced on entry  │
         │  • No transformation at this stage   │
         └─────────────────┬───────────────────┘
                           │
                           ▼
         ┌─────────────────────────────────────┐
         │           RAW LAYER                  │
         │         (Landing Zone)               │
         │                                      │
         │  Format  : Parquet                   │
         │  Partition: YearMonth                │
         │  Mode    : Append-only, immutable    │
         │  Purpose : Full audit trail,         │
         │            reprocessing source       │
         └─────────────────┬───────────────────┘
                           │
                           ▼
         ┌─────────────────────────────────────┐
         │          VALIDATION STAGE            │
         │                                      │
         │  Schema Checks:                      │
         │  • Required fields present           │
         │  • Correct data types                │
         │  • No nulls in critical columns      │
         │                                      │
         │  Business Rule Checks:               │
         │  • Quantity > 0                      │
         │  • UnitPrice ≥ 0                     │
         │  • InvoiceNo starts with C →         │
         │    route to returns (not rejected)   │
         │  • CustomerID null → allowed         │
         │    (guest order, flagged)            │
         └────────┬─────────────────┬───────────┘
                  │                 │
          PASSED  │                 │  FAILED
                  │                 ▼
                  │    ┌────────────────────────┐
                  │    │     QUARANTINE ZONE     │
                  │    │    (Dead Letter Store)  │
                  │    │                         │
                  │    │  Format  : Parquet      │
                  │    │  Contains: raw record + │
                  │    │  _error_reason field    │
                  │    │  Never silently dropped │
                  │    │  Reprocessable anytime  │
                  │    └────────────────────────┘
                  │
                  ▼
         ┌─────────────────────────────────────┐
         │        TRANSFORMATION STAGE          │
         │                                      │
         │  Clean Transforms:                   │
         │  • Parse InvoiceDate → datetime      │
         │  • Compute Revenue = Qty × Price     │
         │  • Derive YearMonth period column    │
         │  • Standardize country / text fields │
         │                                      │
         │  Feature Engineering (per customer): │
         │  • Recency  — days since last order  │
         │  • Frequency — unique invoice count  │
         │  • Monetary  — total revenue         │
         │  • Anomaly flag — Monetary > μ + 3σ  │
         │  • Exclude null CustomerID (guests)  │
         └────────┬──────────────┬──────────────┘
                  │              │
                  ▼              ▼
   ┌──────────────────┐   ┌──────────────────────┐
   │   CLEAN LAYER    │   │    FEATURE LAYER      │
   │                  │   │                       │
   │ Format: Parquet  │   │ Format : Parquet       │
   │ Partition:       │   │ Granularity:           │
   │   YearMonth      │   │   One row/CustomerID  │
   │ Contains:        │   │ Contains:              │
   │  validated +     │   │   RFM columns +        │
   │  enriched rows   │   │   anomaly flag        │
   │ Includes guest   │   │ Excludes guest orders │
   │  orders          │   │                       │
   └───────┬──────────┘   └──────────┬────────────┘
           │                         │
           ▼                         ▼
   ┌───────────────┐        ┌─────────────────────┐
   │  BI DASHBOARD │        │      ML MODEL        │
   │               │        │                      │
   │ Reads: clean/ │        │ Reads: feature/      │
   │ Cadence:      │        │ Features: daily      │
   │   nightly     │        │ Retraining: weekly   │
   │ Queries:      │        │ Target:              │
   │  daily sales, │        │  high-value customer │
   │  top products,│        │  classification      │
   │  country view │        │                      │
   └───────────────┘        └─────────────────────┘

              ▲                        ▲
              └──────────┬─────────────┘
                         │
         ┌───────────────────────────────────────┐
         │           MONITORING LAYER             │
         │         (runs post-transform)          │
         │                                        │
         │  Metrics tracked per pipeline run:     │
         │  • Rejection rate        (alert > 5%)  │
         │  • Null CustomerID rate  (alert > 20%) │
         │  • Data freshness        (alert > 25h) │
         │  • Revenue Δ DoD         (alert > ±30%)│
         │  • Row volume Δ          (spike/drop)  │
         │                                        │
         │  Output: structured run log            │
         │  → alerts if thresholds breached       │
         └───────────────────────────────────────┘

### 1.2 — Describe each component

#### 1. Data Source 1 — Batch File (Online Retail.xlsx)

This component is the historical dataset stored in an Excel file. It contains past transaction records such as invoices, products, quantities, prices, and customer IDs. The input is the Excel file, and the output is the same data converted into Parquet format for efficient storage and processing. 

#### 2. Data Source 2 — Live Transaction Stream

This component simulates a live stream of new transactions coming into the system one by one. Each event represents a new purchase happening in real time. The input is JSON transaction records, and the output is a continuous flow of records sent to the ingestion layer. 

#### 3. Ingestion Layer

The ingestion layer collects data from both the batch source and the streaming source. It attaches metadata to each record, such as the source type (batch or stream) and the ingestion timestamp. It also ensures that all incoming records follow the same schema. The input is Excel-converted records and JSON stream events, and the output is structured records ready for storage, typically written in Parquet format.

#### 4. Raw Layer (Landing Zone)

The raw layer stores the original ingested data exactly as it arrived, without modification. Its main purpose is to keep a full history of the data for auditing and reprocessing if needed. The input is structured records from the ingestion layer, and the output is append-only Parquet files partitioned by YearMonth. Technologies like data lakes or object storage (e.g., S3 or local data lake) are typically used.

#### 5. Validation Stage

This component checks whether the data is correct and usable. It verifies schema rules (correct data types, required fields) and business rules (e.g., quantity must be positive, price cannot be negative). The input is raw transaction records, and the output is either validated records or rejected records with error information. These checks can be implemented using Python validation scripts or frameworks like Great Expectations. 

#### 6. Quarantine Zone (Dead Letter Store)

The quarantine zone stores records that fail validation checks. Instead of deleting bad data, the pipeline keeps these records so they can be inspected and fixed later. The input is invalid records from the validation stage, and the output is Parquet files containing the original record and an _error_reason field.

#### 7. Transformation Stage

This component cleans and enriches the validated data. It converts dates into proper datetime format, calculates revenue (Quantity × UnitPrice), and creates new fields such as YearMonth. It also performs feature engineering like calculating Recency, Frequency, and Monetary values for each customer. The input is validated transaction records, and the output is cleaned transaction data and customer-level features, usually processed using Python with Pandas or Spark.

#### 8. Clean Layer

The clean layer stores processed transaction data that has already passed validation and transformation. This data is structured, standardized, and ready for analytics. The input is cleaned records from the transformation stage, and the output is Parquet files partitioned by YearMonth containing enriched transaction rows. This layer is optimized for analytical queries and reporting. 

#### 9. Feature Layer

The feature layer contains aggregated customer-level features used for machine learning models. Each row represents one customer and includes metrics like recency, frequency, and monetary value. The input is transformed transaction data, and the output is a feature dataset stored in Parquet format with one row per customer. This dataset is used directly by ML training pipelines.

#### 10. BI Dashboard (Consumer)

The BI dashboard provides business insights based on the cleaned transaction data. It reads data from the clean layer and generates reports such as daily sales, top products, and sales by country. The input is clean Parquet datasets, and the output is visual dashboards and reports. Tools like Power BI, Tableau, or Metabase could be used here.

#### 11. ML Model (Consumer)

The ML model uses customer features to predict high-value customers. It reads data from the feature layer and trains a classification model on a regular schedule. The input is customer feature datasets, and the output is predictions or customer scores. Technologies like Python, scikit-learn, or a scheduled training pipeline could be used.

#### 12. Monitoring Layer

The monitoring layer tracks the health and quality of the data pipeline. It measures metrics such as rejection rate, missing customer IDs, data freshness, and unusual revenue changes. The input is pipeline logs and processed datasets, and the output is alerts or monitoring reports when thresholds are exceeded. Tools such as Airflow monitoring, Prometheus, or logging systems can be used to implement this.  

## Task 2: Validation and Error Handling Design (~20 minutes) 

### 2.1 — Define validation rules

**Schema validations** (structural correctness):
- List every field, its expected type, and whether it is required
- Example: "`InvoiceNo` must be a non-empty string"

┌──────────────┬───────────────┬──────────────┐
│    Field     │ Expected Type │   Required   │                               
├──────────────┼───────────────┼──────────────┼
│  InvoiceNo   │    String     │      Yes     │       
│              │               │              │    
├──────────────┼───────────────┼──────────────┼
│  StockCode   │    String     │      Yes     │  
│              │               │              │  
├──────────────┼───────────────┼──────────────┼
│ Description  │    String     │      Soft    │
│              │               │              │
├──────────────┼───────────────┼──────────────┼
│  Quantity    │    Integer    │      Yes     │
│              │               │              │
├──────────────┼───────────────┼──────────────┼
│ InvoiceDate  │   Datetime    │      Yes     │
│              │               │              │
├──────────────┼───────────────┼──────────────┼
│  UnitPrice   │     Float     │      Yes     │
│              │               │              │
├──────────────┼───────────────┼──────────────┼
│  CustomerID  │    String     │      Soft    │
│              │               │              │
├──────────────┼───────────────┼──────────────┼
│   Country    │    String     │      Yes     │
│              │               │              │
└──────────────┴───────────────┴──────────────┴

Yes  = hard requirement — record rejected if violated
Soft = warning only — record flagged but passed through

**Value range validations** (sensible values):
- Define acceptable ranges for numeric fields
- Example: "`UnitPrice` must be > 0 for non-cancellation records"

Quantity ≠ 0
Quantity > 0 for normal ones
Quantity < 0 for cancellation invoices 

UnitPrice >= 0 for normal ones
UniPrice <= 0 for rejected ones

**Business rule validations** (domain logic):
- Define cross-field consistency checks
- Example: "If `InvoiceNo` starts with 'C', then `Quantity` must be negative" 

InvoiceNo starts with 'C' Quantity must be negative
InvoiceNo does not start with 'C' Quantity must be positive

Cross-field Consistency

If CustomerID is null and Country is null 
If Description is null but StockCode is valid → allow, flag as "MISSING_DESCRIPTION"
If InvoiceDate is more than 365 days older than the most recent date - flag as date outlier
A record is a duplicate if InvoiceNo + StockCode + Quantity + InvoiceDate are all identical to an existing clean layer record 

### 2.2 — Design the error handling flow

For each validation category, specify what happens when a record fails:

Category1 - Schema Validation Failures

What happens to the record:
The record is rejected entirely. A record with a missing or unparseable critical field cannot be processed downstream, so partial acceptance is not allowed.
Where it goes:
Rejected records are written to a quarantine table. Each record keeps all its original fields intact, plus three added fields: what the error was, which category it belongs to, and when it failed. Soft failures like a missing Description or null CustomerID are allowed through to the clean layer with a warning flag — they are not quarantined.
Operator alert:
If the schema failure rate exceeds 5% in a single run, the pipeline is marked as degraded and an alert is raised. A sudden spike above 30% is treated as critical and suggests the upstream source has changed its structure.
Recovery:
The operator inspects the quarantine table, identifies the root cause, fixes the source file or stream producer, then re-runs the pipeline from the raw layer. The raw layer never needs to be re-ingested unless it is proven corrupted.

Category 2 — Value Range Validation Failures

What happens to the record:
Two outcomes depending on severity. Records with zero price on a normal invoice, zero quantity, or a future date are rejected to quarantine. Records with unusually large revenue or quantity are accepted into the clean layer but marked with an anomaly flag — they are valid records that need review, not broken ones.
Where it goes:
Hard failures go to the quarantine table with the specific error reason attached. Anomaly-flagged records go to the clean layer with an extra column indicating what triggered the flag.
Operator alert:
If more than 1% of records have zero price on normal invoices, a pricing data alert is raised. Every record flagged as a revenue anomaly is individually logged in the run summary regardless of volume.
Recovery:
The operator checks whether the zero price or out-of-range value was a genuine business case or a feed error. If it was a feed error, the source is corrected and the affected partition is reprocessed from the raw layer. If it was genuinely valid, the business rules are updated to allow it and the pipeline is re-run.

Category 3 — Business Rule Validation Failures

What happens to the record:
Most business rule failures are hard rejections. However, two cases are routed rather than rejected — valid cancellation invoices go to a separate returns table, and known special stock codes go to the clean layer with a flag.
Where it goes:
Hard failures go to the quarantine table with the specific rule violation attached. Valid cancellations go to the returns table. Special stock code records go to the clean layer marked as non-product lines.
Operator alert:
A duplicate rate above 2% triggers an alert suggesting the stream was replayed or the batch was loaded twice. More than 10 unknown stock codes in a single run triggers an alert to update the reference list. Cancellation routing failures above 0.5% trigger a logic error alert.
Recovery:
For sign mismatches, the operator confirms with the source system whether the quantity sign or the cancellation flag is wrong. For unknown stock codes, new legitimate codes are added to the reference list and the pipeline is reprocessed. For duplicates, the double-ingestion is traced and removed at source before reprocessing.

## Task 3: Transformation and Storage Design (~20 minutes)

### 3.1 — Define transformations

Group 1 — Cleaning Operations

Trim and Standardize Text Fields

Input - Raw string values in Description, Country, StockCode
Output - Lowercase, trimmed strings with no extra whitespace
Idempotent? - Yes — applying trim and lowercase repeatedly always produces the same result

Impute Missing Descriptions

Input - Records where Description is null but StockCode is valid
Output - Description filled from a lookup table mapping each StockCode to its most frequent description
Idempotent? - Yes — the lookup table is static, so re-running always produces the same imputed value

Standardize Country Names

Input - Raw Country string (e.g. "EIRE", "Channel Islands")
Output - Normalized country name mapped to an ISO standard reference list
Idempotent? - Yes — deterministic mapping table, safe to re-run

Parse and Cast InvoiceDate

Input - InvoiceDate as a raw string or mixed-type column
Output - Proper datetime object, UTC-normalized
Idempotent? - Yes — parsing the same string always produces the same datetime

Normalize CustomerID Format

Input - CustomerID arriving as a float from Excel (e.g. 12345.0) or inconsistent string
Output - Zero-padded 5-character string (e.g. "12345"), or null for guest orders
Idempotent? - Yes — formatting to a fixed-width string is deterministic

Group 2 — Derived Columns 

Compute Revenue

Input - Quantity (integer) and UnitPrice (float)
Output - New column: Revenue = Quantity × UnitPrice
Idempotent? - Yes — pure arithmetic on fixed columns, always produces the same result

Cancellation Flag

Input - InvoiceNo string
Output - Boolean column: is_cancellation = True if InvoiceNo starts with 'C', else False
Idempotent? - Yes — deterministic string check

Extract Date Components

Input - Parsed InvoiceDate datetime column
Output - New columns: Year, Month, DayOfWeek, Hour, YearMonth period
Idempotent? - Yes — extracting components from a fixed datetime always returns the same values

Special StockCode Flag

Input - StockCode string
Output - Boolean column: is_non_product = True for known non-product codes (POST, D, M, DOT, BANK CHARGES, AMAZONFEE)
Idempotent? - Yes — static reference list lookup, deterministic

Guest Order Flag

Input - CustomerID (nullable)
Output - Boolean column: is_guest = True where CustomerID is null
Idempotent? - Yes — null check on a fixed column

Group 3 — Customer-Level Aggregations

Total Revenue per Customer

Input - Clean layer records grouped by CustomerID, Revenue column
Output - total_revenue — sum of all Revenue values per customer
Idempotent? - Yes — as long as the clean layer partition is overwritten, never appended to

Order Count per Customer - 

Input - Clean layer records grouped by CustomerID, InvoiceNo column
Output - order_count — number of distinct invoices per customer
Idempotent? - Yes — distinct count over a fixed dataset is deterministic

Product Diversity per Customer

Input - Clean layer records grouped by CustomerID, StockCode column
Output - unique_products — count of distinct StockCodes ever purchased per customer
Idempotent? - Yes — distinct count is deterministic over fixed data

Recency

Input - Clean layer grouped by CustomerID, InvoiceDate, and a snapshot date
Output - recency_days — number of days between the customer's most recent invoice and the snapshot date
Idempotent? - No — re-running on a different day produces a different value. Fix: pin the snapshot date to the maximum InvoiceDate in the clean layer, not the system clock

Average Order Value

Input - total_revenue (T11) and order_count (T12) per customer
Output - avg_order_value = total_revenue ÷ order_count
Idempotent? - Yes — derived from two idempotent aggregations

Customer Lifespan

Input - Clean layer grouped by CustomerID, InvoiceDate
Output - lifespan_days — days between the customer's first and most recent invoice
Idempotent? - Yes — min and max of a fixed datetime column is deterministic

Group 4 — ML Feature Engineering

RFM Score Normalization
 
Input - recency_days, order_count, total_revenue per customer within the observation window
Output - recency_score, frequency_score, monetary_score — each scaled to a 0–1 range
Idempotent? - No — min-max scaling shifts when new customers are added. Fix: fit the scaler once on training data, save the min/max parameters, and reapply them as a static transform on all future runs

Purchase Rate

Input - order_count (T12) and lifespan_days (T16) within the observation window
Output - purchase_rate = order_count ÷ lifespan_days (orders per day)
Idempotent? - Yes — deterministic division of two fixed values. Edge case: if lifespan_days = 0 (one-time buyer on a single day), set purchase_rate equal to order_count to avoid division by zero

Country Encoding
 
Input - Country string — the most frequent country across a customer's orders
Output - country_encoded — integer or one-hot encoded country column
Idempotent? - No — the encoding map changes if new countries appear. Fix: save the encoding dictionary at training time and reapply it statically; never refit on new data

High-Value Label (Target Variable)

Input - Total revenue per customer computed over the label window only (data after the cutoff date)
Output - Binary column: is_high_value = 1 if the customer's label-window revenue falls in the top 20%, else 0
Idempotent? - Yes — the 20% threshold is computed once at training time and stored as a fixed constant for all future runs

Anomaly Flag

Input - total_revenue per customer, population mean and standard deviation from the training set
Output - Boolean anomaly_flag = True where total_revenue exceeds mean + 3 standard deviations
Idempotent? - No — mean and standard deviation shift as new customers are added. Fix: compute both values once on training data, store them as pipeline constants, and apply them statically on all future runs 

### 3.2 — Design the storage layers

Layer Definitions
┌─────────────┬──────────────────┬───────────────┬──────────────────────────────────────────┐
│    Layer    │     Contents     │    Format     │        Update Freq / Retention           │
├─────────────┼──────────────────┼───────────────┼──────────────────────────────────────────┤
│             │ Exact copy of    │               │ Append-only. Batch: once at backfill.    │
│             │ every ingested   │ Parquet       │ Stream: continuously as events arrive.   │
│     Raw     │ record. Nothing  │ partitioned   │                                          │
│             │ modified or      │ by YearMonth  │ Retention: Indefinite. Never deleted.    │
│             │ filtered.        │               │ Serves as reprocessing source.           │
├─────────────┼──────────────────┼───────────────┼──────────────────────────────────────────┤
│             │ Validated,       │               │ Nightly refresh of current month.        │
│             │ enriched         │ Parquet       │ Historical partitions immutable          │
│    Clean    │ records with     │ partitioned   │ once closed.                             │
│             │ derived columns. │ by YearMonth  │                                          │
│             │ Guest orders     │               │ Retention: Minimum 3 years.              │
│             │ included.        │               │                                          │
├─────────────┼──────────────────┼───────────────┼──────────────────────────────────────────┤
│             │ One row per      │               │ Daily full rewrite per snapshot.         │
│             │ CustomerID.      │ Parquet       │                                          │
│   Feature   │ RFM values,      │ partitioned   │ Retention: Rolling 12 months of          │
│             │ anomaly flag,    │ by snapshot   │ daily snapshots retained for             │
│             │ ML-ready         │ date          │ model reproducibility.                   │
│             │ features.        │               │                                          │
└─────────────┴──────────────────┴───────────────┴──────────────────────────────────────────┘

Format Justification

Raw Layer — Parquet, partitioned by YearMonth

The raw layer is written once and read rarely — only when reprocessing is needed. Parquet is ideal here because it preserves exact data types, compresses well for large historical datasets, and can be read back efficiently by partition without loading the entire history. CSV would be risky at this layer because type information is lost on write, which could introduce subtle errors during reprocessing. The raw layer will grow continuously and unboundedly, so compression matters — Parquet typically achieves 5–10× compression over CSV on this kind of transactional data.

Clean Layer — Parquet, partitioned by YearMonth

The BI dashboard reads this layer nightly, querying specific date ranges — monthly sales, year-over-year comparisons, country breakdowns. Partitioning by YearMonth means the query engine only reads the relevant partitions rather than scanning the full history, which keeps dashboard refresh times fast even as the dataset grows to millions of rows. Parquet's columnar format also means that a query selecting only Revenue and Country never reads the Quantity or Description columns, which further reduces I/O. This layer needs to support at least 3 years of history for meaningful trend analysis, so size efficiency is important.

Feature Layer — Parquet, partitioned by snapshot date

The ML model reads this layer as a flat table — one row per customer, all features in one scan. There are no complex date-range queries here, so heavy partitioning is unnecessary. However, retaining one snapshot per day for 12 months is critical: it allows the ML team to reconstruct exactly what the feature table looked like on any past date, which is essential for debugging model performance, investigating prediction drift, and reproducing past training runs. Each daily snapshot is a full rewrite rather than an append, so the total size stays manageable — roughly 365 copies of the customer feature table, which for a retailer of this scale would be well under a few gigabytes total.

### 3.3 — Design incremental updates

Tracking What Has Already Been Processed

The pipeline saves the timestamp of the most recent InvoiceDate after every successful run — this is the high-water mark. On the next run, only records with an InvoiceDate beyond that timestamp are processed. If a run fails mid-way, the high-water mark is not updated, so the next run safely reprocesses the same window without skipping or double-counting.

Handling Late-Arriving Records

Any record whose InvoiceDate falls more than 7 days behind the current high-water mark is flagged as a late arrival. The pipeline reopens the correct historical partition, merges the record in, and rewrites that partition to keep it historically accurate. Records arriving more than 90 days late are sent to a separate late arrivals table for analyst review instead of being merged into old partitions.

Refreshing Customer-Level Features

Each night, only customers with new transactions since the last high-water mark are recalculated — all others keep their existing feature row unchanged. Once a week, the entire feature table is fully rewritten from scratch to correct any drift from incremental updates or late arrivals. If a historical partition is rewritten for any reason, all affected customers are immediately flagged for recalculation outside the normal schedule.
