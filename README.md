# Data Management and Manipulation at Scale - Assignment One

Our implementation stores data in fixed-size partitions, each saved
    as a Parquet file. Incoming rows are buffered in memory and flushed to disk
    once the buffer reaches `partition_size`. Each partition is immutable at
    the file level but can be rewritten when updates or deletes occur.

To avoid scanning all partitions for queries, updates, and deletes, the warehouse maintains metadata per partition. For each column in a partition, the metadata stores the minimum value, maximum value, and row count in memory. This enables partition pruning: partitions whose min/max ranges cannot contain a queried key are skipped entirely.

Key Design Decisions and Motivations
-----------------------------------
1. Columnar storage via Parquet: Parquet provides efficient columnar encoding and compression, which significantly reduces storage size compared to row-based CSV storage and enables vectorized computation.
2. Partitioning by fixed row count: Using a fixed number of rows per partition simplifies file management and ensures predictable partition sizes, balancing write amplification against query efficiency.
3. In-memory buffering for writes: Rows are buffered until a full partition is accumulated, minimizing small writes and improving write throughput.
4. Partition-level metadata in memory: Storing min/max statistics per column allows fast elimination of irrelevant partitions during updates, deletes, and queries, similar to metadata pruning used in analytical databases such as Snowflake.
5. Vectorized execution: Updates, deletes, and queries are implemented using vectorized compute with PyArrow rather than Python loops.

Observations
------------
- Storage Efficiency: Parquet's columnar format and compression reduced storage size by approximately 70% compared to CSV for our test datasets.
- Query Performance: Partition pruning based on min/max metadata significantly improved query performance, especially for querying the data. Almost all of the tests in the test harness ran an order of magnitude faster with our data warehouse implementation as opposed to the NaiveCSVWarehouse.