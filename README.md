# Data Management and Manipulation at Scale - Assignment One

**Aman Verma** — **Student ID: 529549**  
**Teammate Name:** ____________ — **Student ID:** ____________  
**Teammate Name:** ____________ — **Student ID:** ____________  

## Short update (Aman)
I validated our implementation end-to-end, fixed a few correctness edge cases in the partitioned warehouse (so inserts are always queryable and partition pruning works correctly), and updated this README to reflect those changes.

## Implementation Summary
Our implementation stores data in fixed-size partitions, each saved as a Parquet file. Incoming rows are buffered in memory and flushed to disk once the buffer reaches `partition_size`. Partitions can be rewritten when updates or deletes occur.

To reduce full scans during queries, updates, and deletes, the warehouse maintains per-partition metadata in memory. For each column in a partition, metadata stores the minimum value, maximum value, and row count. This enables partition pruning: partitions whose min/max ranges cannot contain the requested key(s) are skipped.

## Key Design Decisions
1. **Parquet (columnar storage):** Better compression and faster column scans than CSV.
2. **Fixed row-count partitions:** Predictable partition sizes and simpler file management.
3. **Buffered writes:** Reduces many small writes and improves insert throughput.
4. **Partition metadata pruning:** Uses min/max to skip irrelevant partitions (Snowflake-style idea).
5. **Vectorized execution with PyArrow:** Uses Arrow compute instead of Python loops for filters/updates.

## Notes / Observations
- **Storage:** Parquet is significantly smaller than CSV for the same dataset (depends on data).
- **Performance:** In our harness, MyDataWarehouse is faster for inserts/updates/deletes, while queries may be slower than the naive CSV version because the naive implementation keeps all rows in memory, but MyDataWarehouse reads Parquet partitions from disk.
