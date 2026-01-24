from typing import Any, List, Dict
from data_warehouse import DataWarehouse
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import shutil
from collections import OrderedDict  # ✅ ADDED (for LRU cache)


class MyDataWarehouse(DataWarehouse):

    def __init__(self, partition_size, storage_dir) -> None:
        """
        Delete existing storage directory and initialize a new data warehouse
        Create metadata dict to track min, max, num_rows per column per partition
        """
        self.partition_size = partition_size
        self.storage_dir = Path(storage_dir)

        if self.storage_dir.exists():
            shutil.rmtree(self.storage_dir)

        self.storage_dir.mkdir(parents=True, exist_ok=True)

        self._buffer = []
        self._buffer_rows = 0
        self.metadata = {}
        self.partitions = sorted(self.storage_dir.glob("part_*.parquet"))

        # ✅ ADDED: simple LRU cache for partition reads (Path -> pa.Table)
        self._cache = OrderedDict()
        self._cache_capacity = 3  # keep last N partitions in memory (tweakable)

    def _next_partition_path(self) -> Path:
        idx = len(self.partitions)
        return self.storage_dir / f"part_{idx:05d}.parquet"

    def _read_partition(self, path: Path) -> pa.Table:
        # ✅ ADDED: LRU cache
        if path in self._cache:
            table = self._cache.pop(path)
            self._cache[path] = table  # move to most-recent
            return table

        table = pq.read_table(path)
        self._cache[path] = table
        while len(self._cache) > self._cache_capacity:
            self._cache.popitem(last=False)
        return table

    def _update_metadata(self, table: pa.Table, path: Path) -> None:
        """
        Update metadata for a given partition by column finding min, max, and number of rows like Snowflake

        ✅ CHANGED:
        - For 'id', compute min/max using 8-digit zero-padded strings so lexicographic order matches numeric order.
          (ids arrive as strings)
        """
        partition_meta = {}
        for col in table.schema.names:
            col_data = table[col]

            # ✅ ADDED/CHANGED: special handling for id min/max via zfill(8)
            if col == "id":
                # Convert column to Python list, pad each to 8 chars
                padded = [str(x).zfill(8) for x in col_data.to_pylist()]
                if padded:
                    partition_meta[col] = {
                        "min": min(padded),
                        "max": max(padded),
                        "num_rows": table.num_rows
                    }
                else:
                    partition_meta[col] = {"min": None, "max": None, "num_rows": 0}
            else:
                min_max = pc.min_max(col_data)
                partition_meta[col] = {
                    "min": min_max["min"],
                    "max": min_max["max"],
                    "num_rows": table.num_rows
                }

        self.metadata[path] = partition_meta

    def _write_partition(self, table: pa.Table, path: Path) -> None:
        pq.write_table(table, path)
        self._update_metadata(table, path)

        # ✅ ADDED: keep cache consistent after writes
        if path in self._cache:
            self._cache.pop(path)
        self._cache[path] = table
        while len(self._cache) > self._cache_capacity:
            self._cache.popitem(last=False)

    def _trim_partitions(self, key_column: str, keys: Any) -> List[Path]:
        """
        Iterate over partitions and use metadata to filter out partitions that cannot contain the key(s) by min/max.

        ✅ CHANGED:
        - Supports both a single key (update/delete) and a list of keys (query).
        - For 'id', uses 8-digit padded comparisons for correct pruning.
        - Uses overlap test: key-range overlaps partition-range.
        """
        # ✅ ADDED: normalize keys into a list of strings
        if isinstance(keys, (list, tuple)):
            key_list = [str(k) for k in keys]
        else:
            key_list = [str(keys)]

        # ✅ ADDED: compute key_min/key_max (for pruning) with padding for id
        if key_column == "id":
            padded_keys = [k.zfill(8) for k in key_list]
            key_min = min(padded_keys)
            key_max = max(padded_keys)
        else:
            key_min = min(key_list)
            key_max = max(key_list)

        valid_partitions = []

        for path in self.partitions:
            meta = self.metadata.get(path)

            # ✅ ADDED: if no metadata yet, we can't prune safely
            if not meta:
                valid_partitions.append(path)
                continue

            col_meta = meta.get(key_column)

            # ✅ ADDED: if column not in metadata or missing bounds, don't prune
            if not col_meta or col_meta.get("min") is None or col_meta.get("max") is None:
                valid_partitions.append(path)
                continue

            # ✅ ADDED/CHANGED: compare using padded bounds for id
            if key_column == "id":
                part_min = str(col_meta["min"])
                part_max = str(col_meta["max"])
            else:
                part_min = str(col_meta["min"])
                part_max = str(col_meta["max"])

            # ✅ ADDED: overlap test — keep only if ranges overlap
            if key_max < part_min or key_min > part_max:
                continue

            valid_partitions.append(path)

        return valid_partitions

    def add_data(self, data: Dict[str, Any]) -> None:
        self._buffer.append(data)
        self._buffer_rows += 1

        if self._buffer_rows < self.partition_size:
            return

        # If we hit partition size, flush the buffer to a new partition and write
        table = pa.Table.from_pylist(self._buffer)
        path = self._next_partition_path()
        self._write_partition(table, path)

        self.partitions.append(path)
        self._buffer.clear()
        self._buffer_rows = 0

    def update_data(self, key_column: str, key_value: Any, updated_data: Dict[str, Any]) -> None:
        # ✅ ADDED: simplest "pending batch visibility"—flush buffer before updates
        if self._buffer_rows > 0:
            table = pa.Table.from_pylist(self._buffer)
            path = self._next_partition_path()
            self._write_partition(table, path)
            self.partitions.append(path)
            self._buffer.clear()
            self._buffer_rows = 0

        key_value = str(key_value)

        # ✅ ADDED: for id pruning, pad to match metadata
        if key_column == "id":
            key_value_for_trim = key_value.zfill(8)
        else:
            key_value_for_trim = key_value

        for path in self._trim_partitions(key_column, key_value_for_trim):
            table = self._read_partition(path)

            # Turns all values to string for comparison, then vectorize the equality check
            mask = pc.equal(
                pc.cast(table[key_column], pa.string()),
                pa.scalar(key_value)
            )

            # If no rows match, continue to next partition
            if not pc.any(mask).as_py():
                continue

            cols = {}
            for col in table.schema.names:
                if col in updated_data:
                    new_val = pa.scalar(updated_data[col])
                    cols[col] = pc.if_else(mask, new_val, table[col])
                else:
                    cols[col] = table[col]

            updated_table = pa.table(cols)
            self._write_partition(updated_table, path)
            return

    def delete_data(self, key_column: str, key_value: Any) -> None:
        # ✅ ADDED: simplest "pending batch visibility"—flush buffer before deletes
        if self._buffer_rows > 0:
            table = pa.Table.from_pylist(self._buffer)
            path = self._next_partition_path()
            self._write_partition(table, path)
            self.partitions.append(path)
            self._buffer.clear()
            self._buffer_rows = 0

        key_value = str(key_value)

        # ✅ ADDED: for id pruning, pad to match metadata
        if key_column == "id":
            key_value_for_trim = key_value.zfill(8)
        else:
            key_value_for_trim = key_value

        for path in self._trim_partitions(key_column, key_value_for_trim):
            table = self._read_partition(path)

            mask = pc.equal(
                pc.cast(table[key_column], pa.string()),
                pa.scalar(key_value)
            )

            if not pc.any(mask).as_py():
                continue

            keep = pc.invert(mask)
            filtered = table.filter(keep)

            if filtered.num_rows == 0:
                path.unlink()
                self.partitions.remove(path)

                # ✅ ADDED: keep metadata/cache consistent on deletion
                self.metadata.pop(path, None)
                self._cache.pop(path, None)
            else:
                self._write_partition(filtered, path)

            return

    def query_data(self, key_column: str, keys: List[Any]) -> List[Dict[str, Any]]:
        # ✅ ADDED: simplest "pending batch visibility"—flush buffer before queries
        if self._buffer_rows > 0:
            table = pa.Table.from_pylist(self._buffer)
            path = self._next_partition_path()
            self._write_partition(table, path)
            self.partitions.append(path)
            self._buffer.clear()
            self._buffer_rows = 0

        key_strs = [str(k) for k in keys]
        key_set = pa.array(key_strs, type=pa.string())
        results = []

        # ✅ CHANGED: pass the Python keys list to _trim_partitions (NOT the Arrow array)
        keys_for_trim = key_strs
        if key_column == "id":
            # For pruning, compare padded keys to padded metadata
            keys_for_trim = [k.zfill(8) for k in key_strs]

        for path in self._trim_partitions(key_column, keys_for_trim):
            table = self._read_partition(path)

            mask = pc.is_in(
                pc.cast(table[key_column], pa.string()),
                value_set=key_set
            )

            if not pc.any(mask).as_py():
                continue

            filtered = table.filter(mask)
            results.extend(filtered.to_pylist())

        return results

