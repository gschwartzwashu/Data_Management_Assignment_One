from typing import Any, List, Dict
from data_warehouse import DataWarehouse
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import shutil

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

    def _next_partition_path(self) -> Path:
        idx = len(self.partitions)
        return self.storage_dir / f"part_{idx:05d}.parquet"

    def _read_partition(self, path: Path) -> pa.Table:
        return pq.read_table(path)

    def _update_metadata(self, table: pa.Table, path: Path) -> None:
        """
        Update metadata for a given partition by column finding min, max, and number of rows like Snowflake
        """
        partition_meta = {}
        for col in table.schema.names:
            col_data = table[col]
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

    def _trim_partitions(self, key_column: str, key_value: Any) -> List[Path]:
        """
        Iterate over partitions and use metadata to filter out partitions that cannot contain the key_value by min/max
        """
        key_value = str(key_value)
        valid_partitions = []

        for path in self.partitions:
            meta = self.metadata.get(path)
            col_meta = meta.get(key_column, None)

            if col_meta is None:
                valid_partitions.append(path)
                continue

            if key_value < str(col_meta["min"]) or key_value > str(col_meta["max"]):
                continue

            valid_partitions.append(path)

        return valid_partitions

    def add_data(self, data: Dict[str, Any]) -> None:
        self._buffer.append(data)
        self._buffer_rows += 1

        if self._buffer_rows < self.partition_size:
            return

        # If we over run the partition size, flush the buffer to a new partition and write
        table = pa.Table.from_pylist(self._buffer)
        path = self._next_partition_path()
        self._write_partition(table, path)

        self.partitions.append(path)
        self._buffer.clear()
        self._buffer_rows = 0

    def update_data(self, key_column: str, key_value: Any, updated_data: Dict[str, Any]) -> None:
        key_value = str(key_value)

        for path in self._trim_partitions(key_column, key_value):
            table = self._read_partition(path)

            # Turns all values to string for comparison, then vectorize the equality check to return list of bools of whether there is a match
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
                    # Turn new value into a scalar, set the col to the new value where mask is true, else keep old value
                    new_val = pa.scalar(updated_data[col])
                    cols[col] = pc.if_else(mask, new_val, table[col])
                else:
                    cols[col] = table[col]

            updated_table = pa.table(cols)
            self._write_partition(updated_table, path)
            return

    def delete_data(self, key_column: str, key_value: Any) -> None:
        key_value = str(key_value)

        for path in self._trim_partitions(key_column, key_value):
            table = self._read_partition(path)

            # Turns all values to string for comparison, then vectorize the equality check to return list of bools of whether there is a match
            mask = pc.equal(
                pc.cast(table[key_column], pa.string()),
                pa.scalar(key_value)
            )

            if not pc.any(mask).as_py():
                continue

            # Invert vector to give values we should keep, then only keep those values
            keep = pc.invert(mask)
            filtered = table.filter(keep)

            if filtered.num_rows == 0:
                path.unlink()
                self.partitions.remove(path)
            else:
                self._write_partition(filtered, path)

            return

    def query_data(self, key_column: str, keys: List[Any]) -> List[Dict[str, Any]]:
        key_set = pa.array([str(k) for k in keys], type=pa.string())
        results = []

        for path in self._trim_partitions(key_column, key_set):
            table = self._read_partition(path)

            # Vectorize look up value, and see if each row is in the set of values returning list of bools
            mask = pc.is_in(
                pc.cast(table[key_column], pa.string()),
                value_set=key_set
            )

            if not pc.any(mask).as_py():
                continue

            # Filter where mask is true to get only matching rows
            filtered = table.filter(mask)
            results.extend(filtered.to_pylist())

        return results
