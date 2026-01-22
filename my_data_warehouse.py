from typing import Any, List, Dict
from data_warehouse import DataWarehouse
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

class MyDataWarehouse(DataWarehouse):

    def __init__(self, partition_size, storage_dir) -> None:
        self.partition_size = partition_size
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

        self.partitions = sorted(self.storage_dir.glob("part_*.parquet"))

    def _next_partition_path(self) -> Path:
        idx = len(self.partitions)
        return self.storage_dir / f"part_{idx:05d}.parquet"

    def _read_partition(self, path: Path) -> pa.Table:
        return pq.read_table(path)

    def _write_partition(self, table: pa.Table, path: Path) -> None:
        pq.write_table(table, path)

    def _dict_to_table(self, data: Dict[str, Any]) -> pa.Table:
        return pa.table({k: [v] for k, v in data.items()})

    def add_data(self, data: Dict[str, Any]) -> None:
        row = self._dict_to_table(data)

        if not self.partitions:
            path = self._next_partition_path()
            self._write_partition(row, path)
            self.partitions.append(path)
            return

        last_path = self.partitions[-1]
        table = self._read_partition(last_path)

        if table.num_rows >= self.partition_size:
            path = self._next_partition_path()
            self._write_partition(row, path)
            self.partitions.append(path)
        else:
            combined = pa.concat_tables([table, row])
            self._write_partition(combined, last_path)

    def update_data(self, key_column: str, key_value: Any, updated_data: Dict[str, Any]) -> None:
        key_value = str(key_value)

        for path in self.partitions:
            table = self._read_partition(path)

            mask = pc.equal(
                pc.cast(table[key_column], pa.string()),
                pa.scalar(key_value)
            )

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
        key_value = str(key_value)

        for path in list(self.partitions):
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
            else:
                self._write_partition(filtered, path)

            return

    def query_data(self, key_column: str, keys: List[Any]) -> List[Dict[str, Any]]:
        key_set = pa.array([str(k) for k in keys], type=pa.string())
        results = []

        for path in self.partitions:
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
