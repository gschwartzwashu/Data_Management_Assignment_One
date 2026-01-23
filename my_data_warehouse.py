from typing import Any, List, Dict
from data_warehouse import DataWarehouse
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import shutil


class MyDataWarehouse(DataWarehouse):
    """
    Partitions rows into Parquet files of size `partition_size`.
    Keeps per-partition min/max metadata to skip files when possible.
    Flushes buffered inserts so query/update/delete always see all rows.
    """

    def __init__(self, partition_size, storage_dir) -> None:
        self.partition_size = partition_size
        self.storage_dir = Path(storage_dir)

        if self.storage_dir.exists():
            shutil.rmtree(self.storage_dir)

        self.storage_dir.mkdir(parents=True, exist_ok=True)

        self.buf = []
        self.buf_n = 0
        self.metadata = {}
        self.partitions = sorted(self.storage_dir.glob("part_*.parquet"))

    def _next_partition_path(self) -> Path:
        idx = len(self.partitions)
        return self.storage_dir / f"part_{idx:05d}.parquet"

    def _read_partition(self, path: Path) -> pa.Table:
        return pq.read_table(path)

    def _update_metadata(self, table: pa.Table, path: Path) -> None:
        part_meta = {}
        for col in table.schema.names:
            mm = pc.min_max(table[col])
            part_meta[col] = {"min": mm["min"], "max": mm["max"], "num_rows": table.num_rows}
        self.metadata[path] = part_meta

    def _write_partition(self, table: pa.Table, path: Path) -> None:
        pq.write_table(table, path)
        self._update_metadata(table, path)

    def _flush_buf(self) -> None:
        if self.buf_n == 0:
            return
        table = pa.Table.from_pylist(self.buf)
        path = self._next_partition_path()
        self._write_partition(table, path)
        self.partitions.append(path)
        self.buf.clear()
        self.buf_n = 0

    def _trim_partitions(self, key_column: str, keys: Any) -> List[Path]:
        if key_column == "id":
            return list(self.partitions)

        if isinstance(keys, (list, tuple, set)):
            ks = [str(k) for k in keys]
        else:
            ks = [str(keys)]

        if not ks:
            return list(self.partitions)

        kmin, kmax = min(ks), max(ks)
        out = []

        for path in self.partitions:
            meta = self.metadata.get(path) or {}
            cm = meta.get(key_column)
            if not cm:
                out.append(path)
                continue

            pmin, pmax = str(cm["min"]), str(cm["max"])
            if kmax < pmin or kmin > pmax:
                continue

            out.append(path)

        return out

    def add_data(self, data: Dict[str, Any]) -> None:
        self.buf.append(data)
        self.buf_n += 1

        if self.buf_n < self.partition_size:
            return

        table = pa.Table.from_pylist(self.buf)
        path = self._next_partition_path()
        self._write_partition(table, path)

        self.partitions.append(path)
        self.buf.clear()
        self.buf_n = 0

    def update_data(self, key_column: str, key_value: Any, updated_data: Dict[str, Any]) -> None:
        self._flush_buf()
        key_value = str(key_value)

        for path in self._trim_partitions(key_column, [key_value]):
            table = self._read_partition(path)

            mask = pc.equal(pc.cast(table[key_column], pa.string()), pa.scalar(key_value))
            if not pc.any(mask).as_py():
                continue

            cols = {}
            for col in table.schema.names:
                if col in updated_data:
                    cols[col] = pc.if_else(mask, pa.scalar(updated_data[col]), table[col])
                else:
                    cols[col] = table[col]

            self._write_partition(pa.table(cols), path)
            return

    def delete_data(self, key_column: str, key_value: Any) -> None:
        self._flush_buf()
        key_value = str(key_value)

        for path in self._trim_partitions(key_column, [key_value]):
            table = self._read_partition(path)

            mask = pc.equal(pc.cast(table[key_column], pa.string()), pa.scalar(key_value))
            if not pc.any(mask).as_py():
                continue

            keep = pc.invert(mask)
            table2 = table.filter(keep)

            if table2.num_rows == 0:
                path.unlink()
                self.partitions.remove(path)
                self.metadata.pop(path, None)
            else:
                self._write_partition(table2, path)

            return

    def query_data(self, key_column: str, keys: List[Any]) -> List[Dict[str, Any]]:
        self._flush_buf()
        key_set = pa.array([str(k) for k in keys], type=pa.string())
        out = []

        for path in self._trim_partitions(key_column, keys):
            table = self._read_partition(path)

            mask = pc.is_in(pc.cast(table[key_column], pa.string()), value_set=key_set)
            if not pc.any(mask).as_py():
                continue

            out.extend(table.filter(mask).to_pylist())

        return out
