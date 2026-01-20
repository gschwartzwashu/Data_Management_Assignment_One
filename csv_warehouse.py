from typing import Any, List, Dict
from data_warehouse import DataWarehouse
from pathlib import Path
import csv

class NaiveCSVWarehouse(DataWarehouse):

    def __init__(self, file_name: str):
        self.file_name = file_name

        path = Path(file_name)
        path.touch(exist_ok=True)

        with path.open(newline="", mode="r") as f:
            reader = csv.DictReader(f)
            self.data = list(reader)

    def _write_all(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            Path(self.file_name).write_text("")
            return

        with open(self.file_name, mode="w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    def add_data(self, data: Dict[str, Any]) -> None:
        path = Path(self.file_name)
        file_exists = path.stat().st_size > 0

        with path.open(mode="a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=data.keys())
            if not file_exists:
                writer.writeheader()

            writer.writerow(data)

        self.data.append(data)

    def update_data(self, key_column: str, key_value: Any, updated_data: Dict[str, Any]) -> None:
        key_value = str(key_value)

        for row in self.data:
            if row[key_column] == key_value:
                row.update(updated_data)
                self._write_all(self.data)
                return

    def delete_data(self, key_column: str, key_value: Any) -> None:
        key_value = str(key_value)

        for i, row in enumerate(self.data):
            if row[key_column] == key_value:
                del self.data[i]
                self._write_all(self.data)
                return

    def query_data(self, key_column: str, keys: List[Any]) -> List[Dict[str, Any]]:
        key_set = {str(k) for k in keys}

        return [
            row for row in self.data
            if row.get(key_column) in key_set
        ]
