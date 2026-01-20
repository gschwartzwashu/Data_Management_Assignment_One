from typing import Any, List, Dict
from data_warehouse import DataWarehouse
from pathlib import Path
import csv

class NaiveCSVWarehouse(DataWarehouse):

    def __init__(self, file_name: str):
        self.data = None

        path = Path(file_name)
        path.touch(exist_ok=True)

        with path.open(newline="", mode="r") as f:
            reader = csv.reader(f)
            self.data = list(reader)

    def add_data(self, data: Dict[str, Any]) -> None:
        # Implementation here
        raise NotImplementedError("This method is not implemented yet.")

    def update_data(self, key_column: str, key_value: Any, updated_data: Dict[str, Any]) -> None:
        # Implementation here
        raise NotImplementedError("This method is not implemented yet.")

    def delete_data(self, key_column: str, key_value: Any) -> None:
        # Implementation here
        raise NotImplementedError("This method is not implemented yet.")

    def query_data(self, key_column: str, keys: List[Any]) -> List[Dict[str, Any]]:
        # Implementation here
        raise NotImplementedError("This method is not implemented yet.")
