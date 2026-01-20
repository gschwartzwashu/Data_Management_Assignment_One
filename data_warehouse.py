from abc import ABC, abstractmethod
from typing import Any, List, Dict


class DataWarehouse(ABC):
    @abstractmethod
    def add_data(self, data: Dict[str, Any]) -> None:
        """
        Add a row of data to the warehouse.

        Args:
            data (Dict[str, Any]): A dictionary representing a row of data.
        """
        pass

    @abstractmethod
    def update_data(self, key_column: str, key_value: Any, updated_data: Dict[str, Any]) -> None:
        """
        Update a row of data in the warehouse.

        Args:
            key_column (str): The column to match for the update.
            key_value (Any): The value to match in the key column.
            updated_data (Dict[str, Any]): A dictionary with updated column values.
        """
        pass

    @abstractmethod
    def delete_data(self, key_column: str, key_value: Any) -> None:
        """
        Delete a row of data from the warehouse.

        Args:
            key_column (str): The column to match for the deletion.
            key_value (Any): The value to match in the key column.
        """
        pass

    @abstractmethod
    def query_data(self, key_column: str, keys: List[Any]) -> List[Dict[str, Any]]:
        """
        Query data from the warehouse.

        Args:
            key_column (str): The column to match for the query.
            keys (List[Any]): A list of values to search for in the key column.

        Returns:
            List[Dict[str, Any]]: The query results.
        """
        pass
