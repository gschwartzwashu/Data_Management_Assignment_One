import time
import random
from typing import Callable, Any, Tuple, List
from faker import Faker
from csv_warehouse import NaiveCSVWarehouse
from my_data_warehouse import MyDataWarehouse


def measure_time(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Tuple[Any, float]:
    """
    Measure the time taken to execute a function.

    Args:
        func (Callable[..., Any]): The function to measure.
        *args: Positional arguments for the function.
        **kwargs: Keyword arguments for the function.

    Returns:
        Tuple[Any, float]: The result of the function and the time taken in seconds.
    """
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    return result, end_time - start_time


def generate_fake_data(num_rows: int) -> List[List[str]]:
    """
    Generate fake data using the Faker library.

    Args:
        num_rows (int): The number of rows to generate.

    Returns:
        List[List[str]]: A list of rows, where each row is a list of strings.
    """
    fake = Faker()
    data = []
    for i in range(1, num_rows + 1):
        data.append({"id": str(i), "name": fake.name(), "address": fake.address(), "email": fake.email()})
    return data


def run_tests() -> None:
    # Initialize both implementations
    naive_warehouse = NaiveCSVWarehouse("naive_warehouse.csv")
    my_warehouse = MyDataWarehouse(partition_size=1000, storage_dir="my_partitions")

    # Generate 10,000 rows of data
    num_rows = 10_000
    print(f"Generating {num_rows} rows of fake data...")
    data = generate_fake_data(num_rows)

    # Insert Test
    print("\nTesting Insert Operations...")
    for warehouse, name in [(naive_warehouse, "NaiveCSVWarehouse"), (my_warehouse, "MyDataWarehouse")]:
        _, total_insert_time = measure_time(
            lambda: [warehouse.add_data(row) for row in data]
        )
        avg_insert_time = total_insert_time / num_rows
        print(f"{name}: Inserted {num_rows} rows in {total_insert_time:.6f} seconds (avg: {avg_insert_time:.6f} seconds per call)")

    # Randomly Update 100 Rows
    print("\nTesting Update Operations...")
    update_indices = random.sample(range(num_rows), 100)
    updated_data = [{"id": str(i + 1), "name": f"Updated-{i + 1}", "address": f"Updated-{i + 1}", "email": f"Updated-{i + 1}"} for i in update_indices]
    for warehouse, name in [(naive_warehouse, "NaiveCSVWarehouse"), (my_warehouse, "MyDataWarehouse")]:
        _, total_update_time = measure_time(
            lambda: [warehouse.update_data("id", row["id"], row) for row in updated_data]
        )
        avg_update_time = total_update_time / 100
        print(f"{name}: Updated 100 rows in {total_update_time:.6f} seconds (avg: {avg_update_time:.6f} seconds per call)")

    # Random Queries
    print("\nTesting Query Operations...")
    query_keys = [[str(random.randint(1, num_rows * 2)) for _ in range(i)] for i in range(100)]  # Some keys will not exist
    for warehouse, name in [(naive_warehouse, "NaiveCSVWarehouse"), (my_warehouse, "MyDataWarehouse")]:
        _, total_query_time = measure_time(
            lambda: [warehouse.query_data("id", keys) for keys in query_keys]
        )
        avg_query_time = total_query_time / 100
        print(f"{name}: Queried 100 random keys in {total_query_time:.6f} seconds (avg: {avg_query_time:.6f} seconds per call)")

    # Delete All Rows
    print("\nTesting Delete Operations...")
    for warehouse, name in [(naive_warehouse, "NaiveCSVWarehouse"), (my_warehouse, "MyDataWarehouse")]:
        _, total_delete_time = measure_time(
            lambda: [warehouse.delete_data("id", str(random.randint(1, num_rows * 2))) for _ in range(1, 1000)]
        )
        avg_delete_time = total_delete_time / 1000
        print(f"{name}: Deleted 1000 rows in {total_delete_time:.6f} seconds (avg: {avg_delete_time:.6f} seconds per call)")


if __name__ == "__main__":
    run_tests()
