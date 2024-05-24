import duckdb
import polars as pl

def parquet_desired_columns_list(desired_columns: list, database_path: str, table_name: str) -> None:
    """Writes a parquet of columns from a data set using a list of desired columns. 

    Args:
        list (desired_columns): list of desired columns
        str (database_path): local path to the database of interest
        str (table_name): name of table to work with
    """
    con = duckdb.connect(database_path)
    
    query = f'SELECT {', '.join(desired_columns)} FROM {table_name}'

    desired_data = pl.read_database(query=query, connection=con)

    desired_data.write_parquet("output.parquet")


def parquet_desired_columns_dict(desired_columns: dict, database_path: str, table_name: str) -> None:
    """Writes a parquet of columns from a data set using a dict of desired columns. 

    Args:
        dict (desired_columns): list of desired columns
        str (database_path): local path to the database of interest
        str (table_name): name of table to work with
    """
    parquet_desired_columns_list(desired_columns.keys, database_path, table_name)