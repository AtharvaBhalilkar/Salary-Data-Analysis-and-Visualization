from dagster import op # type: ignore
from sqlalchemy import create_engine # type: ignore

POSTGRES_CONNECTION_STRING = "postgresql://postgres:rootpassword@localhost:5432/dataprogramming"

@op
def load(data):
    """
    Load the extracted data into PostgreSQL.
    """
    table_name = "salaries"
    print("Loading data into PostgreSQL...")
    engine = create_engine(POSTGRES_CONNECTION_STRING)
    try:
        with engine.connect() as connection:
            # Load data into PostgreSQL
            data.to_sql(table_name, con=connection, index=False, if_exists='replace')
        print(f"Data loaded successfully into table: {table_name}")
    except Exception as e:
        print(f"Error loading data: {e}")
    finally:
        engine.dispose()
