from dagster import job
from .assets import extract_salaries, transform_salaries, load_salaries


# Define the ETL pipeline as a job
@job
def etl_pipeline():
    extracted = extract_salaries()
    transformed = transform_salaries(extracted)
    load_salaries(transformed)
