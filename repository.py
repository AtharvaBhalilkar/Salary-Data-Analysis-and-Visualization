from dagster import repository, job
from extract import extract_salaries
from transform import transform_salaries
from load import load_salaries

@job
def etl_job():
    # Define the job steps
    salaries = extract_salaries()
    transformed_salaries = transform_salaries(salaries)
    load_salaries(transformed_salaries)

# Define the repository to hold the job
@repository
def salaries_repository():
    return [etl_job]
