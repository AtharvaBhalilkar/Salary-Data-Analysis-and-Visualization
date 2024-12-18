from dagster import job # type: ignore
from extract import extract
from load import load

@job
def elt_pipeline():
    data = extract()
    load(data)
