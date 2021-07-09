import requests
import csv
from dagster import pipeline, solid, execute_pipeline


@solid
def hello_cereal(context):
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    cereals = [row for row in csv.DictReader(lines)]
    context.log.info(f"Found {len(cereals)} cereals")

    return cereals


"""
This call doesn't actually execute the solid. 
Within the bodies of functions decorated with @pipeline, we use function calls to 
indicate the dependency structure of the solids making up the pipeline. 
"""
@pipeline
def hello_cereal_pipeline():
    hello_cereal()



if __name__ == "__main__":
    result = execute_pipeline(hello_cereal_pipeline)