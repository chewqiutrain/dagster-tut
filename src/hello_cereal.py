import requests
import csv
from dagster import pipeline, solid, execute_pipeline


@solid
def download_cereals():
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    return [row for row in csv.DictReader(lines)]

@solid
def find_sugariest(context, cereals):
    sorted_by_sugar = sorted(cereals, key=lambda cereal: cereal["sugars"])
    context.log.info(f"{sorted_by_sugar[-1]['name']} is the sugariest cereal")

"""
This call doesn't actually execute the solid. 
Within the bodies of functions decorated with @pipeline, we use function calls to 
indicate the dependency structure of the solids making up the pipeline. 
"""
@pipeline
def serial_pipeline():
    find_sugariest(download_cereals())


if __name__ == "__main__":
    result = execute_pipeline(serial_pipeline)