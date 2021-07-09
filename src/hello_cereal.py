import requests
import csv
from dagster import pipeline, solid, execute_pipeline


@solid
def download_cereals():
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    return [row for row in csv.DictReader(lines)]


@solid
def find_highest_calorie_cereal(cereals):
    sorted_cereals = list(
        sorted(cereals, key=lambda cereal: cereal["calories"])
    )
    return sorted_cereals[-1]["name"]


@solid
def find_highest_protein_cereal(cereals):
    sorted_cereals = list(
        sorted(cereals, key=lambda cereal: cereal["protein"])
    )
    return sorted_cereals[-1]["name"]


@solid
def display_results(context, most_calories, most_proteins):
    context.log.info(f"Most caloric cereal: {most_calories}")
    context.log.info(f"Most protein-rich cereal: {most_proteins}")

"""
This call doesn't actually execute the solid. 
Within the bodies of functions decorated with @pipeline, we use function calls to 
indicate the dependency structure of the solids making up the pipeline. 
"""


@pipeline
def serial_pipeline():
    cereals = download_cereals()
    display_results(
        most_calories=find_highest_calorie_cereal(cereals),
        most_proteins=find_highest_protein_cereal(cereals)
    )


if __name__ == "__main__":
    result = execute_pipeline(serial_pipeline)
