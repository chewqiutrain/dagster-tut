import csv
import requests
from dagster import solid, pipeline, execute_pipeline

@solid(
    config_schema={
        "url": str
    }
)
def download_csv(context):
    url = context.solid_config["url"]
    response = requests.get(url)
    lines = response.text.split("\n")
    return [row for row in csv.DictReader(lines)]

@solid
def sorted_by_calories(context, cereals):
    sorted_cereals = sorted(
        cereals,
        key=lambda cereal: int(cereal["calories"])
    )
    context.log.info(f"Most caloric meal: {sorted_cereals[-1]['name']}")

@pipeline
def configurable_pipeline():
    sorted_by_calories(download_csv())