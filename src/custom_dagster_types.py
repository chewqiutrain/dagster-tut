import csv
import requests
from dagster import pipeline, solid, DagsterType, OutputDefinition, InputDefinition, TypeCheck


"""
    Type checks occur at solid execution time 
"""

# can be generalised to contracts?
def is_list_of_dicts(_, value):
    return isinstance(value, list) and all(isinstance(element, dict) for element in value)


SimpleDataFrameType = DagsterType(
    name="SimpleDataFrameType",
    type_check_fn=is_list_of_dicts,
    description="A naive representation of a data frame, as returned by csv.DictReader"
)


"""
    Metadata about type checks 
    e.g. 
    - record number of rows and columns in the dataset, 
    - record where type checks succeeded
    - provide info about where type checks fail 
"""
def more_involved_data_frame_type_check(_, value) -> TypeCheck:
    if not isinstance(value, list):
        return TypeCheck(
            success=False,
            description=f"MoreInvolvedDataFrame should be a list of dicts, got {type(value)}"
        )

    fields = list(value[0].keys())

    nrows = len(value)
    for i in range(0, nrows):
        row = value[i]
        reporting_index = i + 1
        if not isinstance(row, dict):
            return TypeCheck(
                success=False,
                description=(
                    f"MoreInvolvedDataFrame should be a list of dicts, got {type(row)} for row {reporting_index}"
                )
            )

        row_fields = list(row.keys())
        if row_fields != fields:
            return TypeCheck(
                success=False,
                description=(
                    f"Rows in MoreInvolvedDataFrame should have same fields, got: {row_fields}"
                    f"for row {reporting_index}. Expected: {fields}"
                )
            )

    ncols = len(fields)
    return TypeCheck(
        success=True,
        description="MoreInvolvedDataFrame summary statistics",
        metadata={
            "n_rows": nrows,
            "n_cols": ncols,
            "column_names": ", ".join(fields)
        }
    )


MoreInvolvedDataFrameType = DagsterType(
    name="MoreInvolvedDataFrame",
    type_check_fn=more_involved_data_frame_type_check,
    description="A more involved type check"
)


# output type checks occur immediately after the solid is executed
@solid(
    output_defs=[OutputDefinition(MoreInvolvedDataFrameType)]
)
def download_csv(context):
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    context.log.info("Read {n_lines} lines".format(n_lines=len(lines)))
    #return ["not a adict"]
    return [row for row in csv.DictReader(lines)]


# input type checks occur immediately before the solid is executed
@solid(
    input_defs=[InputDefinition(name="cereals", dagster_type=MoreInvolvedDataFrameType)]
)
def sort_by_calories(context, cereals):
    sorted_cereals = sorted(cereals, key=lambda cereal: cereal["calories"])
    context.log.info(f"Most caloric meal is: {sorted_cereals[-1]['name']}")


@pipeline
def typed_pipeline():
    cereals = download_csv()
    sort_by_calories(cereals)