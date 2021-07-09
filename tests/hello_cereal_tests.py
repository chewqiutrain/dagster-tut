from dagster import execute_pipeline, execute_solid
from src.hello_cereal import serial_pipeline, download_cereals
from dagster.core.execution.results import PipelineExecutionResult


def test_download_cereals_solid():
    res = execute_solid(download_cereals)
    assert res.success
    assert len(res.output_value()) == 77


def test_pipeline():
    res = execute_pipeline(serial_pipeline)
    assert res.success
    assert len(res.result_for_solid("download_cereals").output_value()) == 77

