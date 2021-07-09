from src.configuring_pipelines import configurable_pipeline
from dagster import execute_pipeline

def test_configurable_pipeline():
    test_run_config = {
        "solids": {
            "download_csv": {
                "config": {
                    "url": "https://docs.dagster.io/assets/cereal.csv"
                }
            }
        }
    }
    res = execute_pipeline(configurable_pipeline, run_config=test_run_config)
    print(res.success)


if __name__ == "__main__":
    test_configurable_pipeline()