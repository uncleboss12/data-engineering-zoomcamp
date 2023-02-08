from pathlib import Path
import pandas as pd
from prefect import flow, task
from random import randint
from prefect_gcp.cloud_storage import GcsBucket




@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """ Read taxi data from web into pandas Dataframe"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df 

@flow()
def etl_web_to_gcs() -> None:
    """The Main ETL function"""
    color = "yellow"
    year = 2021
    month = 1
    dataset_file =  f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    # df_clean = clean(df)
    # path = write_local(df_clean, color, dataset_file )
    # write_gcs(path)


if __name__=="__main__":
    etl_web_to_gcs()