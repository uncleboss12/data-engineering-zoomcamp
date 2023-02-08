from pathlib import Path
import pandas as pd
from prefect import flow, task
from random import randint
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials 


@task(retries=3)
def extract_from_gcs(color:str, year:int, month:int):
    """Download trip data from GCS"""

    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("prefect-new-zoomcamp")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/" )
    return Path(f"../data/{gcs_path}")


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    #df["passenger_count"] = df["passenger_count"].fillna(0)
    #print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df 

@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("uncleboss-gcp-creds")

    df.to_gbq(
        destination_table= "new_guys.rides1",
        project_id= "friendly-joy-375019",
        credentials= gcp_credentials_block.get_credentials_from_service_account(), 
        chunksize= 500_000,
        if_exists= 'append'
    )
    


@flow()
def etl_gcs_to_bq(year: int, month: int, color:str) -> None:
    """"Main ETL flow to load data into Big Query"""
    color ="yellow"
    year = 2019
    month = 2


    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    
@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2019, color: str = "yellow"
):
    for month in months:
        etl_gcs_to_bq(year, month, color)

    



if __name__=="__main__":
    color = 'yellow'
    months = [2, 3]
    year = 2019
    etl_parent_flow(months, year, color)

    
