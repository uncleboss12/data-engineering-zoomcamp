from pathlib import Path
import pandas as pd
from prefect import flow, task
from random import randint
from prefect_gcp.cloud_storage import GcsBucket


@task()
def write_local(df: pd.DataFrame, color:str, dataset_file: str) -> Path:
    """ Write DataFrame out locally as parquet file """
    path = Path(f"data/{color}/{dataset_file}.parquet")

     
    if not path.parent.is_dir():
        path.parent.mkdir(parents=True)  
  
    
    df.to_parquet(path, compression="gzip")
    return path



@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("prefect-new-zoomcamp")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
    return 

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """ Read taxi data from web into pandas Dataframe"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df 

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtypes issues"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@flow()
def etl_web_to_gcs(year: int, month: int, color:str, ) -> None:
    """The Main ETL finction"""
    # color ="yellow"
    # year = 2019
    # month = 2
    dataset_file =  f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file )
    write_gcs(path)


@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2019, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__=="__main__":
    color = 'yellow'
    months = [2, 3]
    year = 2019
    etl_parent_flow(months, year, color)

