import os

import boto3
import pandas as pd
import psycopg2
from prefect import flow, task
from pymongo import MongoClient

# --------------------------------------------------------------------------------
# MongoDB Connection Setup
# --------------------------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["fitness_db"]

# --------------------------------------------------------------------------------
# Redshift Connection Setup
# --------------------------------------------------------------------------------
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST", "")
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", "5439"))
REDSHIFT_DB = os.getenv("REDSHIFT_DB", "")
REDSHIFT_USER = os.getenv("REDSHIFT_USER", "")
REDSHIFT_PASS = os.getenv("REDSHIFT_PASSWORD", "")

# --------------------------------------------------------------------------------
# AWS and CSV Configuration
# --------------------------------------------------------------------------------
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
CSV_S3_PATH = os.getenv("CSV_S3_PATH", None)

# Mapping for level factors
LEVEL_MAPPING = {"Beginner": 1.0, "Intermediate": 1.3, "Advanced": 1.6}


@task
def download_csv_from_s3():
    """
    Downloads the main CSV from S3 using Boto3 and returns its local filename.
    """
    s3_path_parts = CSV_S3_PATH.replace("s3://", "").split("/", 1)
    bucket = s3_path_parts[0]
    key = s3_path_parts[1]
    local_csv = "megaGymDataset.csv"

    s3 = boto3.client("s3", region_name=AWS_REGION)
    print(f"Downloading CSV from {CSV_S3_PATH} to {local_csv} ...", flush=True)
    s3.download_file(bucket, key, local_csv)
    return local_csv


@task
def get_last_timestamp():
    """
    Connects to Redshift and retrieves the maximum timestamp from the
    exercises_enriched table. If the table does not exist, returns 0.
    """
    conn = psycopg2.connect(
        dbname=os.getenv("REDSHIFT_DB", ""),
        user=os.getenv("REDSHIFT_USER", ""),
        password=os.getenv("REDSHIFT_PASSWORD", ""),
        host=os.getenv("REDSHIFT_HOST", ""),
        port=int(os.getenv("REDSHIFT_PORT", "5439")),
    )
    cur = conn.cursor()
    try:
        cur.execute("SELECT COALESCE(MAX(timestamp), 0) FROM exercises_enriched;")
        last_ts = cur.fetchone()[0]
    except psycopg2.errors.UndefinedTable:
        # Table does not exist; so return 0
        last_ts = 0
    finally:
        cur.close()
        conn.close()
    print(f"Last timestamp in Redshift: {last_ts}", flush=True)
    return last_ts


@task
def extract_from_mongo(last_ts: int):
    """
    Extracts documents from the 'exercises_calories' collection in MongoDB that have a
    'timestamp' greater than the given last_ts.
    """
    docs = list(db["exercises_calories"].find({"timestamp": {"$gt": last_ts}}))
    df = pd.DataFrame(docs)
    print(
        f"Extracted {len(df)} new documents from Mongo (timestamp > {last_ts}).",
        flush=True,
    )
    return df


@task
def merge_csv_data(mongo_df: pd.DataFrame, local_csv: str):
    """
    Merges MongoDB records with the CSV data to add additional exercise fields.
    The CSV columns are renamed as follows:
      - 'Desc' -> 'description'
      - 'Type' -> 'exercise_type'
      - 'BodyPart' -> 'body_part'
      - 'Equipment' -> 'equipment'
      - 'Level' -> 'level'
      - 'Rating' -> 'rating'
      - 'RatingDesc' -> 'rating_desc'
    The merge is performed on the exercise title.
    """
    if mongo_df.empty:
        print("No documents from Mongo to merge with CSV.", flush=True)
        return pd.DataFrame()

    csv_df = pd.read_csv(local_csv)
    rename_map = {
        "Desc": "description",
        "Type": "exercise_type",
        "BodyPart": "body_part",
        "Equipment": "equipment",
        "Level": "level",
        "Rating": "rating",
        "RatingDesc": "rating_desc",
    }
    csv_df.rename(columns=rename_map, inplace=True)

    merged_df = mongo_df.merge(
        csv_df, how="left", left_on="title", right_on="Title"  # original CSV column
    )
    return merged_df


@task
def compute_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes derived metrics for analysis:
      - kcal_per_minute = total_calories_aprox / duration
      - kcal_per_lb = total_calories_aprox / weight
      - enjoyment_weighted_kcal = total_calories_aprox * (rating / 10)
      - level_weighted_kcal = total_calories_aprox * level_factor
      - kcal_per_hour_per_kg = (total_calories_aprox / (duration * weight * 0.453592)) * 60
    """
    if df.empty:
        return df

    # Avoid division by zero
    df["duration"] = df["duration"].replace({0: 1})
    df["weight"] = df["weight"].replace({0: 1})

    df["kcal_per_minute"] = df.apply(
        lambda row: (
            row["total_calories_aprox"] / row["duration"]
            if row.get("total_calories_aprox", 0) and row.get("duration", 0)
            else 0
        ),
        axis=1,
    )

    df["kcal_per_lb"] = df.apply(
        lambda row: (
            row["total_calories_aprox"] / row["weight"]
            if row.get("total_calories_aprox", 0) and row.get("weight", 0)
            else 0
        ),
        axis=1,
    )

    df["enjoyment_weighted_kcal"] = df.apply(
        lambda row: (
            row["total_calories_aprox"] * (row["rating"] / 10.0)
            if pd.notnull(row.get("total_calories_aprox"))
            and pd.notnull(row.get("rating"))
            else 0
        ),
        axis=1,
    )

    df["level_factor"] = df["level"].map(LEVEL_MAPPING).fillna(1.0)
    df["level_weighted_kcal"] = df.apply(
        lambda row: (
            row["total_calories_aprox"] * row["level_factor"]
            if pd.notnull(row.get("total_calories_aprox"))
            else 0
        ),
        axis=1,
    )

    df["kcal_per_hour_per_kg"] = df.apply(
        lambda row: (
            (row["total_calories_aprox"] / (row["duration"] * row["weight"] * 0.453592)) * 60
            if pd.notnull(row.get("total_calories_aprox"))
            and pd.notnull(row.get("duration"))
            and pd.notnull(row.get("weight"))
            and row["duration"] > 0
            and row["weight"] > 0
            else 0
        ),
        axis=1,
    )
    return df


@task
def load_to_redshift(df: pd.DataFrame):
    """
    Connects to Redshift and inserts new records into the 'exercises_enriched' table.
    The table contains 19 columns:
      - event_id, title, weight, duration,
      - calories_per_hour_aprox, total_calories_aprox,
      - description, exercise_type, body_part, equipment,
      - level, rating, rating_desc,
      - kcal_per_minute, kcal_per_lb, enjoyment_weighted_kcal,
      - level_weighted_kcal, kcal_per_hour_per_kg, timestamp.

    This task:
      - Reindexes the DataFrame to ensure exactly these 19 columns.
      - Filters out rows where total_calories_aprox is missing (None/NaN).
      - Then inserts the new records into Redshift.
    """
    if df.empty:
        print("No rows to insert into Redshift.", flush=True)
        return

    # Define the final column order
    final_columns = [
        "event_id",
        "title",
        "weight",
        "duration",
        "calories_per_hour_aprox",
        "total_calories_aprox",
        "description",
        "exercise_type",
        "body_part",
        "equipment",
        "level",
        "rating",
        "rating_desc",
        "kcal_per_minute",
        "kcal_per_lb",
        "enjoyment_weighted_kcal",
        "level_weighted_kcal",
        "kcal_per_hour_per_kg",
        "timestamp",
    ]

    # Reindex the DataFrame to exactly these columns
    df = df.reindex(columns=final_columns)

    # Filter out rows with missing total_calories_aprox
    df = df[df["total_calories_aprox"].notna()]

    # Connect to Redshift
    conn = psycopg2.connect(
        dbname=os.getenv("REDSHIFT_DB", ""),
        user=os.getenv("REDSHIFT_USER", ""),
        password=os.getenv("REDSHIFT_PASSWORD", ""),
        host=os.getenv("REDSHIFT_HOST", ""),
        port=int(os.getenv("REDSHIFT_PORT", "5439")),
    )
    cur = conn.cursor()

    # Ensure the table exists
    create_stmt = """
    CREATE TABLE IF NOT EXISTS exercises_enriched (
        event_id VARCHAR(150) PRIMARY KEY,
        title VARCHAR(255),
        weight INT,
        duration INT,
        calories_per_hour_aprox FLOAT,
        total_calories_aprox FLOAT,
        description VARCHAR(1000),
        exercise_type VARCHAR(255),
        body_part VARCHAR(255),
        equipment VARCHAR(255),
        level VARCHAR(100),
        rating FLOAT,
        rating_desc VARCHAR(1000),
        kcal_per_minute FLOAT,
        kcal_per_lb FLOAT,
        enjoyment_weighted_kcal FLOAT,
        level_weighted_kcal FLOAT,
        kcal_per_hour_per_kg FLOAT,
        timestamp BIGINT
    );
    """
    cur.execute(create_stmt)
    conn.commit()

    # Insert new records
    insert_stmt = """
    INSERT INTO exercises_enriched (
        event_id, title, weight, duration,
        calories_per_hour_aprox, total_calories_aprox,
        description, exercise_type, body_part, equipment,
        level, rating, rating_desc,
        kcal_per_minute, kcal_per_lb,
        enjoyment_weighted_kcal, level_weighted_kcal, kcal_per_hour_per_kg, timestamp
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    rows_inserted = 0
    for _, row in df.iterrows():
        record = tuple(None if pd.isna(row[col]) else row[col] for col in final_columns)
        cur.execute(insert_stmt, record)
        rows_inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {rows_inserted} new rows into Redshift.", flush=True)


@flow
def etl_flow():
    """
    Main Prefect flow:
      1) Download the CSV from S3.
      2) Retrieve the last timestamp from Redshift.
      3) Extract new documents from Mongo (where timestamp > last_ts).
      4) Merge Mongo data with CSV fields.
      5) Compute derived metrics.
      6) Insert new data into Redshift.
    If any task fails, the flow stops.
    """
    local_csv = download_csv_from_s3()
    last_ts = get_last_timestamp()
    mongo_df = extract_from_mongo(last_ts)
    merged_df = merge_csv_data(mongo_df, local_csv)
    derived_df = compute_derived_metrics(merged_df)
    load_to_redshift(derived_df)


if __name__ == "__main__":
    etl_flow()
