from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
import yfinance as yf


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    return hook.get_conn()


@task
def extract():
    symbols = ["AAPL", "GOOG"]
    frames = []

    for stock in symbols:
        df = yf.Ticker(stock).history(period="180d")
        if df.empty:
            continue

        df = df.reset_index().rename(
            columns={
                "Date": "DATE",
                "Open": "OPEN",
                "High": "HIGH",
                "Low": "LOW",
                "Close": "CLOSE",
                "Volume": "VOLUME",
            }
        )

        # Convert date to ISO timestamp for Snowflake TIMESTAMP_NTZ
        df["DATE"] = pd.to_datetime(df["DATE"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        df["VOLUME"] = df["VOLUME"].fillna(0).astype(int)
        for c in ["OPEN", "HIGH", "LOW", "CLOSE"]:
            df[c] = df[c].astype(float)
        df["SYMBOL"] = stock

        frames.append(df[["SYMBOL", "DATE", "OPEN", "CLOSE", "LOW", "HIGH", "VOLUME"]])

    if not frames:
        return {"records": []}

    out = pd.concat(frames, ignore_index=True)
    return {"records": out.to_dict(orient="records")}


@task
def transform(data):
    # Data already in final shape
    return data


@task
def load(data, target_table: str):
    records = data["records"]
    conn = return_snowflake_conn()

    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN")

            # Create or replace table matching your definition
            cur.execute(f"""
                CREATE OR REPLACE TABLE {target_table} (
                    SYMBOL VARCHAR NOT NULL,
                    DATE TIMESTAMP_NTZ(9) NOT NULL,
                    OPEN FLOAT,
                    CLOSE FLOAT,
                    LOW FLOAT,
                    HIGH FLOAT,
                    VOLUME NUMBER,
                    PRIMARY KEY (SYMBOL, DATE)
                )
            """)

            # Optional: clear table before insert
            cur.execute(f"DELETE FROM {target_table}")

            insert_sql = f"""
                INSERT INTO {target_table}
                (SYMBOL, DATE, OPEN, CLOSE, LOW, HIGH, VOLUME)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            params = [
                (
                    r["SYMBOL"],
                    r["DATE"],
                    r["OPEN"],
                    r["CLOSE"],
                    r["LOW"],
                    r["HIGH"],
                    r["VOLUME"],
                )
                for r in records
            ]

            if params:
                cur.executemany(insert_sql, params)

            cur.execute("COMMIT")
    except Exception:
        try:
            with conn.cursor() as cur:
                cur.execute("ROLLBACK")
        finally:
            conn.close()
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


with DAG(
    dag_id="StockPrice_Prediction_YFinance_Snowflake",
    start_date=datetime(2025, 10, 6),
    catchup=False,
    tags=["ETL"],
    schedule="0 2 * * *",
) as dag:
    target_table = "USER_DB_JACKAL.RAW.STOCK_DATA_LAB"

    extract_task = extract()
    transform_task = transform(extract_task)
    load_task = load(transform_task, target_table)

    extract_task >> transform_task >> load_task