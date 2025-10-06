from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    return hook.get_conn().cursor()

@task
def train(train_input_table, train_view, forecast_function_name):
    cur = get_snowflake_cursor()

    create_view_sql = f"""
        CREATE OR REPLACE VIEW {train_view} AS
        SELECT DATE, CLOSE, SYMBOL
        FROM {train_input_table}
        WHERE DATE >= DATEADD('day', -180, CURRENT_DATE());
    """

    create_model_sql = f"""
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
            INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
            SERIES_COLNAME => 'SYMBOL',
            TIMESTAMP_COLNAME => 'DATE',
            TARGET_COLNAME => 'CLOSE',
            CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
        );
    """

    cur.execute(create_view_sql)
    cur.execute(create_model_sql)
    cur.close()

@task
def predict(forecast_function_name, train_input_table, forecast_table, final_table):
    cur = get_snowflake_cursor()

    make_prediction_sql = f"""
    BEGIN
        CALL {forecast_function_name}!FORECAST(FORECASTING_PERIODS => 7);
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS
        SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;
    """

    create_final_table_sql = f"""
        CREATE OR REPLACE TABLE {final_table} AS
        SELECT 
            SYMBOL, 
            DATE, 
            CLOSE AS ACTUAL_CLOSING_PRICE, 
            NULL AS FORECASTED_CLOSING_PRICE,
            NULL AS LOWER_BOUND,
            NULL AS UPPER_BOUND
        FROM {train_input_table}
        UNION ALL
        SELECT 
            REPLACE(series, '"', '') AS SYMBOL,
            CAST(ts AS TIMESTAMP_NTZ(9)) AS DATE,
            NULL AS ACTUAL_CLOSING_PRICE,
            forecast AS FORECASTED_CLOSING_PRICE,
            lower_bound AS LOWER_BOUND,
            upper_bound AS UPPER_BOUND
        FROM {forecast_table};
    """

    cur.execute(make_prediction_sql)
    cur.execute(create_final_table_sql)
    cur.close()

with DAG(
    dag_id="ML_Forecasting_Prediction",
    start_date=datetime(2025, 10, 6),
    catchup=False,
    schedule="30 2 * * *",
    tags=["ML"],
) as dag:

    train_input_table      = Variable.get("train_input_table")      
    train_view             = Variable.get("train_view")             
    forecast_table         = Variable.get("forecast_table")         
    forecast_function_name = Variable.get("forecast_function_name") 
    final_table            = Variable.get("final_table")            

    train_task = train(train_input_table, train_view, forecast_function_name)
    predict_task = predict(forecast_function_name, train_input_table, forecast_table, final_table)

    train_task >> predict_task