import logging
import traceback
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
from pyspark.sql import functions as F
import psycopg2
from psycopg2.extras import execute_values

# ==============================================================================
# 1. SETUP LOGGING & ENVIRONMENT
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# ==============================================================================
# 2. ARGUMENTS
# Same order as the other jobs, so the DAG's spark-submit line can be copied:
#   1 oracle_url   2 oracle_user   3 oracle_password
#   4 pg_host:port/db   5 pg_user   6 pg_password
#   7 oracle schemas, pre-quoted for IN ('...'), e.g. "USERMYR38','USERMYR34"
#   8 pg_host (for psycopg2)   9 pg_schema (optional, default "myo")
# ==============================================================================
JOB_NAME = "EtlOracleKplToTasklist"

ORACLE_URL = sys.argv[1]
ORACLE_USER = sys.argv[2]
ORACLE_PASSWORD = sys.argv[3]

POSTGRES_URL = sys.argv[4]
POSTGRES_USER = sys.argv[5]
POSTGRES_PASS = sys.argv[6]
ORACLE_SCHEMAS = sys.argv[7]
POSTGRES_HOST = sys.argv[8]
PG_SCHEMA = sys.argv[9] if len(sys.argv) > 9 else "myo"

ORACLE_JDBC_URL = "jdbc:oracle:thin:@//" + ORACLE_URL
ORACLE_DB_PROPERTIES = {
    "user": ORACLE_USER,
    "password": ORACLE_PASSWORD,
    "driver": "oracle.jdbc.driver.OracleDriver",
    "fetchSize": "10000"
}

MYO_JDBC_URL = "jdbc:postgresql://" + POSTGRES_URL
MYO_DB_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASS,
    "driver": "org.postgresql.Driver"
}

# tasklist_uuid, created_at and the call rollups are filled by column defaults.
TARGET_TABLE = f"{PG_SCHEMA}.t_tasklist_h"
TARGET_COLUMNS = ["tasklist_date", "outlet_id", "user_id", "created_by"]
# Matches the partial unique index t_tasklist_h_uq (tasklist_date, outlet_id) WHERE deleted_at IS NULL
CONFLICT_COLUMNS = ["tasklist_date", "outlet_id"]

# TODO: resolve the real agent per salesman (m_user_vs_distributor -> m_user in MongoDB)
DEFAULT_USER_ID = "telesales1"

# ==============================================================================
# 3. SPARK SESSION
# The JDBC jars come from the DAG (--conf spark.driver/executor.extraClassPath),
# so they are not configured here.
# ==============================================================================
logger.info(f"Starting ETL Job: {JOB_NAME}")
try:
    spark = SparkSession.builder \
        .appName(JOB_NAME) \
        .config("spark.driver.extraJavaOptions", "-Doracle.jdbc.timezoneAsRegion=false") \
        .config("spark.executor.extraJavaOptions", "-Doracle.jdbc.timezoneAsRegion=false") \
        .getOrCreate()
    logger.info("SparkSession configured and created successfully.")
except Exception:
    logger.error("Fatal: Failed to create SparkSession.")
    logger.error(traceback.format_exc())
    sys.exit(1)


# ==============================================================================
# 4. READ HELPERS
# The query must be wrapped as "(SELECT ...) alias" so it runs inside the database
# and only the result comes over the wire.
# ==============================================================================
def read_oracle(query):
    return spark.read.jdbc(url=ORACLE_JDBC_URL, table=query, properties=ORACLE_DB_PROPERTIES)


def read_postgres(query):
    return spark.read.jdbc(url=MYO_JDBC_URL, table=query, properties=MYO_DB_PROPERTIES)


def get_oracle_schemas():
    query = f"(SELECT username FROM sys.dba_users WHERE username IN ('{ORACLE_SCHEMAS}')) temp"
    return [row['USERNAME'] for row in read_oracle(query).collect()]


# ==============================================================================
# 5. JOB
# ==============================================================================
def run_job():
    try:
        schemas = get_oracle_schemas()
        logger.info(f"Found {len(schemas)} schemas to process: {schemas}")
    except Exception:
        logger.error("Failed to fetch schema list from Oracle. Aborting job.")
        logger.error(traceback.format_exc())
        return

    for schema in schemas:
        logger.info(f"================== Processing schema: {schema} ==================")
        try:
            # --- 5.1 Read KPL from Oracle ---
            # DATA1 is an Oracle DATE (carries a time); sent as text so the JVM/session
            # time zone cannot shift it to another day.
            # TESTING: all dates are read. For production, restore the today-only filter:
            #   WHERE f.DATA1 >= TRUNC(SYSDATE) AND f.DATA1 < TRUNC(SYSDATE) + 1
            sql_query_oracle = f"""
            (
            SELECT
                (SELECT MEMOSTRING FROM {schema}.FMEMO WHERE memonama = 'KODEDIST') dist_id,
                TO_CHAR(f.DATA1, 'YYYY-MM-DD') call_date,
                TRIM(f.DATA2) sls_no,
                TRIM(f.DATA3) cust_no
            FROM {schema}.FTABEL46 f
            ) temp_kpl_oracle
            """
            df_oracle = read_oracle(sql_query_oracle).select(
                col("DIST_ID").cast("int").alias("dist_id"),
                F.to_date(col("CALL_DATE")).alias("call_date"),
                col("SLS_NO").alias("sls_id"),
                col("CUST_NO").alias("cust_no"),
            ).cache()
            oracle_count = df_oracle.count()
            logger.info(f"Read {oracle_count} rows from Oracle.")
            if oracle_count == 0:
                df_oracle.unpersist()
                continue

            # --- 5.2 Read from PostgreSQL ---
            # Filter by the distributor just read, so only this schema's rows come over.
            dist_id = df_oracle.select("dist_id").first()["dist_id"]

            # Salesmen handled by the call centre. Inner-joined below, so KPL rows of
            # unmapped salesmen get no task.
            sql_query_user = f"""
            (
            SELECT muvd.dist_id, muvd.sls_id
            FROM {PG_SCHEMA}.m_user_vs_distributor muvd
            WHERE muvd.dist_id = {dist_id} AND muvd.is_active = true
            ) temp_user_vs_dist_pg
            """
            df_user = read_postgres(sql_query_user).distinct().cache()
            logger.info(f"Read {df_user.count()} salesman mappings from PostgreSQL.")

            # t_tasklist_h.outlet_id holds m_outlet.id, not the customer code.
            # m_outlet.outlet_id (= CUSTNO) is only unique per dist_id.
            # Blocklisted outlets are included on purpose.
            sql_query_outlet = f"""
            (
            SELECT mo.dist_id, mo.outlet_id AS cust_no, CAST(mo.id AS text) AS outlet_id
            FROM {PG_SCHEMA}.m_outlet mo
            WHERE mo.dist_id = {dist_id}
            ) temp_outlet_pg
            """
            df_outlet = read_postgres(sql_query_outlet).cache()
            logger.info(f"Read {df_outlet.count()} outlets from PostgreSQL.")

            # --- 5.3 Process ---
            # One task per outlet per day: the KPL key includes the salesman, so the
            # same customer can appear twice. ON CONFLICT cannot touch a row twice in
            # one statement, hence the dropDuplicates.
            df_result = df_oracle \
                .join(broadcast(df_user), on=["dist_id", "sls_id"], how="inner") \
                .join(broadcast(df_outlet), on=["dist_id", "cust_no"], how="inner") \
                .select(
                    col("call_date").alias("tasklist_date"),
                    col("outlet_id"),
                    F.lit(DEFAULT_USER_ID).alias("user_id"),
                    F.lit("SYS").alias("created_by"),
                ) \
                .dropDuplicates(CONFLICT_COLUMNS) \
                .select(*TARGET_COLUMNS) \
                .cache()
            result_count = df_result.count()
            logger.info(f"{result_count} tasks to write; {oracle_count - result_count} KPL rows "
                        f"dropped (unmapped salesman, outlet not in m_outlet, or duplicate outlet).")

            # --- 5.4 Write to PostgreSQL ---
            # Insert-only: an existing task for the same day/outlet is left untouched,
            # so re-runs never reset call_count / call_duration / no_call_reason_id.
            if result_count > 0:
                logger.info("Starting distributed write to PostgreSQL.")
                df_result.repartition(4).foreachPartition(process_partition)
                logger.info("Distributed write complete.")

            df_result.unpersist()
            df_oracle.unpersist()
            df_user.unpersist()
            df_outlet.unpersist()

        except Exception:
            logger.error(f"An unexpected error occurred while processing schema {schema}.")
            logger.error(traceback.format_exc())
            continue


def build_insert_sql():
    cols = ", ".join(TARGET_COLUMNS)
    conflict = ", ".join(CONFLICT_COLUMNS)
    # The unique index is partial, so ON CONFLICT must repeat its WHERE to match it.
    return f"""
        INSERT INTO {TARGET_TABLE} ({cols}) VALUES %s
        ON CONFLICT ({conflict}) WHERE deleted_at IS NULL DO NOTHING
    """


def process_partition(iterator):
    # Runs on the EXECUTOR: connections are not serializable, so open one here.
    rows = [tuple(row[c] for c in TARGET_COLUMNS) for row in iterator]
    if not rows:
        return

    conn_pg = None
    try:
        conn_pg = psycopg2.connect(
            dbname="mayora-online", user=POSTGRES_USER, password=POSTGRES_PASS,
            host=POSTGRES_HOST, port="5432"
        )
        with conn_pg.cursor() as cur:
            execute_values(cur, build_insert_sql(), rows, page_size=1000)
        conn_pg.commit()
    except Exception:
        if conn_pg:
            conn_pg.rollback()
        raise
    finally:
        if conn_pg:
            conn_pg.close()


# ==============================================================================
# 6. JOB EXECUTION
# ==============================================================================
if __name__ == "__main__":
    run_job()

    logger.info("ETL job has finished all tasks. Shutting down Spark.")
    spark.stop()
    logger.info("SparkSession stopped.")
