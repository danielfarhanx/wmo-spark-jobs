import logging
import traceback
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import psycopg2

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
JOB_NAME = "EtlOracleMasterClassification"

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

TARGET_TABLE = f"{PG_SCHEMA}.m_classification"
# Oracle PK (TYPE, CTYPE) plus the distributor, since every schema has its own FCLASSNEW.
KEY_COLUMNS = ["dist_id", "channel_id", "classification_id"]
VALUE_COLUMNS = ["classification_name", "class_limit", "flag_aktif"]
# Oracle NUMBER and Postgres numeric arrive with different scales; compare on one type.
CLASS_LIMIT_TYPE = "decimal(38,10)"

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
# Mirrors FCLASSNEW per distributor: INSERT new, UPDATE changed, DELETE removed.
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
            # --- 5.1 Read from Oracle ---
            sql_query_oracle = f"""
            (
            SELECT
                (SELECT MEMOSTRING FROM {schema}.FMEMO WHERE memonama = 'KODEDIST') dist_id,
                TRIM(c.TYPE) channel_id,
                TRIM(c.CTYPE) classification_id,
                TRIM(c.CTYPENAME) classification_name,
                c.CLASSLIMIT class_limit,
                TRIM(c.FLAG_AKTIF) flag_aktif
            FROM {schema}.FCLASSNEW c
            ) temp_class_oracle
            """
            df_oracle = read_oracle(sql_query_oracle).select(
                col("DIST_ID").cast("int").alias("dist_id"),
                col("CHANNEL_ID").alias("channel_id"),
                col("CLASSIFICATION_ID").alias("classification_id"),
                col("CLASSIFICATION_NAME").alias("classification_name"),
                col("CLASS_LIMIT").cast(CLASS_LIMIT_TYPE).alias("class_limit"),
                col("FLAG_AKTIF").alias("flag_aktif"),
            ).cache()
            oracle_count = df_oracle.count()
            logger.info(f"Read {oracle_count} rows from Oracle.")

            # Without Oracle rows there is no dist_id to scope the delete pass, and an
            # empty read must never wipe a distributor's classes.
            if oracle_count == 0:
                logger.warning(f"FCLASSNEW is empty for schema {schema}. Skipping.")
                df_oracle.unpersist()
                continue

            dist_id = df_oracle.select("dist_id").first()["dist_id"]
            if dist_id is None:
                logger.error(f"No KODEDIST in {schema}.FMEMO. Skipping schema.")
                df_oracle.unpersist()
                continue

            # --- 5.2 Read from PostgreSQL ---
            # Scoped to this distributor: the full outer join below would otherwise mark
            # every other distributor's rows as DELETE.
            sql_query_pg = f"""
            (
            SELECT mc.dist_id, mc.channel_id, mc.classification_id,
                   mc.classification_name, mc.class_limit, mc.flag_aktif
            FROM {TARGET_TABLE} mc
            WHERE mc.dist_id = {dist_id}
            ) temp_class_pg
            """
            df_pg = read_postgres(sql_query_pg) \
                .withColumn("class_limit", col("class_limit").cast(CLASS_LIMIT_TYPE)) \
                .cache()
            logger.info(f"Read {df_pg.count()} rows from PostgreSQL.")

            # --- 5.3 Find changes ---
            key_match = None
            for c in KEY_COLUMNS:
                cond = col(f"orc.{c}") == col(f"pg.{c}")
                key_match = cond if key_match is None else key_match & cond

            value_changed = None
            for c in VALUE_COLUMNS:
                cond = ~col(f"orc.{c}").eqNullSafe(col(f"pg.{c}"))
                value_changed = cond if value_changed is None else value_changed | cond

            in_oracle = col("orc.dist_id").isNotNull()
            in_pg = col("pg.dist_id").isNotNull()

            df_changes = df_oracle.alias("orc").join(df_pg.alias("pg"), key_match, "full_outer") \
                .withColumn(
                    "operation_status",
                    F.when(in_oracle & ~in_pg, F.lit("INSERT"))
                     .when(~in_oracle & in_pg, F.lit("DELETE"))
                     .when(value_changed, F.lit("UPDATE"))
                ) \
                .where(col("operation_status").isNotNull()) \
                .select(
                    *[F.coalesce(col(f"orc.{c}"), col(f"pg.{c}")).alias(c) for c in KEY_COLUMNS],
                    *[col(f"orc.{c}").alias(c) for c in VALUE_COLUMNS],
                    col("operation_status"),
                )

            # A classification master is small, so it is written from the driver in one
            # transaction per schema.
            changes = df_changes.collect()
            df_oracle.unpersist()
            df_pg.unpersist()

            if not changes:
                logger.info(f"No changes detected for schema {schema}. Skipping database write.")
                continue

            # --- 5.4 Write to PostgreSQL ---
            write_changes(schema, changes)

        except Exception:
            logger.error(f"An unexpected error occurred while processing schema {schema}.")
            logger.error(traceback.format_exc())
            continue


def write_changes(schema, changes):
    conn_pg = None
    try:
        conn_pg = psycopg2.connect(
            dbname="mayora-online", user=POSTGRES_USER, password=POSTGRES_PASS,
            host=POSTGRES_HOST, port="5432"
        )
        insert_count, update_count, delete_count = 0, 0, 0
        with conn_pg.cursor() as cur:
            for row in changes:
                if row["operation_status"] == "INSERT":
                    cur.execute(f"""
                        INSERT INTO {TARGET_TABLE} (
                            dist_id, channel_id, classification_id,
                            classification_name, class_limit, flag_aktif, created_by
                        ) VALUES (%s, %s, %s, %s, %s, %s, 'SYS')
                    """, (
                        row["dist_id"], row["channel_id"], row["classification_id"],
                        row["classification_name"], row["class_limit"], row["flag_aktif"]
                    ))
                    insert_count += 1
                elif row["operation_status"] == "UPDATE":
                    cur.execute(f"""
                        UPDATE {TARGET_TABLE}
                        SET classification_name = %s, class_limit = %s, flag_aktif = %s,
                            modified_on = now(), modified_by = 'SYS'
                        WHERE dist_id = %s AND channel_id = %s AND classification_id = %s
                    """, (
                        row["classification_name"], row["class_limit"], row["flag_aktif"],
                        row["dist_id"], row["channel_id"], row["classification_id"]
                    ))
                    update_count += 1
                else:  # DELETE
                    cur.execute(f"""
                        DELETE FROM {TARGET_TABLE}
                        WHERE dist_id = %s AND channel_id = %s AND classification_id = %s
                    """, (row["dist_id"], row["channel_id"], row["classification_id"]))
                    delete_count += 1
        conn_pg.commit()
        logger.info(f"Write summary for schema {schema}: {insert_count} inserted, "
                    f"{update_count} updated, {delete_count} deleted.")
    except Exception:
        logger.error(f"Database write failed for schema {schema}. Rolling back transaction.")
        logger.error(traceback.format_exc())
        if conn_pg:
            conn_pg.rollback()
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
