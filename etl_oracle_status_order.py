from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
from pyspark.sql import functions as F
import psycopg2 as psycopg2
import requests, os, sys
import logging

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("EtlOracleStatusOutlet")

POSTGRES_DRIVER_PATH = "/opt/jars/postgresql-42.7.7.jar"
ORACLE_DRIVER_PATH = "/opt/jars/ojdbc8.jar"
spark = SparkSession.builder \
    .appName("EtlOracleStatusOutlet") \
    .config("spark.driver.extraClassPath", f"{POSTGRES_DRIVER_PATH}:{ORACLE_DRIVER_PATH}") \
    .config("spark.driver.extraJavaOptions", "-Doracle.jdbc.timezoneAsRegion=false")  \
    .config("spark.executor.extraJavaOptions", "-Doracle.jdbc.timezoneAsRegion=false")  \
    .config("spark.network.timeout", "1000s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.sql.broadcastTimeout", "1000s") \
    .getOrCreate()

MYO_JDBC_URL = "jdbc:postgresql://" + sys.argv[4] #"ss-qa-api.mayora.co.id:5432/mayora-online"
MYO_DB_PROPERTIES = {
    "user": sys.argv[5], #"postgres",
    "password": sys.argv[6], #"postgres",
    "driver": "org.postgresql.Driver"
}

# Optional 9th arg: target PostgreSQL schema. Defaults to "myo" when omitted.
PG_SCHEMA = sys.argv[9] if len(sys.argv) > 9 else "myo"

def run_job_status_order(oracle_url, oracle_user, oracle_password):
    ORACLE_JDBC_URL = "jdbc:oracle:thin:@//"+oracle_url
    ORACLE_DB_PROPERTIES = {
        "user": oracle_user,
        "password": oracle_password,
        "driver": "oracle.jdbc.driver.OracleDriver",
        "fetchSize": "10000"
    }
    dba_query = f"(SELECT username FROM sys.dba_users WHERE username in ('{sys.argv[7]}')) temp" #USERMYR
    try:
        logger.info("Reading schema list from Oracle DBA_USERS")
        df_schema = spark.read.jdbc(
            url=ORACLE_JDBC_URL,
            table=dba_query,
            properties=ORACLE_DB_PROPERTIES
        )
        schemas = [row['USERNAME'] for row in df_schema.collect()]
        logger.info(f"Found schemas: {schemas}")
    except Exception as e:
        logger.error(f"Failed to read schema list: {e}")
        return

    for schema in schemas:
        try:
            logger.info(f"Loading data from schema {schema}.FORDER_ONLINE_H")
            df_orc_forder_online_h = spark.read.jdbc(
                url=ORACLE_JDBC_URL,
                table=schema+".FORDER_ONLINE_H",
                properties=ORACLE_DB_PROPERTIES
            )
            df_orc_forder_online_h = df_orc_forder_online_h.select(
                col("ORDERNO").cast("int").alias("order_no"),
                col("ONLINE_STATUS").alias("status"),
                col("MODIFIED_ON").alias("modified_on"),
                col("MODIFIED_BY").alias("modified_by")
            )
            logger.info(f"Loaded data from schema {schema}")

            logger.info(f"Reading PostgreSQL table {PG_SCHEMA}.t_order_h")
            df_pg_myo_order_h = spark.read.jdbc(
                url=MYO_JDBC_URL,
                table=f"{PG_SCHEMA}.t_order_h",
                properties=MYO_DB_PROPERTIES
            )

            logger.info("Joining Oracle and PostgreSQL dataframes")
            df_joined = df_orc_forder_online_h.alias("orc")\
                .join(
                    df_pg_myo_order_h.alias("pg_toh"),
                    "order_no",
                    "inner"
                )\
                .where(
                    (df_orc_forder_online_h.status != df_pg_myo_order_h.status)
                ).select(
                    col("orc.order_no").alias("order_no"),
                    col("orc.status").alias("status"),
                    col("orc.modified_on").alias("modified_on"),
                    col("orc.modified_by").alias("modified_by")
                )

            update_count = df_joined.count()
            logger.info(f"Data transformed from schema {schema} to UPDATE: {update_count}")

            logger.info("Connecting to PostgreSQL for updates")
            conn_pg = psycopg2.connect(
                dbname="mayora-online",
                user=sys.argv[5],
                password=sys.argv[6],
                host=sys.argv[8],
                port="5432"
            )
            cur = conn_pg.cursor()
            for row in df_joined.collect():
                cur.execute(f"""
                UPDATE {PG_SCHEMA}.t_order_h
                    SET status = %s,
                        modified_on = %s, 
                        modified_by = %s
                WHERE order_no = %s
                """,(
                    row['status'],
                    row['modified_on'],
                    row['modified_by'],
                    row['order_no'],
                ))
            conn_pg.commit()
            cur.close()
            conn_pg.close()
            logger.info(f"Successfully updated {update_count} rows in PostgreSQL for schema {schema}")

        except Exception as e:
            logger.error(f"Failed on schema {schema}: {e}")

if __name__ == "__main__":
    ORACLE_URL = sys.argv[1] #"192.168.8.249:1521/DBMYR"
    ORACLE_USER = sys.argv[2] #"system"
    ORACLE_PASSWORD = sys.argv[3] #"ditributionexcellentKM18"

    logger.info("Starting ETL job for Oracle Status Order")
    run_job_status_order(ORACLE_URL, ORACLE_USER, ORACLE_PASSWORD)
    logger.info("ETL job completed, stopping Spark session")
    spark.stop()
