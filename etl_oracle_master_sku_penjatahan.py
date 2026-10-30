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

# Configure a standard logger for clear, timestamped output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# ==============================================================================
# 2. SPARK SESSION & DATABASE CONFIGURATION
# ==============================================================================
logger.info("Starting ETL Job: EtlOracleMasterSkuPenjatahan")

POSTGRES_DRIVER_PATH = "/opt/jars/postgresql-42.7.7.jar"
ORACLE_DRIVER_PATH = "/opt/jars/ojdbc8.jar"

try:
    spark = SparkSession.builder \
        .appName("EtlOracleMasterSkuPenjatahan") \
        .config("spark.driver.extraClassPath", f"{POSTGRES_DRIVER_PATH}:{ORACLE_DRIVER_PATH}") \
        .config("spark.driver.extraJavaOptions", "-Doracle.jdbc.timezoneAsRegion=false")  \
        .config("spark.executor.extraJavaOptions", "-Doracle.jdbc.timezoneAsRegion=false")  \
        .config("spark.network.timeout", "1000s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.sql.broadcastTimeout", "1000s") \
        .getOrCreate()
    
    logger.info("SparkSession configured and created successfully.")

except Exception as e:
    logger.error("Fatal: Failed to create SparkSession.")
    logger.error(traceback.format_exc())
    sys.exit(1)

# --- PostgreSQL Connection Details (Target) ---
MYO_JDBC_URL = "jdbc:postgresql://" + sys.argv[4] #"ss-qa-api.mayora.co.id:5432/mayora-online"
MYO_DB_PROPERTIES = {
    "user": sys.argv[5], #"postgres",
    "password": sys.argv[6], #"postgres",
    "driver": "org.postgresql.Driver"
}

# Optional 9th arg: target PostgreSQL schema. Defaults to "myo" when omitted.
PG_SCHEMA = sys.argv[9] if len(sys.argv) > 9 else "myo"

def run_job_sku_penjatahan(oracle_url, oracle_user, oracle_password):
    """
    Main function to synchronize SKU parameter data from Oracle to PostgreSQL.
    """
    ORACLE_JDBC_URL = "jdbc:oracle:thin:@//" + oracle_url
    ORACLE_DB_PROPERTIES = {
        "user": oracle_user,
        "password": oracle_password,
        "driver": "oracle.jdbc.driver.OracleDriver",
        "fetchSize": "10000"
    }

    # ==============================================================================
    # 3. GET SCHEMA LIST FROM ORACLE
    # ==============================================================================
    try:
        logger.info(f"Fetching source distributor schemas from Oracle: {oracle_url}")
        dba_query = f"(SELECT username FROM sys.dba_users WHERE username in ('{sys.argv[7]}')) temp" #USERMYR
        df_schema = spark.read.jdbc(url=ORACLE_JDBC_URL, table=dba_query, properties=ORACLE_DB_PROPERTIES)
        schemas = [row['USERNAME'] for row in df_schema.collect()]
        logger.info(f"Found {len(schemas)} schemas to process: {schemas}")
    except Exception as e:
        logger.error("Failed to fetch schema list from Oracle. Aborting job.")
        logger.error(traceback.format_exc())
        return

    # ==============================================================================
    # 4. PROCESS EACH SCHEMA
    # ==============================================================================
    for schema in schemas:
        logger.info(f"================== Processing schema: {schema} ==================")
        try:
            # --- 4.1 Read from Oracle Source ---
            logger.info(f"Reading SKU parameter data from Oracle schema: {schema}")
            sql_query_sku_penjatahan = f"""
            (
            SELECT
                a.dist_id, a.pcode,
                COALESCE(MAX(is_penjatahan), 0) is_penjatahan,
                COALESCE(MAX(is_last_bite), 0) is_last_bite
            FROM(
                SELECT DISTINCT
                    (SELECT MEMOSTRING FROM {schema}.FMEMO WHERE memonama = 'KODEDIST') dist_id,
                    PCODE, 1 is_penjatahan, NULL is_last_bite
                FROM {schema}.FPENJATAHANSTOK_D
                WHERE SLSNO in (
                    SELECT distinct f.SLSNO FROM {schema}.FCUSTSLS f 
                    JOIN {schema}.FCUSTMST f2 ON f.CUSTNO = f2.CUSTNO 
                    JOIN {schema}.FTYPEOUT f3 ON f3.TYPE = f2.TYPEOUT AND f3.TYPENAME LIKE '%GROSIR%'
                )
                UNION ALL
                SELECT DISTINCT
                    (SELECT MEMOSTRING FROM {schema}.FMEMO WHERE memonama = 'KODEDIST') dist_id,
                    fd.PCODE, NULL is_penjatahan, 1 is_last_bite
                FROM {schema}.FKUOTAJUAL_H fh
                JOIN {schema}.FKUOTAJUAL_D3 fd ON fh.APNO = fd.APNO
                WHERE (TO_NUMBER(YEARFROM) * 100 + TO_NUMBER(WEEKFROM)) <= (SELECT TAHUN * 100 + WEEKNO FROM {schema}.FCYCLE3 f WHERE trunc(CDATE) = (SELECT trunc(CURRENT_DATE) FROM DUAL)) 
                AND (TO_NUMBER(YEARTO) * 100 + TO_NUMBER(WEEKTO)) >= (SELECT TAHUN * 100 + WEEKNO FROM {schema}.FCYCLE3 f WHERE trunc(CDATE) = (SELECT trunc(CURRENT_DATE) FROM DUAL))
            ) a
            GROUP BY a.dist_id, a.pcode
            ) temp_sku_penjatahan
            """
            df_orc_sku_penjatahan = spark.read.jdbc(
                url=ORACLE_JDBC_URL,
                table=sql_query_sku_penjatahan,
                properties=ORACLE_DB_PROPERTIES
            ).select(
                col("dist_id").cast("int").alias("dist_id"),
                col("pcode").alias("pcode_id"),
                F.when(col("is_penjatahan") == 1, F.lit(True)).otherwise(False).alias("is_penjatahan"),
                F.when(col("is_last_bite") == 1, F.lit(True)).otherwise(False).alias("is_last_bite"),
                F.current_timestamp().alias("created_on"), F.lit("SYS").alias("created_by"),
                F.lit(None).alias("modified_on"), F.lit(None).alias("modified_by")
            )
            source_count = df_orc_sku_penjatahan.count()
            logger.info(f"Successfully read {source_count} rows from Oracle source.")

            # --- 4.2 Read from PostgreSQL Target ---
            logger.info("Reading SKU parameter data from PostgreSQL target table.")
            df_pg_myo_sku_penjatahan = spark.read.jdbc(
                url=MYO_JDBC_URL,
                table=f"{PG_SCHEMA}.m_sku_parameter",
                properties=MYO_DB_PROPERTIES
            )
            logger.info(f"Successfully read {df_pg_myo_sku_penjatahan.count()} rows from PostgreSQL target.")

            # --- 4.3 Join, Find Changes, and Prepare for Upsert/Delete ---
            logger.info("Identifying new, updated, and deleted records by joining source and target.")
            df_joined = df_orc_sku_penjatahan.alias("orc").join(
                df_pg_myo_sku_penjatahan.alias("pg_msp"),
                (col("orc.dist_id") == col("pg_msp.dist_id")) & (col("orc.pcode_id") == col("pg_msp.pcode_id")),
                "full_outer"
            ).where(
                (col("orc.dist_id").isNotNull() & (
                    col("pg_msp.id").isNull() |
                    (col("orc.is_penjatahan") != col("pg_msp.is_penjatahan")) |
                    (col("orc.is_last_bite") != col("pg_msp.is_last_bite"))
                )) |
                (col("orc.dist_id").isNull() & col("pg_msp.dist_id").isNotNull())
            ).select(
                F.when(col("orc.dist_id").isNotNull(), col("orc.dist_id")).otherwise(col("pg_msp.dist_id")).alias("dist_id"),
                F.when(col("orc.pcode_id").isNotNull(), col("orc.pcode_id")).otherwise(col("pg_msp.pcode_id")).alias("pcode_id"),
                col("orc.is_penjatahan"),
                col("orc.is_last_bite"),
                F.when(col("pg_msp.id").isNull(), col("orc.created_on")).otherwise(col("pg_msp.created_on")).alias("created_on"),
                F.when(col("pg_msp.id").isNull(), col("orc.created_by")).otherwise(col("pg_msp.created_by")).alias("created_by"),
                F.when(col("orc.dist_id").isNull() | col("pg_msp.id").isNotNull(), F.current_timestamp()).alias("modified_on"),
                F.when(col("orc.dist_id").isNull() | col("pg_msp.id").isNotNull(), F.lit("SYS")).alias("modified_by"),
                F.when(col("orc.dist_id").isNull(), F.lit("DELETE"))
                 .when(col("pg_msp.id").isNull(), F.lit("INSERT"))
                 .otherwise(F.lit("UPDATE")).alias("operation_status")
            )
            
            # Cache before multiple actions
            df_joined.cache()
            
            insert_count = df_joined.where(col("operation_status") == "INSERT").count()
            update_count = df_joined.where(col("operation_status") == "UPDATE").count()
            delete_count = df_joined.where(col("operation_status") == "DELETE").count()
            
            logger.info(f"Transformation complete. Found {insert_count} to INSERT, {update_count} to UPDATE, and {delete_count} to DELETE.")
            
            total_to_process = insert_count + update_count + delete_count
            if total_to_process == 0:
                logger.info(f"No changes detected for schema {schema}. Skipping database write.")
                df_joined.unpersist()
                continue
            
            # --- 4.4 Write to PostgreSQL ---
            logger.warning(f"⚠️ Collecting {total_to_process} records to the driver for row-by-row processing.")
            
            conn_pg = None
            try:
                logger.info("Connecting to PostgreSQL target using psycopg2 for write operation.")
                conn_pg = psycopg2.connect(
                    dbname="mayora-online", user=sys.argv[5], password=sys.argv[6],
                    host=sys.argv[8], port="5432"
                )
                cur = conn_pg.cursor()
                logger.info("PostgreSQL connection successful. Starting transaction.")
                
                insert_success, update_success, delete_success = 0, 0, 0

                for row in df_joined.collect():
                    if row["operation_status"] == "INSERT":
                        cur.execute(f"""
                            INSERT INTO {PG_SCHEMA}.m_sku_parameter(
                                dist_id, pcode_id, is_penjatahan, is_last_bite, created_on, created_by
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                        """, (
                            row['dist_id'], row['pcode_id'], row['is_penjatahan'],
                            row['is_last_bite'], row['created_on'], row['created_by']
                        ))
                        insert_success += 1
                    elif row["operation_status"] == "UPDATE":
                        cur.execute(f"""
                            UPDATE {PG_SCHEMA}.m_sku_parameter
                            SET is_penjatahan = %s, is_last_bite = %s, modified_on = %s, modified_by = %s
                            WHERE dist_id = %s and pcode_id = %s
                        """, (
                            row['is_penjatahan'], row['is_last_bite'], row['modified_on'],
                            row['modified_by'], row['dist_id'], row['pcode_id']
                        ))
                        update_success += 1
                    else: # DELETE
                        cur.execute(f"""
                            DELETE FROM {PG_SCHEMA}.m_sku_parameter
                            WHERE dist_id = %s and pcode_id = %s
                        """, (row['dist_id'], row['pcode_id']))
                        delete_success += 1
                
                conn_pg.commit()
                logger.info("✅ Database transaction committed successfully.")
                logger.info(f"Write summary for schema {schema}: {insert_success} inserted, {update_success} updated, {delete_success} deleted.")

            except Exception as db_err:
                 logger.error(f"❌ Database write failed for schema {schema}. Rolling back transaction.")
                 logger.error(traceback.format_exc())
                 if conn_pg:
                     conn_pg.rollback()
            finally:
                if conn_pg:
                    cur.close()
                    conn_pg.close()
                    logger.info("PostgreSQL connection closed.")

            df_joined.unpersist()

        except Exception as e:
            logger.error(f"An unexpected error occurred while processing schema {schema}. See details below.")
            logger.error(traceback.format_exc())
            continue # Continue to the next schema

# ==============================================================================
# 5. JOB EXECUTION
# ==============================================================================
if __name__ == "__main__":
    ORACLE_URL = sys.argv[1] #"192.168.8.249:1521/DBMYR"
    ORACLE_USER = sys.argv[2] #"system"
    ORACLE_PASSWORD = sys.argv[3] #"ditributionexcellentKM18"
    
    run_job_sku_penjatahan(ORACLE_URL, ORACLE_USER, ORACLE_PASSWORD)
    
    logger.info("ETL job has finished all tasks. Shutting down Spark.")
    spark.stop()
    logger.info("SparkSession stopped.")
