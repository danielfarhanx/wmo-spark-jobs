import logging
import traceback
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import jaydebeapi

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
logger.info("Starting ETL Job: EtlOracleMasterItemMapping")

POSTGRES_DRIVER_PATH = "/opt/jars/postgresql-42.7.7.jar"
ORACLE_DRIVER_PATH = "/opt/jars/ojdbc8.jar"

try:
    spark = SparkSession.builder \
        .appName("EtlOracleMasterItemMapping") \
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

# --- PostgreSQL Connection Details (Source) ---
MYO_JDBC_URL = "jdbc:postgresql://" + sys.argv[4] #"ss-qa-api.mayora.co.id:5432/mayora-online"
MYO_DB_PROPERTIES = {
    "user": sys.argv[5], #"postgres",
    "password": sys.argv[6], #"postgres",
    "driver": "org.postgresql.Driver"
}

# Optional 9th arg: source PostgreSQL schema. Defaults to "myo" when omitted.
PG_SCHEMA = sys.argv[9] if len(sys.argv) > 9 else "myo"

def run_job_sku_penjatahan(oracle_url, oracle_user, oracle_password):
    """
    Main function to synchronize item mapping data from PostgreSQL to Oracle.
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
        logger.info(f"Fetching target distributor schemas from Oracle: {oracle_url}")
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
            # --- 4.1 Read from PostgreSQL Source ---
            logger.info("Reading item mapping data from PostgreSQL source.")
            sql_query_master_item = f"""
            (
            SELECT item_id, sku_id, priority
            FROM {PG_SCHEMA}.m_item_map
            ) temp_m_item_map
            """
            df_pg_myo_master_item = spark.read.jdbc(
                url=MYO_JDBC_URL,
                table=sql_query_master_item,
                properties=MYO_DB_PROPERTIES
            ).select(
                col("item_id"),
                col("sku_id").alias("pcode"),
                col("priority")
            )
            source_count = df_pg_myo_master_item.count()
            logger.info(f"Successfully read {source_count} rows from PostgreSQL source.")

            # --- 4.2 Read from Oracle Target ---
            logger.info(f"Reading item mapping data from Oracle target schema: {schema}.FMAPPING_SKU_GLOBAL")
            df_orc_master_item = spark.read.jdbc(
                url=ORACLE_JDBC_URL,
                table=f"{schema}.FMAPPING_SKU_GLOBAL",
                properties=ORACLE_DB_PROPERTIES
            )
            target_count = df_orc_master_item.count()
            logger.info(f"Successfully read {target_count} rows from Oracle target.")
            
            # --- 4.3 Join, Find Changes, and Prepare for Upsert/Delete ---
            logger.info("Identifying new, updated, and deleted records by joining source and target.")
            df_joined = df_pg_myo_master_item.alias("pg").join(
                df_orc_master_item.alias("orc"),
                ["item_id", "pcode"],
                "full_outer"
            ).where(
                # Records from PG that are new or changed
                (col("pg.item_id").isNotNull() & (
                    col("orc.item_id").isNull() |
                    (col("pg.priority") != col("orc.priority"))
                )) |
                # Records from Oracle that are no longer in PG
                (col("pg.item_id").isNull() & col("orc.item_id").isNotNull())
            ).select(
                F.when(col("pg.item_id").isNotNull(), col("pg.item_id")).otherwise(col("orc.item_id")).alias("item_id"),
                F.when(col("pg.pcode").isNotNull(), col("pg.pcode")).otherwise(col("orc.pcode")).alias("pcode"),
                col("pg.priority"), # Only need the new priority for inserts/updates
                F.when(col("pg.item_id").isNull(), F.lit("DELETE"))
                 .when(col("orc.item_id").isNull(), F.lit("INSERT"))
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
            
            # --- 4.4 Write to Oracle using JayDeBeApi ---
            logger.warning(f"⚠️ Collecting {total_to_process} records to the driver for row-by-row processing.")
            
            conn_oracle = None
            try:
                logger.info("Connecting to Oracle target using JayDeBeApi for write operation.")
                conn_oracle = jaydebeapi.connect(
                    "oracle.jdbc.driver.OracleDriver",
                    ORACLE_JDBC_URL,
                    {'user': oracle_user, 'password': oracle_password, "oracle.jdbc.timezoneAsRegion": "false"},
                    jars=ORACLE_DRIVER_PATH
                )
                conn_oracle.jconn.setAutoCommit(False)
                cur = conn_oracle.cursor()
                logger.info("Oracle connection successful. Starting transaction.")
                
                insert_success, update_success, delete_success = 0, 0, 0

                for row in df_joined.collect():
                    if row["operation_status"] == "INSERT":
                        cur.execute(f"""
                            INSERT INTO {schema}.FMAPPING_SKU_GLOBAL(ITEM_ID, PCODE, PRIORITY)
                            VALUES (?, ?, ?)
                        """, (row['item_id'], row['pcode'], row['priority']))
                        insert_success += 1
                    elif row["operation_status"] == "UPDATE":
                        cur.execute(f"""
                            UPDATE {schema}.FMAPPING_SKU_GLOBAL SET priority = ?
                            WHERE item_id = ? AND pcode = ?
                        """, (row['priority'], row['item_id'], row['pcode']))
                        update_success += 1
                    else: # DELETE
                        cur.execute(f"""
                            DELETE FROM {schema}.FMAPPING_SKU_GLOBAL
                            WHERE item_id = ? AND pcode = ?
                        """, (row['item_id'], row['pcode']))
                        delete_success += 1
                
                conn_oracle.commit()
                logger.info("✅ Database transaction committed successfully.")
                logger.info(f"Write summary for schema {schema}: {insert_success} inserted, {update_success} updated, {delete_success} deleted.")

            except Exception as db_err:
                 logger.error(f"❌ Database write failed for schema {schema}. Rolling back transaction.")
                 logger.error(traceback.format_exc())
                 if conn_oracle:
                     conn_oracle.rollback()
            finally:
                if conn_oracle:
                    cur.close()
                    conn_oracle.close()
                    logger.info("Oracle connection closed.")

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
