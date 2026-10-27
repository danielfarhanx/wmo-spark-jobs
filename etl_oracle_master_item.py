import logging
import traceback
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import jaydebeapi
import jpype
import jpype.imports
from jpype.types import JLong

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
logger.info("Starting ETL Job: EtlOracleMasterItem")

POSTGRES_DRIVER_PATH = "/opt/jars/postgresql-42.7.7.jar"
ORACLE_DRIVER_PATH = "/opt/jars/ojdbc8.jar"

try:
    spark = SparkSession.builder \
        .appName("EtlOracleMasterItem") \
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
MYO_JDBC_URL = "jdbc:postgresql://ss-qa-api.mayora.co.id:5432/mayora-online"
MYO_DB_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

def run_job_sku_penjatahan(oracle_url, oracle_user, oracle_password):
    """
    Main function to synchronize master item data from PostgreSQL to Oracle.
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
        dba_query = "(SELECT username FROM sys.dba_users WHERE username LIKE '%USERMYR17%') temp"
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
            logger.info("Reading master item data from PostgreSQL source.")
            sql_query_master_item = """
            (
            SELECT
                msp.id::text item_id, mi.item_name,
                unit1, unit2, unit3,
                conv2 convunit2, conv3 convunit3,
                length carton_length, width carton_width, height carton_height,
                weight3 weight_netto, weight_bruto,
                sellprice1, sellprice2, sellprice3,
                '1' flag_active, subbrand_id matgr
            FROM mygotesting.m_sku_product msp
            JOIN mygotesting.m_item mi ON msp.id = mi.id
            ) temp_m_item
            """
            df_pg_myo_master_item = spark.read.jdbc(
                url=MYO_JDBC_URL,
                table=sql_query_master_item,
                properties=MYO_DB_PROPERTIES
            ).select(
                col("item_id"), col("item_name"), col("unit1"), col("unit2"), col("unit3"),
                col("convunit2"), col("convunit3"), col("carton_length"), col("carton_width"),
                col("carton_height"), col("weight_netto"), col("weight_bruto"), col("sellprice1"),
                col("sellprice2"), col("sellprice3"), col("flag_active"), col("matgr"),
                F.current_timestamp().alias("created_on"), F.lit("SYS").alias("created_by"),
                F.lit(None).alias("modified_on"), F.lit(None).alias("modified_by")
            )
            logger.info(f"Successfully read {df_pg_myo_master_item.count()} rows from PostgreSQL source.")

            # --- 4.2 Read from Oracle Target ---
            logger.info(f"Reading master item data from Oracle target schema: {schema}.FMASTER_ITEM")
            df_orc_master_item = spark.read.jdbc(
                url=ORACLE_JDBC_URL,
                table=f"{schema}.FMASTER_ITEM",
                properties=ORACLE_DB_PROPERTIES
            )
            logger.info(f"Successfully read {df_orc_master_item.count()} rows from Oracle target.")
            
            # --- 4.3 Join, Find Changes, and Prepare for Upsert/Delete ---
            logger.info("Identifying new, updated, and deleted records by joining source and target.")
            df_joined = df_pg_myo_master_item.alias("pg").join(
                df_orc_master_item.alias("orc"), "item_id", "full_outer"
            ).where(
                (col("pg.item_id").isNotNull() & (
                    col("orc.item_id").isNull() |
                    (col("pg.item_name") != col("orc.item_name")) |
                    (col("pg.unit1") != col("orc.unit1")) |
                    (col("pg.unit2") != col("orc.unit2")) |
                    (col("pg.unit3") != col("orc.unit3")) |
                    (col("pg.convunit2") != col("orc.convunit2")) |
                    (col("pg.convunit3") != col("orc.convunit3")) |
                    (col("pg.carton_length") != col("orc.carton_length")) |
                    (col("pg.carton_width") != col("orc.carton_width")) |
                    (col("pg.carton_height") != col("orc.carton_height")) |
                    (col("pg.weight_netto") != col("orc.weight_netto")) |
                    (col("pg.weight_bruto") != col("orc.weight_bruto")) |
                    (col("pg.sellprice1") != col("orc.sellprice1")) |
                    (col("pg.sellprice2") != col("orc.sellprice2")) |
                    (col("pg.sellprice3") != col("orc.sellprice3")) |
                    (col("pg.flag_active") != col("orc.flag_active")) |
                    (col("pg.matgr") != col("orc.matgr"))
                )) |
                (col("pg.item_id").isNull() & col("orc.item_id").isNotNull() & (col("orc.flag_active") == '1'))
            ).select(
                F.when(col("pg.item_id").isNotNull(), col("pg.item_id")).otherwise(col("orc.item_id")).alias("item_id"),
                col("pg.item_name"), col("pg.unit1"), col("pg.unit2"), col("pg.unit3"),
                col("pg.convunit2"), col("pg.convunit3"), col("pg.carton_length"), col("pg.carton_width"),
                col("pg.carton_height"), col("pg.weight_netto"), col("pg.weight_bruto"), col("pg.sellprice1"),
                col("pg.sellprice2"), col("pg.sellprice3"), col("pg.flag_active"), col("pg.matgr"),
                F.when(col("orc.item_id").isNull(), col("pg.created_on")).otherwise(col("orc.created_on")).alias("created_on"),
                F.when(col("orc.item_id").isNull(), col("pg.created_by")).otherwise(col("orc.created_by")).alias("created_by"),
                F.when(col("pg.item_id").isNull(), F.current_timestamp()).otherwise(F.current_timestamp()).alias("modified_on"),
                F.when(col("pg.item_id").isNull(), F.lit("SYS")).otherwise(F.lit("SYS")).alias("modified_by"),
                F.when(col("pg.item_id").isNull(), F.lit("DELETE"))
                 .when(col("orc.item_id").isNull(), F.lit("INSERT"))
                 .otherwise(F.lit("UPDATE")).alias("operation_status")
            )

            # Cache before multiple actions
            df_joined.cache()
            
            insert_count = df_joined.where(col("operation_status") == "INSERT").count()
            update_count = df_joined.where(col("operation_status") == "UPDATE").count()
            delete_count = df_joined.where(col("operation_status") == "DELETE").count()

            logger.info(f"Transformation complete. Found {insert_count} to INSERT, {update_count} to UPDATE, and {delete_count} to DELETE (deactivate).")
            
            total_to_process = insert_count + update_count + delete_count
            if total_to_process == 0:
                logger.info(f"No changes detected for schema {schema}. Skipping database write.")
                df_joined.unpersist()
                continue
            
            # --- 4.4 Write to Oracle using JayDeBeApi ---
            logger.warning(f"Collecting {total_to_process} records to the driver for row-by-row processing.")
            
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
                
                from java.sql import Timestamp
                insert_success, update_success, delete_success = 0, 0, 0

                for row in df_joined.collect():
                    if row["operation_status"] == "INSERT":
                        cur.execute(f"""
                            INSERT INTO {schema}.FMASTER_ITEM(
                                ITEM_ID, ITEM_NAME, UNIT1, UNIT2, UNIT3, CONVUNIT2, CONVUNIT3,
                                CARTON_LENGTH, CARTON_WIDTH, CARTON_HEIGHT, WEIGHT_NETTO, WEIGHT_BRUTO,
                                SELLPRICE1, SELLPRICE2, SELLPRICE3, FLAG_ACTIVE, MATGR,
                                CREATED_ON, CREATED_BY
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            row['item_id'], row['item_name'], row['unit1'], row['unit2'], row['unit3'],
                            row['convunit2'], row['convunit3'], row['carton_length'], row['carton_width'],
                            row['carton_height'], row['weight_netto'], row['weight_bruto'],
                            row['sellprice1'], row['sellprice2'], row['sellprice3'], row['flag_active'],
                            row['matgr'], Timestamp(JLong(row['created_on'].timestamp() * 1000)), row['created_by']
                        ))
                        insert_success += 1
                    elif row["operation_status"] == "UPDATE":
                        cur.execute(f"""
                            UPDATE {schema}.FMASTER_ITEM
                            SET item_name = ?, unit1 = ?, unit2 = ?, unit3 = ?, convunit2 = ?, convunit3 = ?,
                                carton_length = ?, carton_width = ?, carton_height = ?, weight_netto = ?, weight_bruto = ?,
                                sellprice1 = ?, sellprice2 = ?, sellprice3 = ?, flag_active = ?, matgr = ?,
                                modified_on = ?, modified_by = ?
                            WHERE item_id = ?
                        """, (
                            row['item_name'], row['unit1'], row['unit2'], row['unit3'], row['convunit2'], row['convunit3'],
                            row['carton_length'], row['carton_width'], row['carton_height'], row['weight_netto'], row['weight_bruto'],
                            row['sellprice1'], row['sellprice2'], row['sellprice3'], row['flag_active'], row['matgr'],
                            Timestamp(JLong(row['modified_on'].timestamp() * 1000)), row['modified_by'], row['item_id']
                        ))
                        update_success += 1
                    else: # DELETE (Deactivate)
                        cur.execute(f"""
                            UPDATE {schema}.FMASTER_ITEM
                            SET flag_active = '0', modified_on = ?, modified_by = ?
                            WHERE item_id = ?
                        """, (
                            Timestamp(JLong(row['modified_on'].timestamp() * 1000)), row['modified_by'], row['item_id']
                        ))
                        delete_success += 1
                
                conn_oracle.commit()
                logger.info("Database transaction committed successfully.")
                logger.info(f"Write summary for schema {schema}: {insert_success} inserted, {update_success} updated, {delete_success} deactivated.")

            except Exception as db_err:
                 logger.error(f"Database write failed for schema {schema}. Rolling back transaction.")
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
    ORACLE_URL = "192.168.8.249:1521/DBMYR"
    ORACLE_USER = "system"
    ORACLE_PASSWORD = "ditributionexcellentKM18"
    
    run_job_sku_penjatahan(ORACLE_URL, ORACLE_USER, ORACLE_PASSWORD)
    
    logger.info("ETL job has finished all tasks.")
    spark.stop()
    logger.info("SparkSession stopped.")
