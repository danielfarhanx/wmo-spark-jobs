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
# 2. SPARK SESSION & DATABASE CONFIGURATION
# ==============================================================================
logger.info("Starting ETL Job: EtlOracleMasterOutletSalesman")

POSTGRES_DRIVER_PATH = "C:/driver/postgresql-42.7.7.jar"
ORACLE_DRIVER_PATH = "C:/driver/ojdbc8.jar"

try:
    spark = SparkSession.builder \
        .appName("EtlOracleMasterOutletSalesman") \
        .config("spark.driver.extraClassPath", f"{POSTGRES_DRIVER_PATH};{ORACLE_DRIVER_PATH}") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.network.timeout", "1000s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.sql.broadcastTimeout", "1000s") \
        .config("spark.log.level", "WARN") \
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

def run_job_outlet_salesman(oracle_url, oracle_user, oracle_password):
    """
    Main function to synchronize outlet-salesman data from Oracle to PostgreSQL.
    """
    ORACLE_JDBC_URL = "jdbc:oracle:thin:@//" + oracle_url
    ORACLE_DB_PROPERTIES = {
        "user": oracle_user, "password": oracle_password,
        "driver": "oracle.jdbc.driver.OracleDriver", "fetchSize": "10000"
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
            logger.info(f"Reading outlet-salesman data from Oracle schema: {schema}")
            sql_query_outlet_salesman = f"""
            (
            SELECT DISTINCT (SELECT MEMOSTRING FROM {schema}.FMEMO WHERE memonama = 'KODEDIST') dist_id, f.CUSTNO, f2.EMPNO, 
                            f.SLSNO, fs.SLSNAME, fs.DATA04 SLSFC, f3.DIVISI_ID
            FROM {schema}.FCUSTSLS f 
            JOIN {schema}.FSLS fs ON f.SLSNO = fs.SLSNO AND fs.FLAG_NONAKTIF IS NULL
            LEFT JOIN {schema}.FTABLE74 f2 ON f.SLSNO = f2.SLSNO
            JOIN {schema}.FTABLE74_D f3 ON f2.EMPNO = f3.EMPNO
            JOIN {schema}.FCUSTMST f4 ON f.CUSTNO = f4.CUSTNO
            UNION
            SELECT DISTINCT (SELECT MEMOSTRING FROM {schema}.FMEMO WHERE memonama = 'KODEDIST') dist_id, f.CUSTNO, f2.EMPNO, 
                            f.SLSNO, fs.SLSNAME, fs.DATA04 SLSFC, f3.DIVISI_ID
            FROM {schema}.FCUSTSLS f 
            JOIN {schema}.FSLS fs ON f.SLSNO = fs.SLSNO AND fs.FLAG_NONAKTIF IS NULL
            LEFT JOIN {schema}.FTABLE74 f2 ON f.SLSNO = f2.SLSNO
            JOIN {schema}.FTABLE74_D f3 ON f2.EMPNO = f3.EMPNO
            JOIN {schema}.FCUSTMST f4 ON f.CUSTNO = f4.CUSTNO
            WHERE f4.CUSTNO IN (
                SELECT DISTINCT f2.GROUPPAYER
                FROM {schema}.FCUSTMST f
                JOIN {schema}.FTABLE48 f2 ON f.CUSTNO = f2.CUSTNO
            )
            ) temp_outlet_salesman
            """
            df_orc_source = spark.read.jdbc(url=ORACLE_JDBC_URL, table=sql_query_outlet_salesman, properties=ORACLE_DB_PROPERTIES)
            source_count = df_orc_source.count()
            logger.info(f"Successfully read {source_count} rows from Oracle source.")

            # --- 4.2 Read from PostgreSQL Target & Dimension Tables ---
            logger.info("Reading dimension (m_outlet) and target (m_outlet_salesman) tables from PostgreSQL.")
            source_dist_id = df_orc_source.select("dist_id").distinct().collect()[0]["dist_id"]
            full_sql_query = f"(SELECT * FROM {PG_SCHEMA}.m_outlet WHERE dist_id = {source_dist_id}) AS filtered_outlet"
            df_pg_outlet_dim = spark.read.jdbc(url=MYO_JDBC_URL, table=full_sql_query, properties=MYO_DB_PROPERTIES)
            df_pg_target = spark.read.jdbc(url=MYO_JDBC_URL, table=f"(SELECT * FROM {PG_SCHEMA}.m_outlet_salesman WHERE outlet_id in (SELECT id FROM {PG_SCHEMA}.m_outlet WHERE dist_id = {source_dist_id})) AS filtered_outlet_salesman", properties=MYO_DB_PROPERTIES)
            
            # --- 4.3 Prepare Source Data (Translate outlet_id) ---
            logger.info("Preparing source data by mapping Oracle CUSTNO to PostgreSQL outlet ID.")
            df_orc_prepared = df_orc_source.alias("orc") \
                .join(
                    df_pg_outlet_dim.alias("dim"),
                    (col("orc.DIST_ID") == col("dim.dist_id")) & (col("orc.CUSTNO") == col("dim.outlet_id")),
                    "inner"
                ).select(
                    col("dim.id").alias("outlet_id"), # This is the internal PostgreSQL ID
                    col("orc.EMPNO").alias("spv_id"),
                    col("orc.SLSNO").alias("sls_id"),
                    col("orc.SLSNAME").alias("sls_name"),
                    col("orc.SLSFC").alias("slsfc_id"),
                    col("orc.DIVISI_ID").alias("sls_div")
                )

            # --- 4.4 Join, Find Changes, and Prepare for Upsert/Delete ---
            logger.info("Identifying all changes (inserts, updates, deletes) using a full outer join.")
            df_changes = df_orc_prepared.alias("src").join(
                df_pg_target.alias("tgt"),
                (col("src.outlet_id") == col("tgt.outlet_id")) &
                (col("src.spv_id") == col("tgt.spv_id")) &
                (col("src.sls_id") == col("tgt.sls_id")),
                "full_outer"
            ).where(
                (col("src.outlet_id").isNull()) | # Exists in target, not in source -> DELETE
                (col("tgt.outlet_id").isNull()) | # Exists in source, not in target -> INSERT
                ( # Exists in both, check for changes -> UPDATE
                    ~(col("src.sls_name").eqNullSafe(col("tgt.sls_name"))) |
                    ~(col("src.slsfc_id").eqNullSafe(col("tgt.slsfc_id"))) |
                    ~(col("src.sls_div").eqNullSafe(col("tgt.sls_div")))
                )
            ).select(
                F.when(col("src.outlet_id").isNotNull(), col("src.outlet_id")).otherwise(col("tgt.outlet_id")).alias("outlet_id"),
                F.when(col("src.spv_id").isNotNull(), col("src.spv_id")).otherwise(col("tgt.spv_id")).alias("spv_id"),
                F.when(col("src.sls_id").isNotNull(), col("src.sls_id")).otherwise(col("tgt.sls_id")).alias("sls_id"),
                col("src.sls_name"), col("src.slsfc_id"), col("src.sls_div"),
                F.when(col("tgt.outlet_id").isNull(), F.current_timestamp()).otherwise(col("tgt.created_on")).alias("created_on"),
                F.when(col("tgt.outlet_id").isNull(), F.lit("SYS")).otherwise(col("tgt.created_by")).alias("created_by"),
                F.when(col("tgt.outlet_id").isNotNull() & col("src.outlet_id").isNotNull(), F.current_timestamp()).alias("modified_on"),
                F.when(col("tgt.outlet_id").isNotNull() & col("src.outlet_id").isNotNull(), F.lit("SYS")).alias("modified_by"),
                F.when(col("src.outlet_id").isNull(), F.lit("DELETE"))
                 .when(col("tgt.outlet_id").isNull(), F.lit("INSERT"))
                 .otherwise(F.lit("UPDATE")).alias("operation_status")
            )

            # Cache before multiple actions
            df_changes.cache()
            
            insert_count = df_changes.where(col("operation_status") == "INSERT").count()
            update_count = df_changes.where(col("operation_status") == "UPDATE").count()
            delete_count = df_changes.where(col("operation_status") == "DELETE").count()
            
            logger.info(f"Transformation complete. Found {insert_count} to INSERT, {update_count} to UPDATE, {delete_count} to DELETE.")
            
            total_to_process = insert_count + update_count + delete_count
            if total_to_process == 0:
                logger.info(f"No changes detected for schema {schema}. Skipping database write.")
                df_changes.unpersist()
                continue
            
            # --- 4.5 Write to PostgreSQL ---
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

                for row in df_changes.collect():
                    op = row["operation_status"]
                    if op == "INSERT":
                        cur.execute(f"""
                            INSERT INTO {PG_SCHEMA}.m_outlet_salesman(
                                outlet_id, spv_id, sls_id, sls_name, slsfc_id, sls_div, created_on, created_by
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            row['outlet_id'], row['spv_id'], row['sls_id'], row['sls_name'],
                            row['slsfc_id'], row['sls_div'], row['created_on'], row['created_by']
                        ))
                        insert_success += 1
                    elif op == "UPDATE":
                        cur.execute(f"""
                            UPDATE {PG_SCHEMA}.m_outlet_salesman
                            SET sls_name = %s, slsfc_id = %s, sls_div = %s, modified_on = %s, modified_by = %s
                            WHERE outlet_id = %s AND spv_id = %s AND sls_id = %s
                        """, (
                            row['sls_name'], row['slsfc_id'], row['sls_div'], row['modified_on'],
                            row['modified_by'], row['outlet_id'], row['spv_id'], row['sls_id']
                        ))
                        update_success += 1
                    elif op == "DELETE":
                        cur.execute(f"""
                            DELETE FROM {PG_SCHEMA}.m_outlet_salesman
                            WHERE outlet_id = %s AND spv_id = %s AND sls_id = %s
                        """, (row['outlet_id'], row['spv_id'], row['sls_id']))
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

            df_changes.unpersist()

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
    
    run_job_outlet_salesman(ORACLE_URL, ORACLE_USER, ORACLE_PASSWORD)
    
    logger.info("ETL job has finished all tasks. Shutting down Spark.")
    spark.stop()
    logger.info("SparkSession stopped.")
