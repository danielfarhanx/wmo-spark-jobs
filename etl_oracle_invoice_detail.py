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
logger.info("Starting ETL Job: EtlOracleInvoiceDetail")

POSTGRES_DRIVER_PATH = "C:/driver/postgresql-42.7.7.jar"
ORACLE_DRIVER_PATH = "C:/driver/ojdbc8.jar"

try:
    spark = SparkSession.builder \
        .appName("EtlOracleInvoiceDetail") \
        .config("spark.driver.extraClassPath", f"{POSTGRES_DRIVER_PATH};{ORACLE_DRIVER_PATH}") \
        .config("spark.driver.extraJavaOptions", "-Doracle.jdbc.timezoneAsRegion=false")  \
        .config("spark.executor.extraJavaOptions", "-Doracle.jdbc.timezoneAsRegion=false")  \
        .config("spark.network.timeout", "1000s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.sql.broadcastTimeout", "1000s") \
        .getOrCreate()
    
    logger.info("SparkSession configured and created successfully.")
    logger.info(f"Spark Driver Memory: {spark.conf.get('spark.driver.memory')}")
    logger.info(f"Spark Executor Memory: {spark.conf.get('spark.executor.memory')}")

except Exception as e:
    logger.error("Fatal: Failed to create SparkSession.")
    logger.error(traceback.format_exc())
    sys.exit(1)

# --- PostgreSQL Connection Details ---
MYO_JDBC_URL = "jdbc:postgresql://" + sys.argv[4] #"ss-qa-api.mayora.co.id:5432/mayora-online"
MYO_DB_PROPERTIES = {
    "user": sys.argv[5], #"postgres",
    "password": sys.argv[6], #"postgres",
    "driver": "org.postgresql.Driver"
}

def run_job_outlet(oracle_url, oracle_user, oracle_password):
    """
    Main function to run the ETL job for invoice detail data.
    """
    logger.info("ETL process for invoice details starting...")
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
        logger.info(f"Fetching distributor schemas from Oracle: {oracle_url}")
        dba_query = f"(SELECT username FROM sys.dba_users WHERE username in ('{sys.argv[7]}')) temp" #USERMYR
        df_schema = spark.read.jdbc(
            url=ORACLE_JDBC_URL,
            table=dba_query,
            properties=ORACLE_DB_PROPERTIES
        )
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
            # --- 4.1 Read from Oracle Source (Invoice Details) ---
            sql_query_invoice_d = f"""
            (
            SELECT (SELECT memostring
                    FROM {schema}.fmemo
                    WHERE memonama = 'KODEDIST') dist_id, invno, pcode, amount, orgqty,
                qty,
                    CASE
                        WHEN LENGTH (orgqty_bsr) = 1
                            THEN '0000' || orgqty_bsr
                        WHEN LENGTH (orgqty_bsr) = 2
                            THEN '000' || orgqty_bsr
                        WHEN LENGTH (orgqty_bsr) = 3
                            THEN '00' || orgqty_bsr
                        WHEN LENGTH (orgqty_bsr) = 4
                            THEN '0' || orgqty_bsr
                        ELSE TO_CHAR (orgqty_bsr)
                    END
                || '.'
                || CASE
                        WHEN LENGTH (orgqty_tgh) = 1
                            THEN '00' || orgqty_tgh
                        WHEN LENGTH (orgqty_tgh) = 2
                            THEN '0' || orgqty_tgh
                        ELSE TO_CHAR (orgqty_tgh)
                    END
                || '.'
                || CASE
                        WHEN LENGTH (orgqty_kcl) = 1
                            THEN '00' || orgqty_kcl
                        WHEN LENGTH (orgqty_kcl) = 2
                            THEN '0' || orgqty_kcl
                        ELSE TO_CHAR (orgqty_kcl)
                    END xqty_org,
                    CASE
                        WHEN LENGTH (qty_bsr) = 1
                            THEN '0000' || qty_bsr
                        WHEN LENGTH (qty_bsr) = 2
                            THEN '000' || qty_bsr
                        WHEN LENGTH (qty_bsr) = 3
                            THEN '00' || qty_bsr
                        WHEN LENGTH (qty_bsr) = 4
                            THEN '0' || qty_bsr
                        ELSE TO_CHAR (qty_bsr)
                    END
                || '.'
                || CASE
                        WHEN LENGTH (qty_tgh) = 1
                            THEN '00' || qty_tgh
                        WHEN LENGTH (qty_tgh) = 2
                            THEN '0' || qty_tgh
                        ELSE TO_CHAR (qty_tgh)
                    END
                || '.'
                || CASE
                        WHEN LENGTH (qty_kcl) = 1
                            THEN '00' || qty_kcl
                        WHEN LENGTH (qty_kcl) = 2
                            THEN '0' || qty_kcl
                        ELSE TO_CHAR (qty_kcl)
                    END xqty
            FROM (SELECT a.invno, b.pcode,
                            NVL (b.amount, 0)
                        - (   ROUND (NVL (c.disc1, 0), 0)
                            + ROUND (NVL (c.disc1kh, 0), 0)
                            + ROUND (NVL (c.proamount, 0), 0)
                            - ROUND (NVL (b.rpdiscpckk, 0), 0)
                            ) amount,
                            e.orgqty, b.qty, FLOOR (e.orgqty / f.convunit3) AS orgqty_bsr,
                            FLOOR ((MOD (e.orgqty, f.convunit3)) / f.convunit2
                                    ) AS orgqty_tgh,
                            FLOOR (MOD ((MOD (e.orgqty, f.convunit3)), f.convunit2)
                                    ) AS orgqty_kcl,
                            FLOOR (e.qty / f.convunit3) AS qty_bsr,
                            FLOOR ((MOD (e.qty, f.convunit3)) / f.convunit2) AS qty_tgh,
                            FLOOR (MOD ((MOD (e.qty, f.convunit3)), f.convunit2)
                                    ) AS qty_kcl
                    FROM {schema}.fjual_h a INNER JOIN {schema}.fjual_d1 b
                        ON a.orderno = b.orderno
                        AND a.transtype = b.transtype
                        AND a.slsno = b.slsno
                        INNER JOIN {schema}.fjual_d4 c
                        ON a.orderno = c.orderno
                        AND a.slsno = c.slsno
                        AND a.transtype = c.transtype
                        AND b.pcode = c.pcode
                        LEFT JOIN {schema}.forder_h d ON a.data8 = d.orderno
                        LEFT JOIN {schema}.forder_d1 e
                        ON d.orderno = e.orderno AND b.pcode = e.pcode
                        LEFT JOIN {schema}.fmaster f ON b.pcode = f.pcode
                        JOIN {schema}.FCUSTMST fc ON a.CUSTNO = fc.CUSTNO AND fc.TYPEOUT IN ('105','109','110','114','115','116','122','123','126','127','159','160','173','180','108','124','150','160','701','702','703','122')
                    WHERE a.transtype = 'F'
                        AND a.INVDATE >= (SELECT MIN(CDATE) min_date_13w FROM(
                                        SELECT f.CDATE, DENSE_RANK() OVER(ORDER BY tahun DESC, weekno desc) rownumber FROM {schema}.FCYCLE3 f WHERE trunc(CDATE) <= (SELECT trunc(CURRENT_DATE) FROM DUAL)
                                    ) WHERE rownumber = 14))
            ) temp_invoice_d
            """
            
            logger.info(f"Reading invoice detail data from Oracle schema: {schema}")
            df_orc_invoice_d = spark.read.jdbc(
                url=ORACLE_JDBC_URL,
                table=sql_query_invoice_d,
                properties=ORACLE_DB_PROPERTIES
            )
            
            source_count = df_orc_invoice_d.count()
            logger.info(f"Successfully read {source_count} rows from Oracle source.")

            if source_count == 0:
                logger.info(f"No invoice detail data found for schema {schema}. Skipping to next schema.")
                continue

            # --- 4.2 Standardize Source DataFrame ---
            df_orc_invoice_d = df_orc_invoice_d.select(
                col("DIST_ID").alias("dist_id"), col("INVNO").alias("invoice_no"),
                col("PCODE").alias("pcode"), col("AMOUNT").alias("inv_nett"),
                col("ORGQTY").alias("ori_qty"), col("QTY").alias("qty"),
                col("XQTY_ORG").alias("ori_xqty"), col("XQTY").alias("xqty"),
                F.current_timestamp().alias("created_on"), F.lit("SYS").alias("created_by"),
                F.lit(None).alias("modified_on"), F.lit(None).alias("modified_by")
            )
            
            # --- 4.3 Read from PostgreSQL Target ---
            logger.info("Reading target tables (t_invoice_h, t_invoice_d) from PostgreSQL.")
            #df_pg_myo_invoice_h = spark.read.jdbc(url=MYO_JDBC_URL, table="myo.t_invoice_h", properties=MYO_DB_PROPERTIES)
            #df_pg_myo_invoice_d = spark.read.jdbc(url=MYO_JDBC_URL, table="myo.t_invoice_d", properties=MYO_DB_PROPERTIES)

            source_dist_id = df_orc_invoice_d.select("dist_id").distinct().collect()[0]["dist_id"]

            myo_invoice_h_sql_query = f"(SELECT * FROM myo.t_invoice_h WHERE dist_id = '{source_dist_id}') AS filtered_invoice_h"
            df_pg_myo_invoice_h = spark.read.jdbc(url=MYO_JDBC_URL, table=myo_invoice_h_sql_query, properties=MYO_DB_PROPERTIES)

            myo_invoice_d_sql_query = f"(select tid.* from myo.t_invoice_d tid join myo.t_invoice_h tih on tid.invoice_id = tih.id and tih.dist_id = '{source_dist_id}') AS filtered_invoice_d"
            df_pg_myo_invoice_d = spark.read.jdbc(url=MYO_JDBC_URL, table=myo_invoice_d_sql_query, properties=MYO_DB_PROPERTIES)

            # --- 4.4 Join, Find Changes, and Prepare for Upsert ---
            logger.info("Identifying new and updated records by joining source and target dataframes.")
            df_joined = df_orc_invoice_d.alias("orc") \
                .join(
                    df_pg_myo_invoice_h.alias("pg_inv_h"),
                    (col("orc.dist_id") == col("pg_inv_h.dist_id")) & (col("orc.invoice_no") == col("pg_inv_h.invoice_no")),
                    "inner"
                ) \
                .join(
                    df_pg_myo_invoice_d.alias("pg_inv_d"),
                    (col("pg_inv_h.id") == col("pg_inv_d.invoice_id")) & (col("orc.pcode") == col("pg_inv_d.pcode")),
                    "left"
                ) \
                .where(
                    col("pg_inv_d.invoice_id").isNull() #|
                    #(
                    #    ~(col("orc.inv_nett").eqNullSafe(col("pg_inv_d.inv_nett"))) |
                    #    ~(col("orc.ori_qty").eqNullSafe(col("pg_inv_d.ori_qty"))) |
                    #    ~(col("orc.qty").eqNullSafe(col("pg_inv_d.qty"))) |
                    #    ~(col("orc.ori_xqty").eqNullSafe(col("pg_inv_d.ori_xqty"))) |
                    #    ~(col("orc.xqty").eqNullSafe(col("pg_inv_d.xqty")))
                    #)
                ).select(
                    col("pg_inv_h.id").alias("invoice_id"),
                    col("orc.pcode"), col("orc.inv_nett"), col("orc.ori_qty"),
                    col("orc.qty"), col("orc.ori_xqty"), col("orc.xqty"),
                    F.when(col("pg_inv_d.invoice_id").isNull(), col("orc.created_on")).otherwise(col("pg_inv_d.created_on")).alias("created_on"),
                    F.when(col("pg_inv_d.invoice_id").isNull(), col("orc.created_by")).otherwise(col("pg_inv_d.created_by")).alias("created_by"),
                    F.when(col("pg_inv_d.invoice_id").isNull(), col("orc.modified_on")).otherwise(F.current_timestamp()).alias("modified_on"),
                    F.when(col("pg_inv_d.invoice_id").isNull(), col("orc.modified_by")).otherwise(F.lit("SYS")).alias("modified_by"),
                    F.when(col("pg_inv_d.invoice_id").isNull(), F.lit("INSERT")).otherwise(F.lit("UPDATE")).alias("operation_status")
                )

            # Cache the DataFrame before triggering multiple actions
            df_joined.cache()

            insert_count = df_joined.where(col("operation_status") == "INSERT").count()
            update_count = df_joined.where(col("operation_status") == "UPDATE").count()
            logger.info(f"Transformation complete. Found {insert_count} records to INSERT and {update_count} records to UPDATE.")

            total_to_process = insert_count + update_count
            if total_to_process == 0:
                logger.info(f"No new or updated invoice details to write for schema {schema}. Skipping database write.")
                df_joined.unpersist()
                continue
            
            # --- 4.5 Write to PostgreSQL using psycopg2 ---
            logger.warning(f"Collecting {total_to_process} records to the driver for row-by-row processing. This may cause memory issues.")
            
            conn_pg = None
            try:
                logger.info("Connecting to PostgreSQL using psycopg2 for write operation.")
                conn_pg = psycopg2.connect(
                    dbname="mayora-online", user=sys.argv[5], password=sys.argv[6],
                    host=sys.argv[8], port="5432"
                )
                cur = conn_pg.cursor()
                
                insert_success_count = 0
                update_success_count = 0

                for row in df_joined.collect():
                    if row["operation_status"] == "INSERT":
                        cur.execute("""
                            INSERT INTO myo.t_invoice_d(
                                invoice_id, pcode, inv_nett, ori_qty, qty,
                                ori_xqty, xqty, created_on, created_by
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            row["invoice_id"], row["pcode"], row["inv_nett"],
                            row["ori_qty"], row["qty"], row["ori_xqty"],
                            row["xqty"], row["created_on"], row["created_by"]
                        ))
                        insert_success_count += 1
                    else: # UPDATE
                        cur.execute("""
                            UPDATE myo.t_invoice_d
                            SET inv_nett = %s, ori_qty = %s, qty = %s,
                                ori_xqty = %s, xqty = %s,
                                modified_on = %s, modified_by = %s
                            WHERE invoice_id = %s and pcode = %s
                        """, (
                            row['inv_nett'], row['ori_qty'], row['qty'],
                            row['ori_xqty'], row['xqty'], row['modified_on'],
                            row['modified_by'], row['invoice_id'], row['pcode']
                        ))
                        update_success_count += 1
                
                conn_pg.commit()
                logger.info("Database transaction committed.")
                logger.info(f"Successfully wrote data for schema {schema}: {insert_success_count} inserted, {update_success_count} updated.")
                cur.close()

            except Exception as db_err:
                 logger.error(f"Database write failed for schema {schema}.")
                 logger.error(traceback.format_exc())
                 if conn_pg:
                     conn_pg.rollback()
            finally:
                if conn_pg:
                    conn_pg.close()
                    logger.info("PostgreSQL connection closed.")

            df_joined.unpersist() # Unpersist the DataFrame

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
    
    run_job_outlet(ORACLE_URL, ORACLE_USER, ORACLE_PASSWORD)
    
    logger.info("ETL job has finished all tasks.")
    spark.stop()
    logger.info("SparkSession stopped.")
