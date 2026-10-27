import logging
import traceback
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
from pyspark.sql import functions as F
import psycopg2

# ==============================================================================
# 1. SETUP LOGGING & ENVIRONMENT
# ==============================================================================

# Configure logging to provide detailed, timestamped output
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
logger.info("Starting ETL Job: EtlOracleInvoice")

POSTGRES_DRIVER_PATH = "C:/driver/postgresql-42.7.7.jar"
ORACLE_DRIVER_PATH = "C:/driver/ojdbc8.jar"

try:
    spark = SparkSession.builder \
        .appName("EtlOracleInvoice") \
        .config("spark.driver.extraClassPath", f"{POSTGRES_DRIVER_PATH}:{ORACLE_DRIVER_PATH}") \
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
    logger.error("Failed to create SparkSession.")
    logger.error(traceback.format_exc())
    sys.exit(1) # Exit if Spark can't start

# --- PostgreSQL Connection Details ---
MYO_JDBC_URL = "jdbc:postgresql://" + sys.argv[4] #"ss-qa-api.mayora.co.id:5432/mayora-online"
MYO_DB_PROPERTIES = {
    "user": sys.argv[5], #"postgres",
    "password": sys.argv[6], #"postgres",
    "driver": "org.postgresql.Driver"
}

# Optional 9th arg: target PostgreSQL schema. Defaults to "myo" when omitted.
PG_SCHEMA = sys.argv[9] if len(sys.argv) > 9 else "myo"

def run_job_outlet(oracle_url, oracle_user, oracle_password):
    """
    Main function to run the ETL job for processing invoice data.
    """
    logger.info("ETL process starting...")
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
        logger.error("Failed to fetch schema list from Oracle DBA_USERS.")
        logger.error(traceback.format_exc())
        return # Stop execution if we can't get the schemas

    # ==============================================================================
    # 4. PROCESS EACH SCHEMA
    # ==============================================================================
    for schema in schemas:
        logger.info(f"================== Processing schema: {schema} ==================")
        try:
            # --- 4.1 Read from Oracle Source ---
            sql_query_invoice_h = f"""
            (
            SELECT b.orderno, (SELECT memostring
                                FROM {schema}.fmemo
                                WHERE memonama = 'KODEDIST') dist_id, a.custno, a.slsno,
                    a.invno, a.invdate, a.invamountcharge,
                    a.subtotalr - (a.invamountcharge - a.chargeamount) AS disc,
                    CASE
                        WHEN b.orderno IS NOT NULL
                            THEN 'Mayora Online'
                        ELSE 'Offline Order'
                    END order_type,
                    a.duedate,
                    CASE
                        WHEN NVL (a.status, '0') = '1'
                            THEN 'DELIVERED'
                        WHEN NVL (a.status, '0') = '4'
                            THEN 'CANCEL'
                        WHEN NVL (a.status, '0') = '0'
                            THEN 'OPEN'
                    END delivery_status,
                    CASE
                        WHEN a.amountpaidcharge > 0
                        AND a.amountpaidcharge < a.invamountcharge
                            THEN 'PARTIALLY PAID'
                        WHEN a.amountpaidcharge = a.invamountcharge
                            THEN 'PAID'
                        ELSE 'UNPAID'
                    END payment_status
            FROM {schema}.fjual_h a LEFT JOIN {schema}.forder_online_h b ON a.data8 = b.reffno
                LEFT JOIN {schema}.forder_online_inv c
                ON b.orderno = c.orderno AND a.invno = c.invno
                JOIN {schema}.FCUSTMST f ON a.CUSTNO = f.CUSTNO
            WHERE a.transtype = 'F'
                AND a.INVDATE >= (SELECT MIN(CDATE) min_date_13w FROM(
                                SELECT f.CDATE, DENSE_RANK() OVER(ORDER BY tahun DESC, weekno desc) rownumber FROM {schema}.FCYCLE3 f WHERE trunc(CDATE) <= (SELECT trunc(CURRENT_DATE) FROM DUAL)
                            ) WHERE rownumber = 14)
            ) temp_invoice_h
            """
            
            logger.info(f"Reading invoice data from Oracle schema: {schema}")
            df_orc_invoice_h = spark.read.jdbc(
                url=ORACLE_JDBC_URL,
                table=sql_query_invoice_h,
                properties=ORACLE_DB_PROPERTIES
            )
            
            source_count = df_orc_invoice_h.count()
            logger.info(f"Successfully read {source_count} rows from Oracle source.")

            if source_count == 0:
                logger.info(f"No new invoice data found for schema {schema}. Skipping to next schema.")
                continue

            # --- 4.2 Standardize Source DataFrame ---
            df_orc_invoice_h = df_orc_invoice_h.select(
                col("ORDERNO").alias("order_no"), col("DIST_ID").alias("dist_id"),
                col("CUSTNO").alias("outlet_id"), col("SLSNO").alias("sls_id"),
                col("INVNO").alias("invoice_no"), col("INVDATE").alias("invoice_date"),
                col("INVAMOUNTCHARGE").alias("invoice_amount"), col("DISC").alias("discount_total"),
                col("ORDER_TYPE").alias("order_type"), col("DUEDATE").alias("due_date"),
                col("DELIVERY_STATUS").alias("delivery_status"), col("PAYMENT_STATUS").alias("payment_status"),
                F.current_timestamp().alias("created_on"), F.lit("SYS").alias("created_by"),
                F.lit(None).alias("modified_on"), F.lit(None).alias("modified_by"),
            ).cache()
            
            # --- 4.3 Read from PostgreSQL Target ---
            logger.info("Reading target tables (m_outlet, t_order_h, t_invoice_h) from PostgreSQL.")
            #df_pg_myo_outlet = spark.read.jdbc(url=MYO_JDBC_URL, table="myo.m_outlet", properties=MYO_DB_PROPERTIES)
            #df_pg_myo_invoice_h = spark.read.jdbc(url=MYO_JDBC_URL, table="myo.t_invoice_h", properties=MYO_DB_PROPERTIES)
            #df_pg_myo_order_h = spark.read.jdbc(url=MYO_JDBC_URL, table="myo.t_order_h", properties=MYO_DB_PROPERTIES)

            source_dist_id = df_orc_invoice_h.select("dist_id").distinct().collect()[0]["dist_id"]

            myo_outlet_sql_query = f"(SELECT * FROM {PG_SCHEMA}.m_outlet WHERE dist_id = {source_dist_id}) AS filtered_outlet"
            df_pg_myo_outlet = spark.read.jdbc(url=MYO_JDBC_URL, table=myo_outlet_sql_query, properties=MYO_DB_PROPERTIES)

            myo_invoice_h_sql_query = f"(SELECT * FROM {PG_SCHEMA}.t_invoice_h WHERE dist_id = '{source_dist_id}') AS filtered_invoice_h"
            df_pg_myo_invoice_h = spark.read.jdbc(url=MYO_JDBC_URL, table=myo_invoice_h_sql_query, properties=MYO_DB_PROPERTIES)

            myo_order_h_sql_query = f"(SELECT * FROM {PG_SCHEMA}.t_order_h WHERE subdist_id = '{source_dist_id}') AS filtered_order_h"
            df_pg_myo_order_h = spark.read.jdbc(url=MYO_JDBC_URL, table=myo_order_h_sql_query, properties=MYO_DB_PROPERTIES)

            # --- 4.4 Join, Find Changes, and Prepare for Upsert ---
            # logger.info("Identifying new and updated records by joining source and target dataframes.")
            # df_joined = df_orc_invoice_h.alias("orc") \
            #     .join(df_pg_myo_outlet.alias("pg_mo"), (col("orc.dist_id") == col("pg_mo.dist_id")) & (col("orc.outlet_id") == col("pg_mo.outlet_id")), "inner") \
            #     .join(df_pg_myo_order_h.alias("pg_order_h"), col("orc.order_no") == col("pg_order_h.order_no"), "left") \
            #     .join(df_pg_myo_invoice_h.alias("pg_inv_h"), (col("orc.dist_id") == col("pg_inv_h.dist_id")) & (col("orc.invoice_no") == col("pg_inv_h.invoice_no")), "left") \
            #     .where(
            #         (col("orc.order_no").isNull() | col("pg_order_h.order_no").isNotNull()) &
            #         (
            #             col("pg_inv_h.invoice_no").isNull() |
            #             (
            #                 ~(col("orc.sls_id").eqNullSafe(col("pg_inv_h.sls_id"))) |
            #                 ~(col("orc.invoice_date").eqNullSafe(col("pg_inv_h.invoice_date"))) |
            #                 ~(col("orc.invoice_amount").eqNullSafe(col("pg_inv_h.invoice_amount"))) |
            #                 ~(col("orc.discount_total").eqNullSafe(col("pg_inv_h.discount_total"))) |
            #                 ~(col("orc.order_type").eqNullSafe(col("pg_inv_h.order_type"))) |
            #                 ~(col("orc.due_date").eqNullSafe(col("pg_inv_h.due_date"))) |
            #                 ~(col("orc.delivery_status").eqNullSafe(col("pg_inv_h.delivery_status"))) |
            #                 ~(col("orc.payment_status").eqNullSafe(col("pg_inv_h.payment_status")))
            #             )
            #         )
            #     ).select(
            #         col("orc.order_no"), col("orc.dist_id"),
            #         col("pg_mo.id").alias("outlet_id"), # Use internal ID from m_outlet
            #         col("orc.sls_id"), col("orc.invoice_no"), col("orc.invoice_date"),
            #         col("orc.invoice_amount"), col("orc.discount_total"), col("orc.order_type"),
            #         col("orc.due_date"), col("orc.delivery_status"), col("orc.payment_status"),
            #         F.when(col("pg_inv_h.invoice_no").isNull(), col("orc.created_on")).otherwise(col("pg_inv_h.created_on")).alias("created_on"),
            #         F.when(col("pg_inv_h.invoice_no").isNull(), col("orc.created_by")).otherwise(col("pg_inv_h.created_by")).alias("created_by"),
            #         F.when(col("pg_inv_h.invoice_no").isNull(), col("orc.modified_on")).otherwise(F.current_timestamp()).alias("modified_on"),
            #         F.when(col("pg_inv_h.invoice_no").isNull(), col("orc.modified_by")).otherwise(F.lit("SYS")).alias("modified_by"),
            #         F.when(col("pg_inv_h.invoice_no").isNull(), F.lit("INSERT")).otherwise(F.lit("UPDATE")).alias("operation_status")
            #     )
            
            ####
            # --------------------------------------------------------------------------
            # Step 1: Stage and Enrich the Source Data
            # --------------------------------------------------------------------------
            # Join the main source with dimension tables first.
            # This creates a clean, enriched source DataFrame to work with.
            # The `left` join to orders is also done here to check for validity.

            df_source_enriched = df_orc_invoice_h.alias("orc") \
                .join(
                    df_pg_myo_outlet.alias("pg_mo"),
                    (col("orc.dist_id") == col("pg_mo.dist_id")) & (col("orc.outlet_id") == col("pg_mo.outlet_id")),
                    "inner"
                ) \
                .join(
                    df_pg_myo_order_h.alias("pg_order_h"),
                    col("orc.order_no") == col("pg_order_h.order_no"),
                    "left"
                ) \
                .where(
                    col("orc.order_no").isNull() | col("pg_order_h.order_no").isNotNull()
                ) \
                .select(
                    "orc.*",
                    col("pg_mo.id").alias("final_outlet_id") # Use a distinct name to avoid column ambiguity
                )

            # --------------------------------------------------------------------------
            # Step 2: Identify New Records (The "INSERT" logic)
            # --------------------------------------------------------------------------
            # A 'left_anti' join is perfect for this. It returns only the rows from the
            # left DataFrame (our source) that do NOT have a match in the right DataFrame (the target).

            df_inserts = df_source_enriched.alias("source") \
                .join(
                    df_pg_myo_invoice_h.alias("target"),
                    (col("source.dist_id") == col("target.dist_id")) & (col("source.invoice_no") == col("target.invoice_no")),
                    "left_anti"
                ) \
                .select(
                    col("source.order_no"),
                    col("source.dist_id"),
                    col("source.final_outlet_id").alias("outlet_id"),
                    col("source.sls_id"),
                    col("source.invoice_no"),
                    col("source.invoice_date"),
                    col("source.invoice_amount"),
                    col("source.discount_total"),
                    col("source.order_type"),
                    col("source.due_date"),
                    col("source.delivery_status"),
                    col("source.payment_status"),
                    col("source.created_on"),
                    col("source.created_by"),
                    col("source.modified_on"),
                    col("source.modified_by"),
                    F.lit("INSERT").alias("operation_status")
                )

            # --------------------------------------------------------------------------
            # Step 3: Identify Changed Records (The "UPDATE" logic)
            # --------------------------------------------------------------------------
            # First, find all records that exist in both source and target using an INNER join.
            # Then, apply the filter to find only the ones where data has changed.

            # Define the condition for changes to make it reusable and readable
            update_condition = (
                # ~(col("source.sls_id").eqNullSafe(col("target.sls_id"))) |
                # ~(col("source.invoice_date").eqNullSafe(col("target.invoice_date"))) |
                # ~(col("source.invoice_amount").eqNullSafe(col("target.invoice_amount"))) |
                # ~(col("source.discount_total").eqNullSafe(col("target.discount_total"))) |
                # ~(col("source.order_type").eqNullSafe(col("target.order_type"))) |
                # ~(col("source.due_date").eqNullSafe(col("target.due_date"))) |
                ~(col("source.delivery_status").eqNullSafe(col("target.delivery_status"))) |
                ~(col("source.payment_status").eqNullSafe(col("target.payment_status")))
            )

            df_updates = df_source_enriched.alias("source") \
                .join(
                    df_pg_myo_invoice_h.alias("target"),
                    (col("source.dist_id") == col("target.dist_id")) & (col("source.invoice_no") == col("target.invoice_no")),
                    "inner"
                ) \
                .where(update_condition) \
                .select(
                    col("source.order_no"),
                    col("source.dist_id"),
                    col("source.final_outlet_id").alias("outlet_id"),
                    col("source.sls_id"),
                    col("source.invoice_no"),
                    col("source.invoice_date"),
                    col("source.invoice_amount"),
                    col("source.discount_total"),
                    col("source.order_type"),
                    col("source.due_date"),
                    col("source.delivery_status"),
                    col("source.payment_status"),
                    col("target.created_on"),  # Keep original created_on for updates
                    col("target.created_by"),  # Keep original created_by for updates
                    F.current_timestamp().alias("modified_on"), # Set new modified_on
                    F.lit("SYS").alias("modified_by"),          # Set new modified_by
                    F.lit("UPDATE").alias("operation_status")
                )

            # --------------------------------------------------------------------------
            # Step 4: Combine Inserts and Updates
            # --------------------------------------------------------------------------
            # Use unionByName to safely combine the two DataFrames. It matches columns
            # by name, which is more robust than matching by position (the default `union`).

            df_joined = df_inserts.unionByName(df_updates)
            ####

            # Cache the DataFrame as it will be used multiple times
            df_joined.cache()

            insert_count = df_joined.where(col("operation_status") == "INSERT").count()
            update_count = df_joined.where(col("operation_status") == "UPDATE").count()
            logger.info(f"Transformation complete. Found {insert_count} records to INSERT and {update_count} records to UPDATE.")

            total_to_process = insert_count + update_count
            if total_to_process == 0:
                logger.info(f"No new or updated records to write for schema {schema}. Skipping database write.")
                df_joined.unpersist()
                continue
            
            # --- 4.5 Write to PostgreSQL using psycopg2 ---
            logger.warning(f"Collecting {total_to_process} records to the driver for row-by-row processing. This may cause memory issues with large datasets.")
            
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
                        cur.execute(f"""
                            INSERT INTO {PG_SCHEMA}.t_invoice_h(
                                order_no, dist_id, outlet_id, sls_id, invoice_no, invoice_date,
                                invoice_amount, discount_total, order_type, due_date,
                                delivery_status, payment_status, created_on, created_by
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            row['order_no'], row['dist_id'], row['outlet_id'], row['sls_id'],
                            row['invoice_no'], row['invoice_date'], row['invoice_amount'],
                            row['discount_total'], row['order_type'], row['due_date'],
                            row['delivery_status'], row['payment_status'], row['created_on'],
                            row['created_by']
                        ))
                        insert_success_count += 1
                    else: # UPDATE
                        cur.execute(f"""
                            UPDATE {PG_SCHEMA}.t_invoice_h
                            SET sls_id = %s, invoice_date = %s, invoice_amount = %s,
                                discount_total = %s, order_type = %s, due_date = %s,
                                delivery_status = %s, payment_status = %s,
                                modified_on = %s, modified_by = %s
                            WHERE dist_id = %s AND outlet_id = %s AND invoice_no = %s
                        """, (
                            row['sls_id'], row['invoice_date'], row['invoice_amount'],
                            row['discount_total'], row['order_type'], row['due_date'],
                            row['delivery_status'], row['payment_status'], row['modified_on'],
                            row['modified_by'], row['dist_id'], row['outlet_id'], row['invoice_no']
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
                     conn_pg.rollback() # Rollback transaction on error
            finally:
                if conn_pg:
                    conn_pg.close()
                    logger.info("PostgreSQL connection closed.")

            df_joined.unpersist() # Unpersist the DataFrame to free up memory

        except Exception as e:
            logger.error(f"An unexpected error occurred while processing schema {schema}.")
            logger.error(traceback.format_exc())
            # Continue to the next schema even if one fails
            continue

# ==============================================================================
# 5. JOB EXECUTION
# ==============================================================================
if __name__ == "__main__":
    # --- Oracle Connection Credentials ---
    ORACLE_URL = sys.argv[1] #"192.168.8.249:1521/DBMYR"
    ORACLE_USER = sys.argv[2] #"system"
    ORACLE_PASSWORD = sys.argv[3] #"ditributionexcellentKM18"
    
    run_job_outlet(ORACLE_URL, ORACLE_USER, ORACLE_PASSWORD)
    
    logger.info("ETL job has finished all tasks.")
    spark.stop()
    logger.info("SparkSession stopped.")
