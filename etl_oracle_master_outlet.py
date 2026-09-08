import logging
import traceback
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
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
logger.info("Starting ETL Job: EtlOracleMasterOutlet")

POSTGRES_DRIVER_PATH = "C:/driver/postgresql-42.7.7.jar"
ORACLE_DRIVER_PATH = "C:/driver/ojdbc8.jar"

POSTGRES_USER=sys.argv[5]
POSTGRES_PASS=sys.argv[6]
POSTGRES_HOST=sys.argv[8]

try:
    spark = SparkSession.builder \
        .appName("EtlOracleMasterOutlet") \
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

def run_job_outlet(oracle_url, oracle_user, oracle_password):
    """
    Main function to synchronize outlet master data from Oracle to PostgreSQL.
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
            logger.info(f"Reading outlet master data from Oracle schema: {schema}")
            sql_query_outlet = f"""
            (
            SELECT 
                (SELECT MEMOSTRING FROM {schema}.FMEMO WHERE memonama = 'KODEDIST') dist_id, f.CUSTNO, f.CUSTNAME, f.CCONTACT, f.TYPEOUT, f.CUSTADD1||' '||f.CUSTADD2 addr, 
				CASE
				    WHEN TRIM(f4.LONGITUDE) IS NULL OR NOT REGEXP_LIKE(TRIM(f4.LONGITUDE), '^[+-]?[0-9]+(\.[0-9]+)?$')
				    THEN '0.0'
				    ELSE TRIM(f4.LONGITUDE)
				END AS LONGITUDE,
				CASE
				    WHEN TRIM(f4.LATITUDE) IS NULL OR NOT REGEXP_LIKE(TRIM(f4.LATITUDE), '^[+-]?[0-9]+(\.[0-9]+)?$')
				    THEN '0.0'
				    ELSE TRIM(f4.LATITUDE)
				END AS LATITUDE, 
                CASE 
                    WHEN f2.groupdivisi='M1' THEN f5.nobarcode1 
                    WHEN f2.groupdivisi='M2' THEN f5.nobarcode2 
                    WHEN f2.groupdivisi='M3' THEN f5.nobarcode3 
                    WHEN f2.groupdivisi = 'ALL' THEN f5.nobarcode1 
                END nobarcode,
                f2.GROUPPAYER, f.GDISC, f.GRUPOUT, f.GHARGA, f.DATA15 SPECIFICOUT
            FROM {schema}.FCUSTMST f 
            JOIN {schema}.FTABLE48 f2 ON f.CUSTNO = f2.CUSTNO
            LEFT JOIN (
                SELECT a.CUSTNO, a.LONGITUDE, a.LATITUDE FROM (
                    SELECT a.CUSTNO, a.LONGITUDE, a.LATITUDE,
                           RANK() OVER (PARTITION BY a.CUSTNO ORDER BY REGISTRATIONDATE DESC) urut
                    FROM {schema}.FTABLE274 a 
                    LEFT JOIN {schema}.FTABLE275 f ON a.CUSTNO = f.CUSTNO AND f.FLAG_APPROVED = '1'
                ) a WHERE urut = 1
            ) f4 ON f4.CUSTNO = f.CUSTNO
            LEFT JOIN {schema}.FTABLE21 f5 ON f.CUSTNO = f5.CUSTNO
            WHERE f.TYPEOUT IN ('105','108','109','110','114','115','116','120','122','123','124','126','127','150','159','160','160','173','701','702','703') 
            UNION
            SELECT 
                (SELECT MEMOSTRING FROM {schema}.FMEMO WHERE memonama = 'KODEDIST') dist_id, f.CUSTNO, f.CUSTNAME, f.CCONTACT, f.TYPEOUT, f.CUSTADD1||' '||f.CUSTADD2 addr, 
                CASE
                    WHEN TRIM(f4.LONGITUDE) IS NULL OR NOT REGEXP_LIKE(TRIM(f4.LONGITUDE), '^[+-]?[0-9]+(\.[0-9]+)?$')
                    THEN '0.0'
                    ELSE TRIM(f4.LONGITUDE)
                END AS LONGITUDE,
                CASE
                    WHEN TRIM(f4.LATITUDE) IS NULL OR NOT REGEXP_LIKE(TRIM(f4.LATITUDE), '^[+-]?[0-9]+(\.[0-9]+)?$')
                    THEN '0.0'
                    ELSE TRIM(f4.LATITUDE)
                END AS LATITUDE, 
                CASE 
                    WHEN f2.groupdivisi='M1' THEN f5.nobarcode1 
                    WHEN f2.groupdivisi='M2' THEN f5.nobarcode2 
                    WHEN f2.groupdivisi='M3' THEN f5.nobarcode3 
                    WHEN f2.groupdivisi = 'ALL' THEN f5.nobarcode1 
                END nobarcode,
                f2.GROUPPAYER, f.GDISC, f.GRUPOUT, f.GHARGA, f.DATA15 SPECIFICOUT
            FROM {schema}.FCUSTMST f 
            JOIN {schema}.FTABLE48 f2 ON f.CUSTNO = f2.CUSTNO
            LEFT JOIN (
                SELECT a.CUSTNO, a.LONGITUDE, a.LATITUDE FROM (
                    SELECT a.CUSTNO, a.LONGITUDE, a.LATITUDE,
                           RANK() OVER (PARTITION BY a.CUSTNO ORDER BY REGISTRATIONDATE DESC) urut
                    FROM {schema}.FTABLE274 a 
                    LEFT JOIN {schema}.FTABLE275 f ON a.CUSTNO = f.CUSTNO AND f.FLAG_APPROVED = '1'
                ) a WHERE urut = 1
            ) f4 ON f4.CUSTNO = f.CUSTNO
            LEFT JOIN {schema}.FTABLE21 f5 ON f.CUSTNO = f5.CUSTNO
            WHERE f.CUSTNO IN (
                SELECT DISTINCT f2.GROUPPAYER
                FROM {schema}.FCUSTMST f
                JOIN {schema}.FTABLE48 f2 ON f.CUSTNO = f2.CUSTNO
                WHERE f.TYPEOUT IN ('105','108','109','110','114','115','116','120','122','123','124','126','127','150','159','160','160','173','701','702','703') 
            ) 
            ) temp_outlet
            """
            df_orc_outlet = spark.read.jdbc(
                url=ORACLE_JDBC_URL,
                table=sql_query_outlet,
                properties=ORACLE_DB_PROPERTIES
            ).select(
                col("dist_id").cast("int").alias("dist_id"), col("CUSTNO").alias("outlet_id"),
                col("CUSTNAME").alias("outlet_name"), col("CCONTACT").alias("owner_name"),
                col("TYPEOUT").alias("channel_id"), col("addr").alias("addr"),
                col("LONGITUDE").alias("longitude"), col("LATITUDE").alias("latitude"),
                col("nobarcode").alias("barcode"), col("GROUPPAYER").alias("group_payer_id"),
                col("GDISC").alias("group_disc_id"), col("GRUPOUT").alias("group_outlet_id"),
                col("GHARGA").alias("group_harga_id"), col("SPECIFICOUT").alias("specific_outlet_id"),
                F.current_timestamp().alias("created_on"), F.lit("SYS").alias("created_by"),
                F.lit(None).alias("modified_on"), F.lit(None).alias("modified_by")
            )
            source_count = df_orc_outlet.count()
            logger.info(f"Successfully read {source_count} rows from Oracle source.")

            # --- 4.2 Read from PostgreSQL Target ---
            logger.info("Reading outlet master data from PostgreSQL target table.")
            source_dist_id = df_orc_outlet.select("dist_id").distinct().collect()[0]["dist_id"]
            full_sql_query = f"(SELECT * FROM myo.m_outlet WHERE dist_id = {source_dist_id}) AS filtered_outlet"
            df_pg_myo_outlet = spark.read.jdbc(url=MYO_JDBC_URL, table=full_sql_query, properties=MYO_DB_PROPERTIES)
            logger.info(f"Successfully read {df_pg_myo_outlet.count()} rows from PostgreSQL target.")

            # --- 4.3 Join, Find Changes, and Prepare for Upsert ---
            logger.info("Identifying new and updated records by joining source and target.")
            df_joined = df_orc_outlet.cache().alias("orc").join(
                df_pg_myo_outlet.cache().alias("pg"),
                (col("orc.dist_id") == col("pg.dist_id")) & (col("orc.outlet_id") == col("pg.outlet_id")),
                "left"
            ).where(
                (col("pg.dist_id").isNull()) |
                (
                    ~(col("orc.outlet_name").eqNullSafe(col("pg.outlet_name"))) |
                    ~(col("orc.owner_name").eqNullSafe(col("pg.owner_name"))) |
                    ~(col("orc.channel_id").eqNullSafe(col("pg.channel_id"))) |
                    ~(col("orc.addr").eqNullSafe(col("pg.addr"))) |
                    ~(col("orc.longitude").eqNullSafe(col("pg.longitude"))) |
                    ~(col("orc.latitude").eqNullSafe(col("pg.latitude"))) |
                    ~(col("orc.barcode").eqNullSafe(col("pg.barcode"))) |
                    ~(col("orc.group_payer_id").eqNullSafe(col("pg.group_payer_id"))) |
                    ~(col("orc.group_disc_id").eqNullSafe(col("pg.group_disc_id"))) |
                    ~(col("orc.group_outlet_id").eqNullSafe(col("pg.group_outlet_id"))) |
                    ~(col("orc.group_harga_id").eqNullSafe(col("pg.group_harga_id"))) |
                    ~(col("orc.specific_outlet_id").eqNullSafe(col("pg.specific_outlet_id")))
                )
            ).select(
                col("orc.*"),
                F.when(col("pg.dist_id").isNull(), col("orc.created_on")).otherwise(col("pg.created_on")).alias("created_on_final"),
                F.when(col("pg.dist_id").isNull(), col("orc.created_by")).otherwise(col("pg.created_by")).alias("created_by_final"),
                F.when(col("pg.dist_id").isNotNull(), F.current_timestamp()).alias("modified_on_final"),
                F.when(col("pg.dist_id").isNotNull(), F.lit("SYS")).alias("modified_by_final"),
                F.when(col("pg.dist_id").isNull(), F.lit("INSERT")).otherwise(F.lit("UPDATE")).alias("operation_status")
            )

            # Cache before multiple actions
            df_joined.cache()
            
            # insert_count = df_joined.where(col("operation_status") == "INSERT").count()
            # update_count = df_joined.where(col("operation_status") == "UPDATE").count()
            
            # logger.info(f"Transformation complete. Found {insert_count} to INSERT and {update_count} to UPDATE.")
            
            # total_to_process = insert_count + update_count
            # if total_to_process == 0:
            #     logger.info(f"No changes detected for schema {schema}. Skipping database write.")
            #     df_joined.unpersist()
            #     continue
            
            # --- 4.4 Write to PostgreSQL ---
            # logger.warning(f"⚠️ Collecting {total_to_process} records to the driver for row-by-row processing.")
            
            # conn_pg = None
            # try:
            #     logger.info("Connecting to PostgreSQL target using psycopg2 for write operation.")
            #     conn_pg = psycopg2.connect(
            #         dbname="mayora-online", user=sys.argv[5], password=sys.argv[6],
            #         host=sys.argv[8], port="5432"
            #     )
            #     cur = conn_pg.cursor()
            #     logger.info("PostgreSQL connection successful. Starting transaction.")
                
            #     insert_success, update_success = 0, 0

            #     for row in df_joined.collect():
            #         if row["operation_status"] == "INSERT":
            #             cur.execute("""
            #                 INSERT INTO myo.m_outlet(
            #                     dist_id, outlet_id, outlet_name, owner_name, channel_id, addr, longitude, latitude, 
            #                     barcode, group_payer_id, group_disc_id, group_outlet_id, group_harga_id, 
            #                     specific_outlet_id, created_on, created_by
            #                 ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            #             """, (
            #                 row['dist_id'], row['outlet_id'], row['outlet_name'], row['owner_name'], row['channel_id'],
            #                 row['addr'], row['longitude'], row['latitude'], row['barcode'], row['group_payer_id'],
            #                 row['group_disc_id'], row['group_outlet_id'], row['group_harga_id'],
            #                 row['specific_outlet_id'], row['created_on_final'], row['created_by_final']
            #             ))
            #             insert_success += 1
            #         else: # UPDATE
            #             cur.execute("""
            #                 UPDATE myo.m_outlet
            #                 SET outlet_name = %s, owner_name = %s, channel_id = %s, addr = %s, longitude = %s, 
            #                     latitude = %s, barcode = %s, group_payer_id = %s, group_disc_id = %s, 
            #                     group_outlet_id = %s, group_harga_id = %s, specific_outlet_id = %s, 
            #                     modified_on = %s, modified_by = %s
            #                 WHERE dist_id = %s AND outlet_id = %s
            #             """, (
            #                 row['outlet_name'], row['owner_name'], row['channel_id'], row['addr'], row['longitude'],
            #                 row['latitude'], row['barcode'], row['group_payer_id'], row['group_disc_id'],
            #                 row['group_outlet_id'], row['group_harga_id'], row['specific_outlet_id'],
            #                 row['modified_on_final'], row['modified_by_final'], row['dist_id'], row['outlet_id']
            #             ))
            #             update_success += 1
                
            #     conn_pg.commit()
            #     logger.info("✅ Database transaction committed successfully.")
            #     logger.info(f"Write summary for schema {schema}: {insert_success} inserted, {update_success} updated.")

            # except Exception as db_err:
            #      logger.error(f"❌ Database write failed for schema {schema}. Rolling back transaction.")
            #      logger.error(traceback.format_exc())
            #      if conn_pg:
            #          conn_pg.rollback()
            # finally:
            #     if conn_pg:
            #         cur.close()
            #         conn_pg.close()
            #         logger.info("PostgreSQL connection closed.")
            logger.info("Starting distributed write to PostgreSQL using foreachPartition.")
            df_joined = df_joined.repartition(16)
            df_joined.foreachPartition(process_partition)
            
            logger.info("Distributed write complete.")
            df_joined.unpersist()

        except Exception as e:
            logger.error(f"An unexpected error occurred while processing schema {schema}. See details below.")
            logger.error(traceback.format_exc())
            continue # Continue to the next schema
        
def process_partition(iterator):
    # This code runs on the EXECUTOR, not the driver
    
    # 1. Create the connection *inside* the function.
    # Connections are not serializable and must be created on the worker.
    conn_pg = None
    try:
        conn_pg = psycopg2.connect(
            dbname="mayora-online", user=POSTGRES_USER, password=POSTGRES_PASS,
            host=POSTGRES_HOST, port="5432"
        )
        cur = conn_pg.cursor()
        
        # 2. Loop through the rows in this partition
        for row in iterator:
            if row["operation_status"] == "INSERT":
                cur.execute("""
                    INSERT INTO myo.m_outlet(
                        dist_id, outlet_id, outlet_name, owner_name, channel_id, addr, longitude, latitude, 
                        barcode, group_payer_id, group_disc_id, group_outlet_id, group_harga_id, 
                        specific_outlet_id, created_on, created_by
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['dist_id'], row['outlet_id'], row['outlet_name'], row['owner_name'], row['channel_id'],
                    row['addr'], row['longitude'], row['latitude'], row['barcode'], row['group_payer_id'],
                    row['group_disc_id'], row['group_outlet_id'], row['group_harga_id'],
                    row['specific_outlet_id'], row['created_on_final'], row['created_by_final']
                ))
            else: # UPDATE
                cur.execute("""
                    UPDATE myo.m_outlet
                    SET outlet_name = %s, owner_name = %s, channel_id = %s, addr = %s, longitude = %s, 
                        latitude = %s, barcode = %s, group_payer_id = %s, group_disc_id = %s, 
                        group_outlet_id = %s, group_harga_id = %s, specific_outlet_id = %s, 
                        modified_on = %s, modified_by = %s
                    WHERE dist_id = %s AND outlet_id = %s
                """, (
                    row['outlet_name'], row['owner_name'], row['channel_id'], row['addr'], row['longitude'],
                    row['latitude'], row['barcode'], row['group_payer_id'], row['group_disc_id'],
                    row['group_outlet_id'], row['group_harga_id'], row['specific_outlet_id'],
                    row['modified_on_final'], row['modified_by_final'], row['dist_id'], row['outlet_id']
                ))
        
        # 3. Commit the transaction for this partition
        conn_pg.commit()
        
    except Exception as e:
        # Handle or log the error
        if conn_pg:
            conn_pg.rollback()
        # You might want to raise the exception to fail the task
        raise e
    finally:
        # 4. Always close connections
        if conn_pg:
            cur.close()
            conn_pg.close()

# ==============================================================================
# 5. JOB EXECUTION
# ==============================================================================
if __name__ == "__main__":
    ORACLE_URL = sys.argv[1] #"192.168.8.249:1521/DBMYR"
    ORACLE_USER = sys.argv[2] #"system"
    ORACLE_PASSWORD = sys.argv[3] #"ditributionexcellentKM18"
    
    run_job_outlet(ORACLE_URL, ORACLE_USER, ORACLE_PASSWORD)
    
    logger.info("ETL job has finished all tasks. Shutting down Spark.")
    spark.stop()
    logger.info("SparkSession stopped.")
