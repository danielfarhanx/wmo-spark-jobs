import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    # 1. Initialize Spark Session with required dynamic packages
    spark = SparkSession.builder \
        .appName("EtlReportFicom") \
        .getOrCreate()

    # Access Hadoop configuration for MinIO integration
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://ss-dev-app.mayora.co.id:8083")
    hadoop_conf.set("fs.s3a.access.key", "devss")
    hadoop_conf.set("fs.s3a.secret.key", "@MayoR4Dev")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.endpoint.region", "us-east-1")
    hadoop_conf.set("fs.s3a.impl.disable.cache", "true")

    try:
        # =================================================================
        # 2. EXTRACT & TRANSFORM POSTGRESQL DATA
        # =================================================================
        print("Extracting order data from Postgres...")
        
        # We pass your optimized SQL query directly into the 'dbtable' option using subquery syntax
        postgres_query = """
        (
            select
                mo.dist_id::text,
                mo.outlet_id::text,
                mc."year"::int4,
                mc."period"::int4,
                mc.week::int4,
                toh.order_date::date::text,
                count(toh.order_no)::int4 as count_order,
                sum(toh.amount)::int8 as sum_amount
            from myo.m_outlet mo
            left join myo.t_order_h toh on
                mo.outlet_id = toh.outlet_mtx
                and mo.dist_id::text = toh.subdist_id
                and toh.approval_status = 'APPROVED'
            left join myo.m_cycle3 mc on
                mc.cdate = toh.order_date
            group by
                mo.dist_id::text,
                mo.outlet_id::text,
                mc."year"::int4,
                mc."period"::int4,
                mc.week::int4,
                toh.order_date::date::text
        ) as order_metrics
        """

        df_pg = spark.read.format("jdbc") \
                .option("url", "jdbc:postgresql://172.31.37.135:5432/mayora-online") \
            .option("dbtable", postgres_query) \
            .option("user", "myon") \
            .option("password", "M@yor4Online2025km!18") \
            .load()

        # =================================================================
        # 3. EXTRACT & TRANSFORM MONGODB DATA (UNIVERSE)
        # =================================================================
        print("Extracting registered outlet universe from MongoDB...")

        # Read the relevant MongoDB collections
        base_uri = "mongodb://myon:M%40yor4Onlinekm!18@172.31.37.133:27017/db_myo?directConnection=true"
        #df_users = spark.read.format("mongodb").option("connection.uri", mongo_uri).option("database", "db_myo").option(".collection", "m_user").load()
        #df_user_outlets = spark.read.format("mongodb").option("connection.uri", mongo_uri).option("database", "db_myo").option("collection", "m_user_outlet").load()
        #df_approvals = spark.read.format("mongodb").option("connection.uri", mongo_uri).option("database", "db_myo").option("collection", "t_approval_outlet_div").load()

        df_users = spark.read.format("mongodb").option("connection.uri", base_uri).option("collection", "m_user").option("spark.mongodb.input.collection", "m_user").load()
        df_user_outlets = spark.read.format("mongodb").option("connection.uri", base_uri).option("collection", "m_user_outlet").option("spark.mongodb.input.collection", "m_user_outlet").load()
        df_approvals = spark.read.format("mongodb").option("connection.uri", base_uri).option("collection", "t_approval_outlet_div").option("spark.mongodb.input.collection", "t_approval_outlet_div").load()

        # Target filter: Only approved statuses
        df_approvals_filtered = df_approvals.filter(
            (F.col("approval_ss_status") == "APPROVED") & 
            (F.col("approval_rsm_status") == "APPROVED")
        )

        # Handle Java logic: Check if empIdSs overlaps with a specific list of target userIds
        # Replace this list with your actual dynamic filter parameters if needed
        # target_user_ids = ["USER_ID_1", "USER_ID_2"] 
        # df_approvals_filtered = df_approvals_filtered.filter(
        #     F.arrays_overlap(F.col("empIdSs"), F.array([F.lit(uid) for uid in target_user_ids]))
        # )

        # Execute relational joins mimicking the Java loops
        # Join 1: Match users to their assigned outlets
        df_user_join = df_users.join(df_user_outlets, df_users.user_id == df_user_outlets.user_id, "inner")

        # Join 2: Match to approval details based on compound keys (distId and outletId)
        df_mongo_universe = df_user_join.join(
            df_approvals_filtered,
            (df_user_outlets.dist_id == df_approvals_filtered.dist_id) & 
            (df_user_outlets.outlet_id == df_approvals_filtered.outlet_id),
            "inner"
        ).select(
            df_user_outlets.dist_id.alias("dist_id"),
            df_user_outlets.outlet_id.alias("outlet_id"),
            df_approvals_filtered.created_on.alias("registered_date")
        ).dropDuplicates(["dist_id", "outlet_id"])

        # =================================================================
        # 4. JOIN BOTH SOURCES (UNIVERSE + PERFORMANCE METRICS)
        # =================================================================
        print("Merging MongoDB Universe with Postgres Order Metrics...")
        
        # Cast data types to be absolutely certain the join keys match seamlessly
        df_mongo_universe = df_mongo_universe.withColumn("dist_id", F.col("dist_id").cast("string")) \
                                             .withColumn("outlet_id", F.col("outlet_id").cast("string"))
        
        df_pg = df_pg.withColumn("dist_id", F.col("dist_id").cast("string")) \
                     .withColumn("outlet_id", F.col("outlet_id").cast("string"))

        # Left join to preserve the entire universe even if an outlet has 0 orders
        final_df = df_mongo_universe.join(
            df_pg,
            (df_mongo_universe.dist_id == df_pg.dist_id) & 
            (df_mongo_universe.outlet_id == df_pg.outlet_id),
            "left"
        ).select(
            df_mongo_universe.dist_id,
            df_mongo_universe.outlet_id,
            df_mongo_universe.registered_date,
            F.col("year"),
            F.col("period"),
            F.col("week"),
            F.col("order_date"),
            F.col("count_order").alias("count_order"), 
            F.col("sum_amount").alias("sum_amount")  
        )

        # =================================================================
        # 5. WRITE SINGLE FILE TO MINIO
        # =================================================================
        print("Streaming unified CSV payload out to MinIO drop-off...")
        final_df.coalesce(1).write \
            .format("csv") \
            .option("header", "true") \
            .mode("overwrite") \
            .save("s3a://spark-exports/report_mayora_online/")

        print("Pipeline process completed perfectly!")

    except Exception as e:
        print(f"Pipeline crashed during execution: {str(e)}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
