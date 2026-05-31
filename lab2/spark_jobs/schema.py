"""
Lab 2 - Step 1: ETL Pipeline from Staging to Star Schema
Run: spark-submit --jars /jars/postgresql-42.7.3.jar transform_to_star_schema.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

DB_URL = "jdbc:postgresql://postgres:5432/petshop"
DB_CONFIG = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


def init_spark():
    """Initialize Spark session with PostgreSQL driver"""
    return (SparkSession.builder
            .appName("StarSchemaTransformation")
            .config("spark.jars", "/jars/postgresql-42.7.3.jar")
            .getOrCreate())


def load_staging_data(spark):
    """Read data from staging table in PostgreSQL"""
    return spark.read.jdbc(DB_URL, "staging_source", properties=DB_CONFIG)


def transform_vendors(dataframe):
    """Extract and load vendor dimension"""
    vendors = (dataframe
               .select("vendor_title", "vendor_contact", "vendor_email",
                       "vendor_phone", "vendor_addr", "vendor_city", "vendor_country")
               .dropDuplicates(["vendor_title", "vendor_city"])
               .filter(F.col("vendor_title").isNotNull() & (F.col("vendor_title") != "")))

    window_spec = Window.orderBy("vendor_title", "vendor_city")
    vendors = vendors.withColumn("vendor_id", F.row_number().over(window_spec))

    vendors.write.jdbc(DB_URL, "ref_vendor", mode="append", properties=DB_CONFIG)
    return vendors


def transform_buyers(dataframe):
    """Extract and load customer dimension"""
    buyers = (dataframe
              .select(F.col("trans_buyer_ref").cast("int").alias("buyer_id"),
                      "buyer_first_name", "buyer_last_name",
                      F.col("buyer_age").cast("int").alias("buyer_age"),
                      "buyer_email", "buyer_country", "buyer_zip",
                      "buyer_pet_class", "buyer_pet_name", "buyer_pet_breed")
              .dropDuplicates(["buyer_id"])
              .filter(F.col("buyer_id").isNotNull()))
    
    buyers.write.jdbc(DB_URL, "ref_buyer", mode="append", properties=DB_CONFIG)


def transform_agents(dataframe):
    """Extract and load seller dimension"""
    agents = (dataframe
              .select(F.col("trans_agent_ref").cast("int").alias("agent_id"),
                      "agent_first_name", "agent_last_name",
                      "agent_email", "agent_country", "agent_zip")
              .dropDuplicates(["agent_id"])
              .filter(F.col("agent_id").isNotNull()))
    
    agents.write.jdbc(DB_URL, "ref_agent", mode="append", properties=DB_CONFIG)


def transform_items(dataframe, vendors_df):
    """Extract and load product dimension with vendor reference"""
    items = (dataframe
             .select(F.col("trans_item_ref").cast("int").alias("item_id"),
                     "item_name", "item_type",
                     F.col("item_cost").cast("double").alias("item_price"),
                     F.col("item_stock").cast("int").alias("item_quantity"),
                     F.col("item_weight").cast("double").alias("item_weight"),
                     "item_color", "item_size", "item_brand",
                     "item_material", "item_desc",
                     F.col("item_score").cast("double").alias("item_rating"),
                     F.col("feedback_cnt").cast("int").alias("item_reviews"),
                     F.to_date("release_dt", "MM/dd/yyyy").alias("item_release_date"),
                     F.to_date("expire_dt", "MM/dd/yyyy").alias("item_expiry_date"),
                     "pet_group", "vendor_title", "vendor_city")
             .dropDuplicates(["item_id"])
             .filter(F.col("item_id").isNotNull()))

    items = (items
             .join(vendors_df.select("vendor_id", "vendor_title", "vendor_city"),
                   on=["vendor_title", "vendor_city"], how="left")
             .drop("vendor_title", "vendor_city"))

    items.write.jdbc(DB_URL, "ref_item", mode="append", properties=DB_CONFIG)


def transform_outlets(dataframe):
    """Extract and load store dimension"""
    outlets = (dataframe
               .select("outlet_title", "outlet_addr", "outlet_city", "outlet_state",
                       "outlet_country", "outlet_phone", "outlet_email")
               .dropDuplicates(["outlet_title", "outlet_city"])
               .filter(F.col("outlet_title").isNotNull() & (F.col("outlet_title") != "")))

    window_spec = Window.orderBy("outlet_title", "outlet_city")
    outlets = outlets.withColumn("outlet_id", F.row_number().over(window_spec))
    
    outlets.write.jdbc(DB_URL, "ref_outlet", mode="append", properties=DB_CONFIG)
    return outlets


def transform_calendar(dataframe):
    """Extract and load date dimension"""
    calendar = (dataframe
                .select(F.to_date("trans_date", "MM/dd/yyyy").alias("full_date"))
                .dropDuplicates(["full_date"])
                .filter(F.col("full_date").isNotNull()))

    calendar = (calendar
                .withColumn("day_num", F.dayofmonth("full_date"))
                .withColumn("month_num", F.month("full_date"))
                .withColumn("year_num", F.year("full_date"))
                .withColumn("quarter_num", F.quarter("full_date")))

    window_spec = Window.orderBy("full_date")
    calendar = calendar.withColumn("calendar_id", F.row_number().over(window_spec))
    
    calendar.write.jdbc(DB_URL, "ref_calendar", mode="append", properties=DB_CONFIG)
    return calendar


def transform_facts(dataframe, outlets_df, calendar_df):
    """Extract and load fact table"""
    facts = (dataframe
             .select(F.col("trans_buyer_ref").cast("int").alias("buyer_id"),
                     F.col("trans_agent_ref").cast("int").alias("agent_id"),
                     F.col("trans_item_ref").cast("int").alias("item_id"),
                     "outlet_title", "outlet_city",
                     F.to_date("trans_date", "MM/dd/yyyy").alias("full_date"),
                     F.col("trans_qty").cast("int").alias("quantity_sold"),
                     F.col("trans_total").cast("double").alias("total_amount"))
             .filter(F.col("buyer_id").isNotNull()))

    facts = facts.join(
        outlets_df.select("outlet_id", "outlet_title", "outlet_city"),
        on=["outlet_title", "outlet_city"], how="left"
    ).drop("outlet_title", "outlet_city")

    facts = facts.join(
        calendar_df.select("calendar_id", "full_date"),
        on="full_date", how="left"
    ).drop("full_date")

    facts.write.jdbc(DB_URL, "fact_transactions", mode="append", properties=DB_CONFIG)


def run_etl():
    """Main ETL pipeline execution"""
    spark = init_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Loading staging data from PostgreSQL...")
    source_df = load_staging_data(spark)
    source_df.cache()

    print("Transforming vendor dimension...")
    vendors_df = transform_vendors(source_df)

    print("Transforming buyer dimension...")
    transform_buyers(source_df)

    print("Transforming agent dimension...")
    transform_agents(source_df)

    print("Transforming item dimension...")
    transform_items(source_df, vendors_df)

    print("Transforming outlet dimension...")
    outlets_df = transform_outlets(source_df)

    print("Transforming calendar dimension...")
    calendar_df = transform_calendar(source_df)

    print("Transforming fact table...")
    transform_facts(source_df, outlets_df, calendar_df)

    print("ETL pipeline completed successfully!")
    spark.stop()


if __name__ == "__main__":
    run_etl()
