"""
Lab 2 - Step 2: Generate Analytics Reports from Star Schema to ClickHouse
Run: spark-submit --jars /jars/postgresql-42.7.3.jar,/jars/clickhouse-jdbc-0.6.1-all.jar generate_reports.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# PostgreSQL connection settings
PG_DB_URL = "jdbc:postgresql://postgres:5432/petshop"
PG_CONN_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# ClickHouse connection settings
CH_DB_URL = "jdbc:clickhouse://clickhouse:8123/petshop"
CH_CONN_PROPS = {
    "user": "default",
    "password": "clickhouse",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}


def init_spark_session():
    """Create Spark session with required JDBC drivers"""
    return (SparkSession.builder
            .appName("AnalyticsReportGenerator")
            .config("spark.jars", "/jars/postgresql-42.7.3.jar,/jars/clickhouse-jdbc-0.6.1-all.jar")
            .getOrCreate())


def load_from_postgres(spark_session, table_name):
    """Load data from PostgreSQL table"""
    return spark_session.read.jdbc(PG_DB_URL, table_name, properties=PG_CONN_PROPS)


def save_to_clickhouse(dataframe, target_table, write_mode="overwrite"):
    """Save dataframe to ClickHouse table"""
    dataframe.write.jdbc(CH_DB_URL, target_table, mode=write_mode, properties=CH_CONN_PROPS)


def setup_clickhouse_tables():
    """Create report tables in ClickHouse via HTTP API"""
    import urllib.request

    report_tables = [
        ("top10_items_report", """
            CREATE TABLE IF NOT EXISTS petshop.top10_items_report (
                item_id        Int32,
                item_name      String,
                item_category  String,
                total_quantity Int64,
                total_revenue  Float64,
                avg_score      Float64,
                feedback_count Int64
            ) ENGINE = MergeTree() ORDER BY item_id
        """),
        ("revenue_by_item_group", """
            CREATE TABLE IF NOT EXISTS petshop.revenue_by_item_group (
                item_category  String,
                total_revenue  Float64,
                total_quantity Int64,
                item_count     Int64
            ) ENGINE = MergeTree() ORDER BY item_category
        """),
        ("top10_buyers_report", """
            CREATE TABLE IF NOT EXISTS petshop.top10_buyers_report (
                buyer_id       Int32,
                full_name      String,
                country        String,
                total_spent    Float64,
                avg_transaction Float64,
                order_count    Int64
            ) ENGINE = MergeTree() ORDER BY buyer_id
        """),
        ("buyers_by_country_report", """
            CREATE TABLE IF NOT EXISTS petshop.buyers_by_country_report (
                country        String,
                buyer_count    Int64
            ) ENGINE = MergeTree() ORDER BY country
        """),
        ("monthly_sales_analysis", """
            CREATE TABLE IF NOT EXISTS petshop.monthly_sales_analysis (
                year_num       Int32,
                month_num      Int32,
                total_revenue  Float64,
                total_quantity Int64,
                avg_transaction_value Float64
            ) ENGINE = MergeTree() ORDER BY (year_num, month_num)
        """),
        ("yearly_sales_analysis", """
            CREATE TABLE IF NOT EXISTS petshop.yearly_sales_analysis (
                year_num       Int32,
                total_revenue  Float64,
                total_quantity Int64
            ) ENGINE = MergeTree() ORDER BY year_num
        """),
        ("top5_outlets_report", """
            CREATE TABLE IF NOT EXISTS petshop.top5_outlets_report (
                outlet_id      Int32,
                outlet_name    String,
                outlet_city    String,
                outlet_country String,
                total_revenue  Float64,
                avg_transaction Float64,
                order_count    Int64
            ) ENGINE = MergeTree() ORDER BY outlet_id
        """),
        ("outlets_by_city_report", """
            CREATE TABLE IF NOT EXISTS petshop.outlets_by_city_report (
                outlet_city    String,
                outlet_country String,
                total_revenue  Float64,
                order_count    Int64
            ) ENGINE = MergeTree() ORDER BY outlet_city
        """),
        ("top5_vendors_report", """
            CREATE TABLE IF NOT EXISTS petshop.top5_vendors_report (
                vendor_id      Int32,
                vendor_name    String,
                vendor_country String,
                total_revenue  Float64,
                avg_item_price Float64,
                order_count    Int64
            ) ENGINE = MergeTree() ORDER BY vendor_id
        """),
        ("vendors_by_country_report", """
            CREATE TABLE IF NOT EXISTS petshop.vendors_by_country_report (
                vendor_country String,
                total_revenue  Float64,
                order_count    Int64
            ) ENGINE = MergeTree() ORDER BY vendor_country
        """),
        ("item_quality_analysis", """
            CREATE TABLE IF NOT EXISTS petshop.item_quality_analysis (
                item_id        Int32,
                item_name      String,
                item_category  String,
                avg_score      Float64,
                feedback_count Int64,
                total_quantity Int64,
                total_revenue  Float64
            ) ENGINE = MergeTree() ORDER BY item_id
        """),
    ]

    api_endpoint = "http://clickhouse:8123/?user=default&password=clickhouse"
    for report_name, create_ddl in report_tables:
        request = urllib.request.Request(api_endpoint, data=create_ddl.encode(), method="POST")
        with urllib.request.urlopen(request):
            pass
    print("ClickHouse report tables created successfully")


def generate_item_reports(spark, fact_df, items_df):
    """Generate reports related to products/items"""
    # Top 10 items by sales quantity
    top_items = (fact_df
                 .join(items_df, "item_id")
                 .groupBy("item_id", "item_name", "item_category", "item_rating", "item_reviews")
                 .agg(F.sum("quantity_sold").alias("total_quantity"),
                      F.sum("total_amount").alias("total_revenue"))
                 .orderBy(F.desc("total_quantity"))
                 .limit(10)
                 .select("item_id", "item_name", "item_category",
                         "total_quantity", "total_revenue",
                         F.col("item_rating").alias("avg_score"),
                         F.col("item_reviews").cast("long").alias("feedback_count")))
    save_to_clickhouse(top_items, "top10_items_report")

    # Revenue grouped by category
    revenue_by_category = (fact_df
                           .join(items_df, "item_id")
                           .groupBy("item_category")
                           .agg(F.sum("total_amount").alias("total_revenue"),
                                F.sum("quantity_sold").alias("total_quantity"),
                                F.countDistinct("item_id").alias("item_count")))
    save_to_clickhouse(revenue_by_category, "revenue_by_item_group")

    print("Item reports completed")


def generate_buyer_reports(spark, fact_df, buyers_df):
    """Generate reports related to customers/buyers"""
    # Top 10 buyers by spending
    top_buyers = (fact_df
                  .join(buyers_df, "buyer_id")
                  .groupBy("buyer_id", "buyer_first_name", "buyer_last_name", "buyer_country")
                  .agg(F.sum("total_amount").alias("total_spent"),
                       F.avg("total_amount").alias("avg_transaction"),
                       F.count("trans_id").alias("order_count"))
                  .withColumn("full_name", F.concat_ws(" ", "buyer_first_name", "buyer_last_name"))
                  .orderBy(F.desc("total_spent"))
                  .limit(10)
                  .select("buyer_id", "full_name",
                          F.col("buyer_country").alias("country"),
                          "total_spent", "avg_transaction", "order_count"))
    save_to_clickhouse(top_buyers, "top10_buyers_report")

    # Buyer count by country
    buyers_by_country = (fact_df
                         .join(buyers_df, "buyer_id")
                         .groupBy("buyer_country")
                         .agg(F.countDistinct("buyer_id").alias("buyer_count"))
                         .select(F.col("buyer_country").alias("country"), "buyer_count"))
    save_to_clickhouse(buyers_by_country, "buyers_by_country_report")

    print("Buyer reports completed")


def generate_time_analysis(spark, fact_df, calendar_df):
    """Generate reports by time period"""
    # Monthly sales aggregation
    monthly_analysis = (fact_df
                        .join(calendar_df, "calendar_id")
                        .groupBy("year_num", "month_num")
                        .agg(F.sum("total_amount").alias("total_revenue"),
                             F.sum("quantity_sold").alias("total_quantity"),
                             F.avg("total_amount").alias("avg_transaction_value"))
                        .orderBy("year_num", "month_num"))
    save_to_clickhouse(monthly_analysis, "monthly_sales_analysis")

    # Yearly sales aggregation
    yearly_analysis = (fact_df
                       .join(calendar_df, "calendar_id")
                       .groupBy("year_num")
                       .agg(F.sum("total_amount").alias("total_revenue"),
                            F.sum("quantity_sold").alias("total_quantity"))
                       .orderBy("year_num"))
    save_to_clickhouse(yearly_analysis, "yearly_sales_analysis")

    print("Time analysis completed")


def generate_outlet_reports(spark, fact_df, outlets_df):
    """Generate reports related to stores/outlets"""
    # Top 5 outlets by revenue
    top_outlets = (fact_df
                   .join(outlets_df, "outlet_id")
                   .groupBy("outlet_id", "outlet_name", "outlet_city", "outlet_country")
                   .agg(F.sum("total_amount").alias("total_revenue"),
                        F.avg("total_amount").alias("avg_transaction"),
                        F.count("trans_id").alias("order_count"))
                   .orderBy(F.desc("total_revenue"))
                   .limit(5))
    save_to_clickhouse(top_outlets, "top5_outlets_report")

    # Outlet performance by city
    outlets_by_city = (fact_df
                       .join(outlets_df, "outlet_id")
                       .groupBy("outlet_city", "outlet_country")
                       .agg(F.sum("total_amount").alias("total_revenue"),
                            F.count("trans_id").alias("order_count")))
    save_to_clickhouse(outlets_by_city, "outlets_by_city_report")

    print("Outlet reports completed")


def generate_vendor_reports(spark, fact_df, items_df, vendors_df):
    """Generate reports related to suppliers/vendors"""
    # Top 5 vendors by revenue
    top_vendors = (fact_df
                   .join(items_df.select("item_id", "item_price", "vendor_id"), "item_id")
                   .join(vendors_df, "vendor_id")
                   .groupBy("vendor_id", "vendor_name", "vendor_country")
                   .agg(F.sum("total_amount").alias("total_revenue"),
                        F.avg("item_price").alias("avg_item_price"),
                        F.count("trans_id").alias("order_count"))
                   .orderBy(F.desc("total_revenue"))
                   .limit(5))
    save_to_clickhouse(top_vendors, "top5_vendors_report")

    # Vendor performance by country
    vendors_by_country = (fact_df
                          .join(items_df.select("item_id", "vendor_id"), "item_id")
                          .join(vendors_df.select("vendor_id", "vendor_country"), "vendor_id")
                          .groupBy("vendor_country")
                          .agg(F.sum("total_amount").alias("total_revenue"),
                               F.count("trans_id").alias("order_count")))
    save_to_clickhouse(vendors_by_country, "vendors_by_country_report")

    print("Vendor reports completed")


def generate_quality_analysis(spark, fact_df, items_df):
    """Generate product quality analysis reports"""
    quality_analysis = (fact_df
                        .join(items_df.select("item_id", "item_name", "item_category",
                                              "item_rating", "item_reviews"), "item_id")
                        .groupBy("item_id", "item_name", "item_category",
                                 "item_rating", "item_reviews")
                        .agg(F.sum("quantity_sold").alias("total_quantity"),
                             F.sum("total_amount").alias("total_revenue"))
                        .select("item_id", "item_name", "item_category",
                                F.col("item_rating").alias("avg_score"),
                                F.col("item_reviews").cast("long").alias("feedback_count"),
                                "total_quantity", "total_revenue"))
    save_to_clickhouse(quality_analysis, "item_quality_analysis")

    print("Quality analysis completed")


def run_report_pipeline():
    """Main report generation pipeline"""
    spark = init_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Setting up ClickHouse report tables...")
    setup_clickhouse_tables()

    print("Loading star schema data from PostgreSQL...")
    fact_table = load_from_postgres(spark, "fact_transactions").cache()
    buyers_table = load_from_postgres(spark, "ref_buyer").cache()
    agents_table = load_from_postgres(spark, "ref_agent").cache()
    items_table = load_from_postgres(spark, "ref_item").cache()
    outlets_table = load_from_postgres(spark, "ref_outlet").cache()
    vendors_table = load_from_postgres(spark, "ref_vendor").cache()
    calendar_table = load_from_postgres(spark, "ref_calendar").cache()

    generate_item_reports(spark, fact_table, items_table)
    generate_buyer_reports(spark, fact_table, buyers_table)
    generate_time_analysis(spark, fact_table, calendar_table)
    generate_outlet_reports(spark, fact_table, outlets_table)
    generate_vendor_reports(spark, fact_table, items_table, vendors_table)
    generate_quality_analysis(spark, fact_table, items_table)

    print("All 6 reports successfully generated and saved to ClickHouse!")
    spark.stop()


if __name__ == "__main__":
    run_report_pipeline()
