import dlt
from pyspark.sql import functions as F

# ================================================================
# BRONZE LAYER
# ================================================================

@dlt.table(
    name="dlt_raw_orders",
    comment="Raw orders from landing zone",
    table_properties={"quality": "bronze"}
)
def dlt_raw_orders():
    return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv("/Volumes/ecommerce/bronze/landing_zone/olist_orders_dataset.csv")
            .withColumn("_ingestion_time", F.current_timestamp())
            .withColumn("_source_file", F.lit("olist_orders_dataset.csv"))
    )

@dlt.table(
    name="dlt_raw_customers",
    comment="Raw customers from landing zone",
    table_properties={"quality": "bronze"}
)
def dlt_raw_customers():
    return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv("/Volumes/ecommerce/bronze/landing_zone/olist_customers_dataset.csv")
            .withColumn("_ingestion_time", F.current_timestamp())
            .withColumn("_source_file", F.lit("olist_customers_dataset.csv"))
    )

@dlt.table(
    name="dlt_raw_order_items",
    comment="Raw order items from landing zone",
    table_properties={"quality": "bronze"}
)
def dlt_raw_order_items():
    return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv("/Volumes/ecommerce/bronze/landing_zone/olist_order_items_dataset.csv")
            .withColumn("_ingestion_time", F.current_timestamp())
            .withColumn("_source_file", F.lit("olist_order_items_dataset.csv"))
    )

@dlt.table(
    name="dlt_raw_payments",
    comment="Raw payments from landing zone",
    table_properties={"quality": "bronze"}
)
def dlt_raw_payments():
    return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv("/Volumes/ecommerce/bronze/landing_zone/olist_order_payments_dataset.csv")
            .withColumn("_ingestion_time", F.current_timestamp())
            .withColumn("_source_file", F.lit("olist_order_payments_dataset.csv"))
    )

@dlt.table(
    name="dlt_raw_reviews",
    comment="Raw reviews from landing zone",
    table_properties={"quality": "bronze"}
)
def dlt_raw_reviews():
    return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv("/Volumes/ecommerce/bronze/landing_zone/olist_order_reviews_dataset.csv")
            .withColumn("_ingestion_time", F.current_timestamp())
            .withColumn("_source_file", F.lit("olist_order_reviews_dataset.csv"))
    )


# ================================================================
# SILVER LAYER
# ================================================================

@dlt.table(
    name="dlt_silver_orders",
    comment="Cleaned and validated orders",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_order_id",      "order_id IS NOT NULL")
@dlt.expect("valid_customer_id",   "customer_id IS NOT NULL")
@dlt.expect("valid_status",        "order_status IS NOT NULL")
def dlt_silver_orders():
    return (
        dlt.read("dlt_raw_orders")
            .dropDuplicates(["order_id"])
            .filter(F.col("order_id").isNotNull())
            .withColumn("order_status",
                F.upper(F.trim(F.col("order_status"))))
            .withColumn("order_purchase_timestamp",
                F.to_timestamp("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("order_approved_at",
                F.to_timestamp("order_approved_at", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("order_delivered_carrier_date",
                F.to_timestamp("order_delivered_carrier_date", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("order_delivered_customer_date",
                F.to_timestamp("order_delivered_customer_date", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("order_estimated_delivery_date",
                F.to_timestamp("order_estimated_delivery_date", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("delivery_delay_days",
                F.when(
                    F.col("order_delivered_customer_date").isNotNull(),
                    F.datediff(
                        F.col("order_delivered_customer_date"),
                        F.col("order_estimated_delivery_date")
                    )
                ).otherwise(None))
            .withColumn("updated_at", F.current_timestamp())
            .drop("_ingestion_time", "_source_file")
    )

@dlt.table(
    name="dlt_silver_customers",
    comment="Cleaned and validated customers",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_customer_id",  "customer_id IS NOT NULL")
@dlt.expect("valid_state",        "customer_state IS NOT NULL")
def dlt_silver_customers():
    return (
        dlt.read("dlt_raw_customers")
            .dropDuplicates(["customer_id"])
            .filter(F.col("customer_id").isNotNull())
            .withColumn("customer_city",
                F.initcap(F.trim(F.col("customer_city"))))
            .withColumn("customer_state",
                F.upper(F.trim(F.col("customer_state"))))
            .withColumn("customer_zip_code_prefix",
                F.col("customer_zip_code_prefix").cast("integer"))
            .withColumn("updated_at", F.current_timestamp())
            .drop("_ingestion_time", "_source_file")
    )

@dlt.table(
    name="dlt_silver_order_items",
    comment="Cleaned and validated order items",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_order_id",   "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("positive_price",   "price > 0")
def dlt_silver_order_items():
    return (
        dlt.read("dlt_raw_order_items")
            .dropDuplicates(["order_id", "order_item_id"])
            .filter(F.col("order_id").isNotNull())
            .withColumn("order_item_id",
                F.col("order_item_id").cast("integer"))
            .withColumn("price",
                F.col("price").cast("double"))
            .withColumn("freight_value",
                F.col("freight_value").cast("double"))
            .withColumn("shipping_limit_date",
                F.to_timestamp("shipping_limit_date", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("line_total",
                F.round(F.col("price") + F.col("freight_value"), 2))
            .withColumn("updated_at", F.current_timestamp())
            .drop("_ingestion_time", "_source_file")
    )

@dlt.table(
    name="dlt_silver_payments",
    comment="Cleaned and validated payments",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_order_id",     "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_payment",   "payment_value > 0")
@dlt.expect_or_drop("valid_payment_type", "payment_type != 'not_defined'")
def dlt_silver_payments():
    return (
        dlt.read("dlt_raw_payments")
            .dropDuplicates(["order_id", "payment_sequential"])
            .filter(F.col("order_id").isNotNull())
            .filter(F.col("payment_type") != "not_defined")
            .withColumn("payment_type",
                F.upper(F.trim(F.col("payment_type"))))
            .withColumn("payment_sequential",
                F.col("payment_sequential").cast("integer"))
            .withColumn("payment_installments",
                F.col("payment_installments").cast("integer"))
            .withColumn("payment_value",
                F.col("payment_value").cast("double"))
            .withColumn("updated_at", F.current_timestamp())
            .drop("_ingestion_time", "_source_file")
    )

@dlt.table(
    name="dlt_silver_reviews",
    comment="Cleaned and validated reviews",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_review_id", "review_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_id",  "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_score",     "review_score BETWEEN 1 AND 5")
def dlt_silver_reviews():
    return (
        dlt.read("dlt_raw_reviews")
            .dropDuplicates(["review_id"])
            .filter(F.col("review_id").isNotNull())
            .withColumn("review_score",
                F.col("review_score").cast("integer"))
            .withColumn("review_creation_date",
                F.to_timestamp("review_creation_date", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("review_answer_timestamp",
                F.to_timestamp("review_answer_timestamp", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("review_comment_title",
                F.when(F.col("review_comment_title").isNull(), "No Title")
                 .otherwise(F.trim(F.col("review_comment_title"))))
            .withColumn("review_comment_message",
                F.when(F.col("review_comment_message").isNull(), "No Comment")
                 .otherwise(F.trim(F.col("review_comment_message"))))
            .withColumn("sentiment",
                F.when(F.col("review_score") >= 4, "POSITIVE")
                 .when(F.col("review_score") == 3, "NEUTRAL")
                 .otherwise("NEGATIVE"))
            .withColumn("updated_at", F.current_timestamp())
            .drop("_ingestion_time", "_source_file")
    )


# ================================================================
# GOLD LAYER
# ================================================================

@dlt.table(
    name="dlt_gold_daily_revenue",
    comment="Daily revenue aggregated",
    table_properties={"quality": "gold"}
)
def dlt_gold_daily_revenue():
    orders = dlt.read("dlt_silver_orders")
    items  = dlt.read("dlt_silver_order_items")

    return (
        orders
            .filter(F.col("order_status") == "DELIVERED")
            .join(items, "order_id", "inner")
            .groupBy(
                F.date_trunc("day", "order_purchase_timestamp")
                 .alias("order_day")
            )
            .agg(
                F.countDistinct("order_id")      .alias("total_orders"),
                F.countDistinct("customer_id")   .alias("unique_customers"),
                F.round(F.sum("price"), 2)       .alias("total_revenue"),
                F.round(F.avg("price"), 2)       .alias("avg_order_value"),
                F.round(F.sum("freight_value"),2).alias("total_freight")
            )
            .withColumn("updated_at", F.current_timestamp())
            .orderBy("order_day")
    )

@dlt.table(
    name="dlt_gold_payment_analysis",
    comment="Payment type analysis",
    table_properties={"quality": "gold"}
)
def dlt_gold_payment_analysis():
    payments = dlt.read("dlt_silver_payments")
    orders   = dlt.read("dlt_silver_orders")

    return (
        payments
            .join(
                orders.filter(F.col("order_status") == "DELIVERED")
                      .select("order_id"),
                "order_id", "inner"
            )
            .groupBy("payment_type")
            .agg(
                F.countDistinct("order_id")         .alias("total_orders"),
                F.round(F.sum("payment_value"), 2)  .alias("total_revenue"),
                F.round(F.avg("payment_value"), 2)  .alias("avg_payment"),
                F.round(F.avg("payment_installments"), 2).alias("avg_installments")
            )
            .withColumn("updated_at", F.current_timestamp())
            .orderBy("total_revenue", ascending=False)
    )
