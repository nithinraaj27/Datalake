from airflow import DAG
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from datetime import datetime, timedelta

from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from botocore.exceptions import ClientError

class SafeGlueCrawlerOperator(GlueCrawlerOperator):
    def execute(self, context):
        try:
            return super().execute(context)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]

            if error_code == "CrawlerRunningException":
                self.log.info(
                    "Crawler already running. Waiting for completion instead of retrying."
                )
                return self.hook.wait_for_crawler_completion(
                    self.config["Name"]
                )

            raise  # real failure → Airflow retry logic applies


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_bronze_silver_gold_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end Ecommerce DataLake Pipeline (Bronze → Silver → Gold)",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ecommerce", "datalake"],
) as dag:

    # =========================================================
    # 1️⃣ BRONZE CRAWLER
    # =========================================================
    run_bronze_crawler = SafeGlueCrawlerOperator(
        task_id="run_bronze_crawler",
        config={"Name": "bronze-events-crawler"},
        region_name="eu-north-1",
        wait_for_completion=True,
        poll_interval=60,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    # =========================================================
    # 2️⃣ SILVER CTAS (FIXED – THIS WAS YOUR FAILURE POINT)
    # =========================================================
    create_silver_table = AthenaOperator(
        task_id="create_silver_table",
        query="""
        CREATE TABLE IF NOT EXISTS ecommerce_silver.silver_ecommerce_event_silver
        WITH (
            format = 'PARQUET',
            external_location = 's3://ecommerce-event-silver/',
            partitioned_by = ARRAY['date', 'hour']
        ) AS
        SELECT
            event_id,
            event_type,
            event_time,
            source,
            schema_version,
            trace_id,
            user_id,

            order_id,
            order_amount,
            currency,
            order_status,
            order_items_count,

            payment_id,
            payment_amount,
            payment_method,
            payment_status,
            failure_reason,

            activity_type,
            device_type,
            platform,
            location_country,
            registered_at,

            topic,

            date(from_iso8601_timestamp(event_time)) AS date,
            hour(from_iso8601_timestamp(event_time)) AS hour
        FROM (
            SELECT
                event_id, event_type, event_time, source, schema_version, trace_id, user_id,
                order_id, order_amount, currency, order_status, order_items_count,
                NULL AS payment_id, NULL AS payment_amount, NULL AS payment_method,
                NULL AS payment_status, NULL AS failure_reason,
                NULL AS activity_type, NULL AS device_type, NULL AS platform,
                NULL AS location_country, NULL AS registered_at,
                'order-events' AS topic
            FROM ecommerce_bronze.bronze_topic_order_events

            UNION ALL

            SELECT
                event_id, event_type, event_time, source, schema_version, trace_id, user_id,
                order_id, NULL, NULL, NULL, NULL,
                payment_id, payment_amount, payment_method, payment_status, failure_reason,
                NULL, NULL, NULL, NULL, NULL,
                'payment-events' AS topic
            FROM ecommerce_bronze.bronze_topic_payment_events

            UNION ALL

            SELECT
                event_id, event_type, event_time, source, schema_version, trace_id, user_id,
                NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL,
                activity_type, device_type, platform, location_country, registered_at,
                'user-events' AS topic
            FROM ecommerce_bronze.bronze_topic_user_events
        );
        """,
        database="ecommerce_silver",
        output_location="s3://athena-query-results-nithinraaj-eu-north-1/",
        region_name="eu-north-1",
    )

    # =========================================================
    # 3️⃣ SILVER CRAWLER
    # =========================================================
    run_silver_crawler = SafeGlueCrawlerOperator(
        task_id="run_silver_crawler",
        config={"Name": "silver-events-crawler"},
        region_name="eu-north-1",
        wait_for_completion=True,
        poll_interval=60,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    # =========================================================
    # 4️⃣ GOLD – ORDERS SUMMARY
    # =========================================================
    gold_orders_summary = AthenaOperator(
        task_id="gold_orders_daily_summary",
        query="""
        CREATE TABLE IF NOT EXISTS ecommerce_gold.orders_daily_summary
        WITH (
            format = 'PARQUET',
            external_location = 's3://ecommerce-event-gold/orders_daily_summary/',
            partitioned_by = ARRAY['date']
        ) AS
        SELECT
            COUNT(*) AS total_orders,
            SUM(CASE WHEN order_status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_orders,
            SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_orders,
            SUM(order_amount) AS total_order_amount,
            date
        FROM ecommerce_silver.silver_ecommerce_event_silver
        WHERE topic = 'order-events'
        GROUP BY date;
        """,
        database="ecommerce_gold",
        output_location="s3://athena-query-results-nithinraaj-eu-north-1/",
        region_name="eu-north-1",
    )

    # =========================================================
    # 5️⃣ GOLD – PAYMENT SUMMARY
    # =========================================================
    gold_payment_summary = AthenaOperator(
        task_id="gold_payment_daily_summary",
        query="""
        CREATE TABLE IF NOT EXISTS ecommerce_gold.payment_daily_summary
        WITH (
            format = 'PARQUET',
            external_location = 's3://ecommerce-event-gold/payment_daily_summary/',
            partitioned_by = ARRAY['date']
        ) AS
        SELECT
            COUNT(*) AS total_payments,
            SUM(CASE WHEN payment_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_payments,
            SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_payments,
            SUM(CASE WHEN payment_status = 'INITIATED' THEN 1 ELSE 0 END) AS initiated_payments,
            SUM(CASE WHEN payment_status = 'SUCCESS' THEN payment_amount ELSE 0 END) AS total_success_amount,
            date
        FROM ecommerce_silver.silver_ecommerce_event_silver
        WHERE topic = 'payment-events'
        GROUP BY date;
        """,
        database="ecommerce_gold",
        output_location="s3://athena-query-results-nithinraaj-eu-north-1/",
        region_name="eu-north-1",
    )

    # =========================================================
    # 6️⃣ GOLD – USER ACTIVITY SUMMARY
    # =========================================================
    gold_user_activity_summary = AthenaOperator(
        task_id="gold_user_daily_activity_summary",
        query="""
        CREATE TABLE IF NOT EXISTS ecommerce_gold.user_daily_activity_summary
        WITH (
            format = 'PARQUET',
            external_location = 's3://ecommerce-event-gold/user_daily_activity_summary/',
            partitioned_by = ARRAY['date']
        ) AS
        SELECT
            user_id,
            COUNT(*) AS total_events,
            SUM(CASE WHEN event_type = 'USER_REGISTERED' THEN 1 ELSE 0 END) AS registrations,
            SUM(CASE WHEN event_type = 'USER_LOGIN' THEN 1 ELSE 0 END) AS logins,
            SUM(CASE WHEN event_type = 'USER_ACTIVITY' THEN 1 ELSE 0 END) AS activities,
            COUNT(DISTINCT device_type) AS device_count,
            date
        FROM ecommerce_silver.silver_ecommerce_event_silver
        WHERE topic = 'user-events'
        GROUP BY user_id, date;
        """,
        database="ecommerce_gold",
        output_location="s3://athena-query-results-nithinraaj-eu-north-1/",
        region_name="eu-north-1",
    )

    # =========================================================
    # DAG ORDER
    # =========================================================
    (
        run_bronze_crawler
        >> create_silver_table
        >> run_silver_crawler
        >> gold_orders_summary
        >> gold_payment_summary
        >> gold_user_activity_summary
    )
