from flask import Blueprint, jsonify, request
from app.services.couchbase import CouchbaseService
from app.models.transaction import Transaction
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType, IntegerType
from pyspark.sql.functions import col, countDistinct, sum as _sum, expr, to_date
import datetime

bp = Blueprint("transactions", __name__, url_prefix="/api/transactions")

couchbaseService = CouchbaseService(
    bucket_name="Transactions",
)

spark = SparkSession.builder.appName("TransactionAnalytics").getOrCreate()
schema = StructType([
    StructField("amount", DoubleType(), True),
    StructField("card_number", StringType(), True),
    StructField("card_present", BooleanType(), True),
    StructField("card_type", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("city", StringType(), True),
    StructField("city_size", StringType(), True),
    StructField("country", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("device", StringType(), True),
    StructField("device_fingerprint", StringType(), True),
    StructField("distance_from_home", IntegerType(), True),
    StructField("high_risk_merchant", BooleanType(), True),
    StructField("ip_address", StringType(), True),
    StructField("is_fraudulent", BooleanType(), True),
    StructField("merchant", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("merchant_type", StringType(), True),
    StructField("text", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("transaction_hour", IntegerType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("velocity_last_hour", StringType(), True),
    StructField("weekend_transaction", BooleanType(), True)
])

@bp.route("/", methods=["GET"])
def get_transactions():
    """Fetch all transactions from Couchbase."""
    transactions = couchbaseService.get_all_transactions()
    print(transactions)
    return jsonify({
        "status": "success",
        "data": [txn.to_dict() for txn in transactions],
        "message": "Transactions retrieved successfully."
    })

@bp.route("/", methods=["POST"])
def add_transaction():
    """Add a transaction to Couchbase."""
    data = request.json
    transaction = Transaction(
        transaction_id=data.get("transaction_id"),
        amount=data.get("amount"),
        location=data.get("location"),
        is_fraudulent=data.get("is_fraudulent"),
        timestamp=datetime.datetime.utcnow().isoformat()
    )
    success = couchbaseService.insert_transaction(transaction)
    if success:
        return jsonify({"message": "Transaction added successfully"}), 201
    return jsonify({"error": "Failed to add transaction"}), 500

@bp.route("/analytics/summary", methods=["GET"])
def transaction_summary():
    """Calculate total transactions, fraud count, unique customers, and countries."""
    transactions = couchbaseService.get_all_transactions()
    df = spark.createDataFrame(transactions, schema=schema)

    summary = df.selectExpr(
        "count(*) as total_transactions",
        "sum(case when is_fraudulent = true then 1 else 0 end) as fraud_count",
        "count(distinct customer_id) as unique_customers",
        "count(distinct country) as unique_countries"
    ).collect()[0]

    result = {
        "total_transactions": summary["total_transactions"],
        "fraud_count": summary["fraud_count"],
        "unique_customers": summary["unique_customers"],
        "unique_countries": summary["unique_countries"]
    }
    return jsonify(
        {
            "status": "success",
            "data": result,
            "message": "Transactions summary retrieved successfully."
        }
    )

@bp.route("/analytics/fraud-by-country", methods=["GET"])
def fraud_by_country():
    """Calculate the count of fraud transactions by country in ascending order."""
    transactions = couchbaseService.get_all_transactions()
    df = spark.createDataFrame(transactions, schema=schema)

    country_fraud_counts = df.filter(col("is_fraudulent") == True) \
        .groupBy("country") \
        .count() \
        .orderBy("count", ascending=False) \
        .collect()

    result = [
        {"country": row["country"], "fraud_count": row["count"]}
        for row in country_fraud_counts
    ]
    return jsonify({
        "status": "success",
        "data": result,
        "message": "Transactions analytics retrieved successfully."
        })

@bp.route("/analytics/fraud-trends", methods=["POST"])
def fraud_trends():
    """Calculate fraud and non-fraud transaction amounts between two dates."""    

    data = request.json
    start_date = data.get("start_date")  
    end_date = data.get("end_date")      
    currency = data.get("currency")   

    transactions = couchbaseService.get_all_transactions()
    df = spark.createDataFrame(transactions, schema=schema)

    df = df.withColumn("date", to_date(col("timestamp")))

    # Filter by the provided date range and currency
    filtered_df = df.filter(
        (col("date") >= start_date) &
        (col("date") <= end_date) &
        (col("currency") == currency)
    )

    # Calculate fraud and non-fraud trends
    fraud_trend = filtered_df.filter(col("is_fraudulent") == True) \
        .select("amount").rdd.flatMap(lambda x: x).collect()

    non_fraud_trend = filtered_df.filter(col("is_fraudulent") == False) \
        .select("amount").rdd.flatMap(lambda x: x).collect()

    result = {
        "currency": currency,
        "fraud_trend": fraud_trend,
        "non_fraud_trend": non_fraud_trend
    }
    return jsonify(
        {
            "status": "success",
            "data": result,
            "message": "Transactions analytics retrieved successfully."
        }
    )
    
