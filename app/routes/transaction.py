from flask import Blueprint, jsonify, request
from app.services.couchbase import CouchbaseService
from app.models.transaction import Transaction
import datetime

bp = Blueprint("transactions", __name__, url_prefix="/api/transactions")

# Initialize Couchbase service
couchbaseService = CouchbaseService(
    bucket_name="Transactions",
)

@bp.route("/", methods=["GET"])
def get_transactions():
    """Fetch all transactions from Couchbase."""
    transactions = couchbaseService.get_all_transactions()
    print(transactions)
    return jsonify([txn.to_dict() for txn in transactions])

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
