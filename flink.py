from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import MapFunction
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import RestartStrategies
import pandas as pd
import joblib
from app.services.couchbase import CouchbaseService
from sklearn.preprocessing import LabelEncoder, StandardScaler
import json


# Fraud Prediction Map Function
class FraudPrediction(MapFunction):
    def __init__(self, model_path):
        self.model_path = model_path
        self.model = None

    def open(self, runtime_context):
        # Load the ML model
        self.model = joblib.load(self.model_path)

    def map(self, value):
        try:
        # Parse transaction data from Kafka
            transaction = json.loads(value)
            data = pd.DataFrame([transaction])

            uninteresting_columns = [
                "card_number", 
                "transaction_id", 
                "customer_id", 
                "timestamp", 
                "merchant", 
                "device_fingerprint", 
                "ip_address", 
                "velocity_last_hour"
            ]

            interest_data = data.drop(columns=[col for col in uninteresting_columns if col in data.columns], axis=1)
            
            object_columns = interest_data.select_dtypes(include=['object']).columns
            number_columns = interest_data.select_dtypes(include=['number']).columns
            
            label_encoder = LabelEncoder()
            scaler = StandardScaler()
            interest_data[number_columns] = scaler.fit_transform(interest_data[number_columns])
           
            for col in object_columns:
                interest_data[col] = label_encoder.fit_transform(interest_data[col])

            # model prediction
            prediction = self.model.predict(interest_data)
            print(prediction[0])
            transaction['is_fraudulent'] = bool(prediction[0])

            couchbaseService = CouchbaseService(bucket_name="Transactions")
            couchbaseService.insert_transaction(transaction)
            return transaction
        
        except json.JSONDecodeError as e:
            print("Error decoding transaction JSON:", e)
        except KeyError as e:
            print(f"Missing key in transaction data: {e}")
        except AttributeError as e:
            print(f"Error processing transaction: {e}")
            print(e)
        except Exception as e:
            print(f"Error processing transaction: {e}")


# Main Flink Pipeline
def fraud_detection_pipeline():
    # Initialing Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///Users/mosope/Desktop/fraud-detection-api/flink-sql-connector-kafka-3.3.0-1.20.jar")

    # Enable checkpointing for fault tolerance
    env.enable_checkpointing(5000)  # Checkpoints every 5 seconds
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_fail_on_checkpointing_errors(False)
    checkpoint_config.set_checkpoint_timeout(60000)
    checkpoint_config.set_min_pause_between_checkpoints(1000)
    checkpoint_config.set_max_concurrent_checkpoints(1)
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(
        restart_attempts=3,
        delay_between_attempts=10000
    ))
    # set flink parallel nodes
    env.set_parallelism(4)

    # Kafka consumer for transactions
    kafka_props = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "fraud-detection-group"
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics="transactions",
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    # Add Kafka as a source to Flink pipeline
    transaction_stream = env.add_source(kafka_consumer)

    # Process transaction-Run flink job
    predicted_stream = transaction_stream.map(FraudPrediction("lg_fraud_detection_model.pkl"))

    env.execute("Fraud Detection with Kafka, Flink, and Couchbase")

if __name__ == "__main__":
    fraud_detection_pipeline()
