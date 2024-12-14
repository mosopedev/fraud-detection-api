class Transaction:
    def __init__(
    self, 
    transaction_id, 
    customer_id, 
    card_number, 
    timestamp, 
    merchant_category, 
    merchant_type, 
    merchant, 
    amount, 
    currency, 
    country, 
    city, 
    city_size, 
    card_type, 
    card_present, 
    device, 
    channel, 
    device_fingerprint, 
    ip_address, 
    distance_from_home, 
    high_risk_merchant, 
    transaction_hour, 
    weekend_transaction, 
    velocity_last_hour, 
    text, 
    is_fraudulent):
        
        self.transaction_id = transaction_id
        self.customer_id = customer_id
        self.card_number = card_number
        self.timestamp = timestamp
        self.merchant_category = merchant_category
        self.merchant_type = merchant_type
        self.merchant = merchant
        self.amount = amount
        self.currency = currency
        self.country = country
        self.city = city
        self.city_size = city_size
        self.card_type = card_type
        self.card_present = card_present
        self.device = device
        self.channel = channel
        self.device_fingerprint = device_fingerprint
        self.ip_address = ip_address
        self.distance_from_home = distance_from_home
        self.high_risk_merchant = high_risk_merchant
        self.transaction_hour = transaction_hour
        self.weekend_transaction = weekend_transaction
        self.velocity_last_hour = velocity_last_hour
        self.text = text
        self.is_fraudulent = is_fraudulent


    def to_dict(self):
        return {
            "transaction_id": self.transaction_id,
            "customer_id": self.customer_id,
            "card_number": self.card_number,
            "timestamp": self.timestamp,
            "merchant_category": self.merchant_category,
            "merchant_type": self.merchant_type,
            "merchant": self.merchant,
            "amount": self.amount,
            "currency": self.currency,
            "country": self.country,
            "city": self.city,
            "city_size": self.city_size,
            "card_type": self.card_type,
            "card_present": self.card_present,
            "device": self.device,
            "channel": self.channel,
            "device_fingerprint": self.device_fingerprint,
            "ip_address": self.ip_address,
            "distance_from_home": self.distance_from_home,
            "high_risk_merchant": self.high_risk_merchant,
            "transaction_hour": self.transaction_hour,
            "weekend_transaction": self.weekend_transaction,
            "velocity_last_hour": self.velocity_last_hour,
            "text": self.text,
            "is_fraudulent": self.is_fraudulent,
        }


    @staticmethod
    def from_dict(data):
        return Transaction(
            transaction_id=data.get("transaction_id"),
            customer_id=data.get("customer_id"),
            card_number=data.get("card_number"),
            timestamp=data.get("timestamp"),
            merchant_category=data.get("merchant_category"),
            merchant_type=data.get("merchant_type"),
            merchant=data.get("merchant"),
            amount=data.get("amount"),
            currency=data.get("currency"),
            country=data.get("country"),
            city=data.get("city"),
            city_size=data.get("city_size"),
            card_type=data.get("card_type"),
            card_present=data.get("card_present"),
            device=data.get("device"),
            channel=data.get("channel"),
            device_fingerprint=data.get("device_fingerprint"),
            ip_address=data.get("ip_address"),
            distance_from_home=data.get("distance_from_home"),
            high_risk_merchant=data.get("high_risk_merchant"),
            transaction_hour=data.get("transaction_hour"),
            weekend_transaction=data.get("weekend_transaction"),
            velocity_last_hour=data.get("velocity_last_hour"),
            text=data.get("text"),
            is_fraudulent=data.get("is_fraudulent"),
        )
