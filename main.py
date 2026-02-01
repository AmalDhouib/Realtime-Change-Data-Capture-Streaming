from faker import Faker
import psycopg2
from datetime import datetime, UTC
import random

fake = Faker()

# Genère une transaction fake
def generate_transaction():
    user = fake.simple_profile()

    return {
        "transaction_id": fake.uuid4(),
        "user_id": user["username"],
        "timestamp": datetime.now(UTC).timestamp(),
        "amount": round(random.uniform(10, 1000), 2),
        "currency": random.choice(["EUR", "USD"]),
        "city": fake.city(),
        "country": fake.country(),
        "merchant_name": fake.company(),
        "payment_method": random.choice(["credit_card", "debit_card", "online_transfer"]),
        "ip_address": fake.ipv4(),
        "voucher_code": random.choice(["", "DISCOUNT10", ""]),
        "affiliate_id": fake.uuid4()
    }


# Cree la table PostgreSQL
def create_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255),
            timestamp TIMESTAMP,
            amount DECIMAL,
            currency VARCHAR(10),
            city VARCHAR(255),
            country VARCHAR(255),
            merchant_name VARCHAR(255),
            payment_method VARCHAR(255),
            ip_address VARCHAR(255),
            voucher_code VARCHAR(255),
            affiliate_id VARCHAR(255)
        );
    """)
    conn.commit()
    cursor.close()
if __name__ =="__main__":
    conn=psycopg2.connect(host="localhost", database="financialDB", user="postgres", password="amal",port="5432")
    create_table(conn)
    transaction = generate_transaction()
    cursor = conn.cursor()
    print(transaction)
    cursor.execute(
        """
        INSERT INTO transactions (transaction_id,
                                  user_id,
                                  timestamp,
                                  amount,
                                  currency,
                                  city,
                                  country,
                                  merchant_name,
                                  payment_method,
                                  ip_address,
                                  voucher_code,
                                  affiliate_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            transaction["transaction_id"],
            transaction["user_id"],
            datetime.fromtimestamp(transaction["timestamp"]),
            transaction["amount"],
            transaction["currency"],
            transaction["city"],
            transaction["country"],
            transaction["merchant_name"],
            transaction["payment_method"],
            transaction["ip_address"],
            transaction["voucher_code"],
            transaction["affiliate_id"]
        )
    )
    cursor = conn.cursor()
    cursor.execute("SELECT inet_server_addr(), current_database();")
    print(cursor.fetchone())

    conn.commit()
    cursor.close()

